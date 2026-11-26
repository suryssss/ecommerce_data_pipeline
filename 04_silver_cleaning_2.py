# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in locals() else os.getcwd()
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from scd_processors import process_scd1, process_scd2, validate_scd2_integrity

# COMMAND ----------

# DBTITLE 1,Parameters & Strategy Mapping
# PARAMETERS & STRATEGY MAPPING

dbutils.widgets.text("entity", "customers", "Source Entity")
entity = dbutils.widgets.get("entity").strip().lower()

STRATEGY = {
    "customers": "SCD2",
    "products": "SCD2",
    "orders": "SCD1",
    "order_items": "SCD1",
    "payments": "SCD1"
}

if entity not in STRATEGY:
    raise ValueError(f"Invalid entity '{entity}'. Expected one of: {list(STRATEGY.keys())}")

strategy = STRATEGY[entity]

PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id"
}
PRIMARY_KEY = PRIMARY_KEYS[entity]

CATALOG = "ecommerce_pipeline"
SOURCE_TABLE = f"{CATALOG}.silver.silver_{entity}"
TARGET_TABLE = f"{CATALOG}.silver.silver2_{entity}"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/default/silver2_checkpoint/{entity}"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.default.silver2_checkpoint")

dbutils.fs.mkdirs(CHECKPOINT_PATH)

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Silver table {SOURCE_TABLE} does not exist. Run Silver cleaning 1 first.")

# COMMAND ----------

# DBTITLE 1,SCD Batch Processing Logic
# SCD BATCH PROCESSING LOGIC

def process_scd_batch(micro_batch_df, batch_idx=0):
    """
    Processes SCD1/SCD2 for a single incremental micro-batch or manual batch.
    """
    if micro_batch_df.isEmpty():
        print(f"No records found to process for {entity} in micro-batch {batch_idx}.")
        return

    batch_rows = micro_batch_df.select("_batch_id").distinct().collect()
    batch_ids = [
        row["_batch_id"] for row in batch_rows
        if row["_batch_id"] is not None and str(row["_batch_id"]).strip() != ""
    ]
    batch_label = ", ".join(batch_ids) if batch_ids else f"stream_epoch_{batch_idx}"
    print(f"Processing micro-batch {batch_idx} (batch_ids: [{batch_label}], {micro_batch_df.count()} records) for {entity} ({strategy})")

    if strategy == "SCD1":
        process_scd1(
            spark=spark,
            source_df=micro_batch_df,
            target_table_name=TARGET_TABLE,
            primary_key=PRIMARY_KEY
        )
    elif strategy == "SCD2":
        process_scd2(
            spark=spark,
            source_df=micro_batch_df,
            target_table_name=TARGET_TABLE,
            primary_key=PRIMARY_KEY
        )
        validate_scd2_integrity(
            spark=spark,
            target_table_name=TARGET_TABLE,
            primary_key=PRIMARY_KEY
        )

    print(f"Micro-batch {batch_idx} ({batch_label}) SCD processing complete for {TARGET_TABLE}.")

# COMMAND ----------

# DBTITLE 1,Execution: Incremental Stream vs Manual Override
# EXECUTION: TRUE INCREMENTAL STREAM VS MANUAL OVERRIDE

# True Incremental Mode: Read only newly committed Delta files via streaming checkpoint
print(f"Starting true incremental read on {SOURCE_TABLE} via Delta streaming...")
incremental_query = (
    spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .table(SOURCE_TABLE)
        .writeStream
        .foreachBatch(process_scd_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
)
incremental_query.awaitTermination()
print(f"Incremental SCD processing completed successfully for {entity}.")