# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import (
    current_timestamp,
    col,
    lit,
    regexp_extract
)

# COMMAND ----------

# ENTITY PARAMETER & VALIDATION

dbutils.widgets.text("entity", "customers", "Source Entity")
entity = dbutils.widgets.get("entity").strip().lower()

VALID_ENTITIES = ["customers", "products", "orders", "order_items", "payments"]
if entity not in VALID_ENTITIES:
    raise ValueError(f"Invalid entity '{entity}'. Expected one of: {VALID_ENTITIES}")

# COMMAND ----------

# CONFIGURATION

CATALOG = "ecommerce_pipeline"
LANDING_BASE = "/Volumes/ecommerce_pipeline/default/landing"
LANDING_PATH = f"{LANDING_BASE}/{entity}"
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_{entity}"

SCHEMA_PATH = f"/Volumes/{CATALOG}/default/bronze_schema/{entity}"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/default/bronze_checkpoint/{entity}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
dbutils.fs.mkdirs(SCHEMA_PATH)
dbutils.fs.mkdirs(CHECKPOINT_PATH)

print(f"Bronze ingestion started for entity: {entity} -> {BRONZE_TABLE}")

# COMMAND ----------

# AUTO LOADER INGESTION

bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(LANDING_PATH)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source_entity", lit(entity))
        .withColumn("_batch_id", regexp_extract(col("_metadata.file_path"), r"batch_([^/\\]+)", 1))
)

# Write batch stream to Bronze Delta table
bronze_query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
)

bronze_query.awaitTermination()

# COMMAND ----------

# VALIDATION

record_count = spark.table(BRONZE_TABLE).count()
print(f"Ingested {record_count} total records for {entity} in {BRONZE_TABLE}")