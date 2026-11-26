# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Parameters & Validation
# PARAMETERS & VALIDATION

dbutils.widgets.text("entity", "customers", "Source Entity")
entity = dbutils.widgets.get("entity").strip().lower()

VALID_ENTITIES = ["customers", "products", "orders", "order_items", "payments"]
if entity not in VALID_ENTITIES:
    raise ValueError(f"Invalid entity: {entity}. Expected: {VALID_ENTITIES}")

PK_MAPPING = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id"
}
primary_key = PK_MAPPING[entity]

# COMMAND ----------

# DBTITLE 1,Configuration & Checkpointing
# CONFIGURATION & CHECKPOINTING

CATALOG = "ecommerce_pipeline"
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_{entity}"
SILVER_TABLE = f"{CATALOG}.silver.silver_{entity}"
QUARANTINE_TABLE = f"{CATALOG}.silver.quarantine_{entity}"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/default/silver_checkpoint/{entity}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.default.silver_checkpoint")
dbutils.fs.mkdirs(CHECKPOINT_PATH)

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise ValueError(f"Bronze table {BRONZE_TABLE} does not exist. Run Bronze ingestion first.")

# COMMAND ----------

# DBTITLE 1,Batch Cleaning & Validation Logic
# BATCH CLEANING & VALIDATION LOGIC

def clean_and_write_batch(micro_batch_df, batch_idx=0):
    """
    Cleans, validates, quarantines bad records, and idempotently writes
    valid records to Silver for a single incremental micro-batch or batch.
    """
    if micro_batch_df.isEmpty():
        print(f"No records found to process for {entity} in micro-batch {batch_idx}.")
        return

    # Identify distinct batch IDs in the current micro-batch for logging and idempotency
    batch_rows = micro_batch_df.select("_batch_id").distinct().collect()
    batch_ids = [
        row["_batch_id"] for row in batch_rows
        if row["_batch_id"] is not None and str(row["_batch_id"]).strip() != ""
    ]
    batch_label = ", ".join(batch_ids) if batch_ids else f"stream_epoch_{batch_idx}"
    print(f"Processing micro-batch {batch_idx} (batch_ids: [{batch_label}], {micro_batch_df.count()} records) for {entity}")

    # 1. Standardization & Type Casting
    df = (
        micro_batch_df
        .withColumn("_ingestion_timestamp", F.col("_ingestion_timestamp").cast("timestamp"))
        .withColumn("_source_file", F.col("_source_file").cast("string"))
        .withColumn("_source_entity", F.col("_source_entity").cast("string"))
        .withColumn("_batch_id", F.col("_batch_id").cast("string"))
    )

    if "operation" in df.columns:
        df = df.withColumn("operation", F.upper(F.trim(F.col("operation"))))

    if "event_timestamp" in df.columns:
        df = df.withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))

    if entity == "customers":
        df = (
            df
            .withColumn("customer_id", F.trim(F.col("customer_id")))
            .withColumn("first_name", F.trim(F.col("first_name")))
            .withColumn("last_name", F.trim(F.col("last_name")))
            .withColumn("email", F.lower(F.trim(F.col("email"))))
            .withColumn("phone", F.trim(F.col("phone")))
            .withColumn("city", F.trim(F.col("city")))
            .withColumn("state", F.trim(F.col("state")))
            .withColumn("country", F.trim(F.col("country")))
            .withColumn("signup_date", F.to_date(F.col("signup_date")))
            .withColumn("created_at", F.to_timestamp(F.col("created_at")))
            .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
        )

    elif entity == "products":
        df = (
            df
            .withColumn("product_id", F.trim(F.col("product_id")))
            .withColumn("product_name", F.trim(F.col("product_name")))
            .withColumn("category", F.trim(F.col("category")))
            .withColumn("brand", F.trim(F.col("brand")))
            .withColumn("price", F.col("price").cast("decimal(18,2)"))
            .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
            .withColumn("product_status", F.upper(F.trim(F.col("product_status"))))
            .withColumn("created_at", F.to_timestamp(F.col("created_at")))
            .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
        )

    elif entity == "orders":
        df = (
            df
            .withColumn("order_id", F.trim(F.col("order_id")))
            .withColumn("customer_id", F.trim(F.col("customer_id")))
            .withColumn("order_date", F.to_timestamp(F.col("order_date")))
            .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
            .withColumn("shipping_city", F.trim(F.col("shipping_city")))
            .withColumn("shipping_state", F.trim(F.col("shipping_state")))
            .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)"))
            .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
        )

    elif entity == "order_items":
        df = (
            df
            .withColumn("order_item_id", F.trim(F.col("order_item_id")))
            .withColumn("order_id", F.trim(F.col("order_id")))
            .withColumn("product_id", F.trim(F.col("product_id")))
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
            .withColumn("discount", F.col("discount").cast("decimal(18,2)"))
            .withColumn("line_total", F.col("line_total").cast("decimal(18,2)"))
        )

    elif entity == "payments":
        df = (
            df
            .withColumn("payment_id", F.trim(F.col("payment_id")))
            .withColumn("order_id", F.trim(F.col("order_id")))
            .withColumn("payment_date", F.to_timestamp(F.col("payment_date")))
            .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
            .withColumn("payment_status", F.upper(F.trim(F.col("payment_status"))))
            .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
            .withColumn("transaction_reference", F.trim(F.col("transaction_reference")))
        )

    # 2. Data Quality & CDC Rules
    is_delete = (F.col("operation") == "DELETE") if "operation" in df.columns else F.lit(False)

    cdc_invalid = F.lit(False)
    if "operation" in df.columns:
        cdc_invalid = cdc_invalid | F.col("operation").isNull() | (~F.col("operation").isin("INSERT", "UPDATE", "DELETE"))
    if "event_timestamp" in df.columns:
        cdc_invalid = cdc_invalid | F.col("event_timestamp").isNull()

    if entity == "customers":
        entity_invalid = (
            F.col("customer_id").isNull()
            | (F.col("customer_id") == "")
            | (~is_delete & (
                F.col("email").isNull()
                | ~F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
            ))
        )

    elif entity == "products":
        entity_invalid = (
            F.col("product_id").isNull()
            | (F.col("product_id") == "")
            | (~is_delete & (
                F.col("price").isNull()
                | (F.col("price") <= 0)
                | F.col("stock_quantity").isNull()
                | (F.col("stock_quantity") < 0)
            ))
        )

    elif entity == "orders":
        entity_invalid = (
            F.col("order_id").isNull()
            | (F.col("order_id") == "")
            | (~is_delete & (
                F.col("customer_id").isNull()
                | (F.col("customer_id") == "")
                | F.col("order_date").isNull()
                | F.col("total_amount").isNull()
                | (F.col("total_amount") < 0)
            ))
        )

    elif entity == "order_items":
        entity_invalid = (
            F.col("order_item_id").isNull()
            | (F.col("order_item_id") == "")
            | (~is_delete & (
                F.col("order_id").isNull()
                | (F.col("order_id") == "")
                | F.col("product_id").isNull()
                | (F.col("product_id") == "")
                | F.col("quantity").isNull()
                | (F.col("quantity") <= 0)
                | F.col("unit_price").isNull()
                | (F.col("unit_price") < 0)
            ))
        )

    elif entity == "payments":
        valid_methods = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "WALLET"]
        valid_statuses = ["SUCCESS", "FAILED", "REFUNDED"]
        entity_invalid = (
            F.col("payment_id").isNull()
            | (F.col("payment_id") == "")
            | (~is_delete & (
                F.col("order_id").isNull()
                | (F.col("order_id") == "")
                | F.col("amount").isNull()
                | (F.col("amount") < 0)
                | (~F.col("payment_method").isin(valid_methods))
                | (~F.col("payment_status").isin(valid_statuses))
            ))
        )

    # 3. Cross-Entity Referential Integrity Validation (applies to non-DELETE records)
    ref_invalid = F.lit(False)
    _ref_drop_cols = []

    if entity == "orders":
        cust_table = f"{CATALOG}.silver.silver_customers"
        if spark.catalog.tableExists(cust_table):
            valid_custs = spark.table(cust_table).select(F.col("customer_id").alias("_ref_cid")).distinct()
            df = df.join(valid_custs, df["customer_id"] == valid_custs["_ref_cid"], "left")
            ref_invalid = ~is_delete & F.col("_ref_cid").isNull()
            _ref_drop_cols.append("_ref_cid")

    elif entity == "order_items":
        ord_table = f"{CATALOG}.silver.silver_orders"
        if spark.catalog.tableExists(ord_table):
            valid_orders = spark.table(ord_table).select(F.col("order_id").alias("_ref_oid")).distinct()
            df = df.join(valid_orders, df["order_id"] == valid_orders["_ref_oid"], "left")
            ref_invalid = ref_invalid | (~is_delete & F.col("_ref_oid").isNull())
            _ref_drop_cols.append("_ref_oid")

        prod_table = f"{CATALOG}.silver.silver_products"
        if spark.catalog.tableExists(prod_table):
            valid_prods = spark.table(prod_table).select(F.col("product_id").alias("_ref_pid")).distinct()
            df = df.join(valid_prods, df["product_id"] == valid_prods["_ref_pid"], "left")
            ref_invalid = ref_invalid | (~is_delete & F.col("_ref_pid").isNull())
            _ref_drop_cols.append("_ref_pid")

    elif entity == "payments":
        ord_table = f"{CATALOG}.silver.silver_orders"
        if spark.catalog.tableExists(ord_table):
            valid_orders = spark.table(ord_table).select(F.col("order_id").alias("_ref_oid")).distinct()
            df = df.join(valid_orders, df["order_id"] == valid_orders["_ref_oid"], "left")
            ref_invalid = ~is_delete & F.col("_ref_oid").isNull()
            _ref_drop_cols.append("_ref_oid")

    invalid_condition = cdc_invalid | entity_invalid | ref_invalid

    # 4. Quarantine Invalid Records
    quarantine_df = (
        df
        .filter(invalid_condition)
        .drop(*_ref_drop_cols)
        .withColumn(
            "_quarantine_reason",
            F.when(cdc_invalid, F.lit("INVALID_CDC_METADATA"))
            .when(entity_invalid, F.lit("DQ_RULE_FAILED"))
            .otherwise(F.lit("REFERENTIAL_INTEGRITY_VIOLATION"))
        )
        .withColumn("_quarantine_timestamp", F.current_timestamp())
    )

    q_count = quarantine_df.count()
    if q_count > 0:
        if spark.catalog.tableExists(QUARANTINE_TABLE) and batch_ids:
            batch_predicates = " OR ".join([f"_batch_id = '{b}'" for b in batch_ids])
            spark.sql(f"DELETE FROM {QUARANTINE_TABLE} WHERE {batch_predicates}")

        (
            quarantine_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(QUARANTINE_TABLE)
        )

    # 5. Deduplicate & Write Valid Records to Silver
    valid_df = df.filter(~invalid_condition).drop(*_ref_drop_cols)

    # Micro-batch deduplication: keep latest event per primary key within the batch
    dedup_cols = []
    if "event_timestamp" in valid_df.columns:
        dedup_cols.append(F.col("event_timestamp").desc_nulls_last())
    if "updated_at" in valid_df.columns:
        dedup_cols.append(F.col("updated_at").desc_nulls_last())
    dedup_cols.append(F.col("_ingestion_timestamp").desc())

    window = Window.partitionBy(primary_key, "_batch_id").orderBy(*dedup_cols)
    valid_df = (
        valid_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    v_count = valid_df.count()
    if v_count > 0:
        if spark.catalog.tableExists(SILVER_TABLE) and batch_ids:
            batch_predicates = " OR ".join([f"_batch_id = '{b}'" for b in batch_ids])
            spark.sql(f"DELETE FROM {SILVER_TABLE} WHERE {batch_predicates}")

        (
            valid_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(SILVER_TABLE)
        )

    print(f"Micro-batch {batch_idx} ({batch_label}) finished: {v_count} valid records written to {SILVER_TABLE}, {q_count} quarantined to {QUARANTINE_TABLE}.")

# COMMAND ----------

# DBTITLE 1,Execution: Incremental Stream vs Manual Override
# EXECUTION: TRUE INCREMENTAL STREAM VS MANUAL OVERRIDE

# True Incremental Mode: Read only newly committed Delta files via streaming checkpoint
print(f"Starting true incremental read on {BRONZE_TABLE} via Delta streaming...")
incremental_query = (
    spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .table(BRONZE_TABLE)
        .writeStream
        .format("delta")
        .foreachBatch(clean_and_write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
)
incremental_query.awaitTermination()
print(f"Incremental cleaning completed successfully for {entity}.")