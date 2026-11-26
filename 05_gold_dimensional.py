# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "ecommerce_pipeline"
SILVER_SCHEMA = f"{CATALOG}.silver"
DIM_SCHEMA = f"{CATALOG}.gold_dimensional"
FACT_SCHEMA = f"{CATALOG}.gold_facts"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DIM_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FACT_SCHEMA}")

CUSTOMERS_TABLE = f"{SILVER_SCHEMA}.silver2_customers"
PRODUCTS_TABLE = f"{SILVER_SCHEMA}.silver2_products"
ORDERS_TABLE = f"{SILVER_SCHEMA}.silver2_orders"
ORDER_ITEMS_TABLE = f"{SILVER_SCHEMA}.silver2_order_items"
PAYMENTS_TABLE = f"{SILVER_SCHEMA}.silver2_payments"

DIM_DATE_TABLE = f"{DIM_SCHEMA}.dim_date"
DIM_CUSTOMER_TABLE = f"{DIM_SCHEMA}.dim_customer"
DIM_PRODUCT_TABLE = f"{DIM_SCHEMA}.dim_product"
FACT_SALES_TABLE = f"{FACT_SCHEMA}.fact_sales"
FACT_PAYMENTS_TABLE = f"{FACT_SCHEMA}.fact_payments"

DIM_CUSTOMER_CHECKPOINT = f"/Volumes/{CATALOG}/default/gold_checkpoint/dim_customer"
DIM_PRODUCT_CHECKPOINT = f"/Volumes/{CATALOG}/default/gold_checkpoint/dim_product"
FACT_SALES_CHECKPOINT = f"/Volumes/{CATALOG}/default/gold_checkpoint/fact_sales"
FACT_PAYMENTS_CHECKPOINT = f"/Volumes/{CATALOG}/default/gold_checkpoint/fact_payments"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.default.gold_checkpoint")

dbutils.fs.mkdirs(DIM_CUSTOMER_CHECKPOINT)
dbutils.fs.mkdirs(DIM_PRODUCT_CHECKPOINT)
dbutils.fs.mkdirs(FACT_SALES_CHECKPOINT)
dbutils.fs.mkdirs(FACT_PAYMENTS_CHECKPOINT)

# COMMAND ----------

# DIMENSION: DATE (Static Dimension)

if not spark.catalog.tableExists(DIM_DATE_TABLE):
    date_df = (
        spark.sql("""
            SELECT explode(
                sequence(
                    to_date('2024-01-01'),
                    to_date('2030-12-31'),
                    interval 1 day
                )
            ) AS full_date
        """)
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
    )

    (
        date_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(DIM_DATE_TABLE)
    )
    print(f"Created static dimension table: {DIM_DATE_TABLE}")
else:
    print(f"Dimension table {DIM_DATE_TABLE} already exists. Skipping date generation.")

# COMMAND ----------

# HELPER: ENSURE UNKNOWN MEMBER IN DIMENSION

def ensure_unknown_customer():
    if not spark.catalog.tableExists(DIM_CUSTOMER_TABLE):
        unknown_df = spark.sql("""
            SELECT
                CAST(0 AS LONG)         AS customer_key,
                'UNKNOWN'                AS customer_id,
                'Unknown'                AS first_name,
                'Customer'               AS last_name,
                'unknown@unknown.com'    AS email,
                CAST(NULL AS STRING)     AS phone,
                'Unknown'                AS city,
                'Unknown'                AS state,
                'Unknown'                AS country,
                CAST(NULL AS DATE)       AS signup_date,
                TIMESTAMP '1900-01-01 00:00:00' AS effective_start,
                TIMESTAMP '9999-12-31 23:59:59' AS effective_end,
                TRUE                     AS is_current
        """)
        unknown_df.write.format("delta").mode("overwrite").saveAsTable(DIM_CUSTOMER_TABLE)

def ensure_unknown_product():
    if not spark.catalog.tableExists(DIM_PRODUCT_TABLE):
        unknown_df = spark.sql("""
            SELECT
                CAST(0 AS LONG)          AS product_key,
                'UNKNOWN'                AS product_id,
                'Unknown Product'        AS product_name,
                'Unknown'                AS category,
                'Unknown'                AS brand,
                CAST(0 AS DECIMAL(18,2)) AS price,
                CAST(0 AS INT)           AS stock_quantity,
                'Unknown'                AS product_status,
                TIMESTAMP '1900-01-01 00:00:00' AS effective_start,
                TIMESTAMP '9999-12-31 23:59:59' AS effective_end,
                TRUE                     AS is_current
        """)
        unknown_df.write.format("delta").mode("overwrite").saveAsTable(DIM_PRODUCT_TABLE)

ensure_unknown_customer()
ensure_unknown_product()

# COMMAND ----------

# DIMENSION: CUSTOMER (INCREMENTAL MERGE)

def process_dim_customer_batch(micro_batch_df, batch_idx=0):
    if micro_batch_df.isEmpty():
        return

    dim_cust_batch = (
        micro_batch_df
        .withColumn("customer_key", F.xxhash64(F.col("customer_id"), F.col("effective_start")))
        .select(
            "customer_key", "customer_id", "first_name", "last_name",
            "email", "phone", "city", "state", "country", "signup_date",
            "effective_start", "effective_end", "is_current"
        )
    )

    target_table = DeltaTable.forName(spark, DIM_CUSTOMER_TABLE)
    (
        target_table.alias("target")
        .merge(
            dim_cust_batch.alias("source"),
            "target.customer_key = source.customer_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Incremental batch {batch_idx}: Processed {dim_cust_batch.count()} records into {DIM_CUSTOMER_TABLE}")

dim_customer_query = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .table(CUSTOMERS_TABLE)
    .writeStream
    .foreachBatch(process_dim_customer_batch)
    .option("checkpointLocation", DIM_CUSTOMER_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
dim_customer_query.awaitTermination()

# COMMAND ----------

# DIMENSION: PRODUCT (INCREMENTAL MERGE)

def process_dim_product_batch(micro_batch_df, batch_idx=0):
    if micro_batch_df.isEmpty():
        return

    dim_prod_batch = (
        micro_batch_df
        .withColumn("product_key", F.xxhash64(F.col("product_id"), F.col("effective_start")))
        .select(
            "product_key", "product_id", "product_name", "category",
            "brand", "price", "stock_quantity", "product_status",
            "effective_start", "effective_end", "is_current"
        )
    )

    target_table = DeltaTable.forName(spark, DIM_PRODUCT_TABLE)
    (
        target_table.alias("target")
        .merge(
            dim_prod_batch.alias("source"),
            "target.product_key = source.product_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Incremental batch {batch_idx}: Processed {dim_prod_batch.count()} records into {DIM_PRODUCT_TABLE}")

dim_product_query = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .table(PRODUCTS_TABLE)
    .writeStream
    .foreachBatch(process_dim_product_batch)
    .option("checkpointLocation", DIM_PRODUCT_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
dim_product_query.awaitTermination()

# COMMAND ----------

# FACT: SALES (INCREMENTAL TEMPORAL MERGE)

def process_fact_sales_batch(micro_batch_df, batch_idx=0):
    if micro_batch_df.isEmpty():
        return

    dim_date_curr = spark.table(DIM_DATE_TABLE)
    dim_cust_curr = spark.table(DIM_CUSTOMER_TABLE)
    dim_prod_curr = spark.table(DIM_PRODUCT_TABLE)
    orders_curr = spark.table(ORDERS_TABLE)

    fact_sales_batch = (
        micro_batch_df.alias("oi")
        .join(
            orders_curr.alias("o"),
            F.col("oi.order_id") == F.col("o.order_id"),
            "inner"
        )
        .join(
            dim_date_curr.alias("d"),
            F.to_date(F.col("o.order_date")) == F.col("d.full_date"),
            "left"
        )
        .join(
            dim_cust_curr.alias("c"),
            (
                (F.col("o.customer_id") == F.col("c.customer_id"))
                & (F.col("o.order_date") >= F.col("c.effective_start"))
                & (F.col("o.order_date") < F.col("c.effective_end"))
            ),
            "left"
        )
        .join(
            dim_prod_curr.alias("p"),
            (
                (F.col("oi.product_id") == F.col("p.product_id"))
                & (F.col("o.order_date") >= F.col("p.effective_start"))
                & (F.col("o.order_date") < F.col("p.effective_end"))
            ),
            "left"
        )
        .select(
            F.xxhash64(F.col("oi.order_item_id")).alias("sales_key"),
            F.col("oi.order_id").alias("order_id"),
            F.col("oi.order_item_id").alias("order_item_id"),
            F.coalesce(F.col("c.customer_key"), F.lit(0)).alias("customer_key"),
            F.coalesce(F.col("p.product_key"), F.lit(0)).alias("product_key"),
            F.col("d.date_key"),
            F.col("o.order_date"),
            F.col("oi.quantity"),
            F.col("oi.unit_price"),
            F.col("oi.discount"),
            F.col("oi.line_total"),
            F.col("o.order_status").alias("order_status")
        )
    )

    if not spark.catalog.tableExists(FACT_SALES_TABLE):
        fact_sales_batch.write.format("delta").mode("overwrite").saveAsTable(FACT_SALES_TABLE)
    else:
        target_table = DeltaTable.forName(spark, FACT_SALES_TABLE)
        (
            target_table.alias("target")
            .merge(
                fact_sales_batch.alias("source"),
                "target.sales_key = source.sales_key"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    print(f"Incremental batch {batch_idx}: Processed {fact_sales_batch.count()} records into {FACT_SALES_TABLE}")

fact_sales_query = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .table(ORDER_ITEMS_TABLE)
    .writeStream
    .foreachBatch(process_fact_sales_batch)
    .option("checkpointLocation", FACT_SALES_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
fact_sales_query.awaitTermination()

# COMMAND ----------

# FACT: PAYMENTS (INCREMENTAL TEMPORAL MERGE)

def process_fact_payments_batch(micro_batch_df, batch_idx=0):
    if micro_batch_df.isEmpty():
        return

    dim_date_curr = spark.table(DIM_DATE_TABLE)
    dim_cust_curr = spark.table(DIM_CUSTOMER_TABLE)
    orders_curr = spark.table(ORDERS_TABLE)

    fact_payments_batch = (
        micro_batch_df.alias("pmt")
        .join(
            orders_curr.alias("o"),
            F.col("pmt.order_id") == F.col("o.order_id"),
            "left"
        )
        .join(
            dim_date_curr.alias("d"),
            F.to_date(F.col("pmt.payment_date")) == F.col("d.full_date"),
            "left"
        )
        .join(
            dim_cust_curr.alias("c"),
            (
                (F.col("o.customer_id") == F.col("c.customer_id"))
                & (F.col("pmt.payment_date") >= F.col("c.effective_start"))
                & (F.col("pmt.payment_date") < F.col("c.effective_end"))
            ),
            "left"
        )
        .select(
            F.xxhash64(F.col("pmt.payment_id")).alias("payment_key"),
            F.col("pmt.payment_id").alias("payment_id"),
            F.col("pmt.order_id").alias("order_id"),
            F.coalesce(F.col("c.customer_key"), F.lit(0)).alias("customer_key"),
            F.col("d.date_key"),
            F.col("pmt.payment_date"),
            F.col("pmt.payment_method"),
            F.col("pmt.amount"),
            F.col("pmt.payment_status")
        )
    )

    if not spark.catalog.tableExists(FACT_PAYMENTS_TABLE):
        fact_payments_batch.write.format("delta").mode("overwrite").saveAsTable(FACT_PAYMENTS_TABLE)
    else:
        target_table = DeltaTable.forName(spark, FACT_PAYMENTS_TABLE)
        (
            target_table.alias("target")
            .merge(
                fact_payments_batch.alias("source"),
                "target.payment_key = source.payment_key"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    print(f"Incremental batch {batch_idx}: Processed {fact_payments_batch.count()} records into {FACT_PAYMENTS_TABLE}")

fact_payments_query = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .table(PAYMENTS_TABLE)
    .writeStream
    .foreachBatch(process_fact_payments_batch)
    .option("checkpointLocation", FACT_PAYMENTS_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
fact_payments_query.awaitTermination()

# COMMAND ----------

# VALIDATION: GRAIN & SURROGATE KEY INTEGRITY

fact_sales = spark.table(FACT_SALES_TABLE)
null_cust_keys = fact_sales.filter(F.col("customer_key").isNull()).count()
null_prod_keys = fact_sales.filter(F.col("product_key").isNull()).count()
null_date_keys = fact_sales.filter(F.col("date_key").isNull()).count()

assert null_cust_keys == 0, f"fact_sales contains {null_cust_keys} NULL customer_key references"
assert null_prod_keys == 0, f"fact_sales contains {null_prod_keys} NULL product_key references"
assert null_date_keys == 0, f"fact_sales contains {null_date_keys} NULL date_key references"

print("Incremental Gold dimensional modeling completed successfully:")
print(f"  Dimensions: {DIM_DATE_TABLE} ({spark.table(DIM_DATE_TABLE).count()} rows)")
print(f"              {DIM_CUSTOMER_TABLE} ({spark.table(DIM_CUSTOMER_TABLE).count()} rows)")
print(f"              {DIM_PRODUCT_TABLE} ({spark.table(DIM_PRODUCT_TABLE).count()} rows)")
print(f"  Facts     : {FACT_SALES_TABLE} ({spark.table(FACT_SALES_TABLE).count()} rows)")
print(f"              {FACT_PAYMENTS_TABLE} ({spark.table(FACT_PAYMENTS_TABLE).count()} rows)")
print("Surrogate key completeness: 100% (No orphaned facts).")