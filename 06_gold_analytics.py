# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "ecommerce_pipeline"
DIM_SCHEMA = f"{CATALOG}.gold_dimensional"
FACT_SCHEMA = f"{CATALOG}.gold_facts"
ANALYTICS_SCHEMA = f"{CATALOG}.gold_analytics"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS_SCHEMA}")

fact_sales = spark.table(f"{FACT_SCHEMA}.fact_sales")
fact_payments = spark.table(f"{FACT_SCHEMA}.fact_payments")
dim_date = spark.table(f"{DIM_SCHEMA}.dim_date")
dim_customer = spark.table(f"{DIM_SCHEMA}.dim_customer")
dim_product = spark.table(f"{DIM_SCHEMA}.dim_product")

# COMMAND ----------

# 1. DAILY SALES ANALYTICS

daily_sales = (
    fact_sales.alias("s")
    .join(dim_date.alias("d"), F.col("s.date_key") == F.col("d.date_key"), "inner")
    .groupBy(
        F.col("d.full_date").alias("order_date"),
        F.col("d.year"),
        F.col("d.month"),
        F.col("d.month_name"),
        F.col("d.day_name"),
        F.col("d.is_weekend")
    )
    .agg(
        F.countDistinct("s.order_id").alias("total_orders"),
        F.sum("s.quantity").alias("total_items_sold"),
        F.round(F.sum("s.line_total"), 2).alias("total_revenue"),
        F.round(F.sum("s.discount"), 2).alias("total_discount"),
        F.round(F.sum("s.line_total") / F.countDistinct("s.order_id"), 2).alias("avg_order_value")
    )
    .orderBy("order_date")
)

(
    daily_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ANALYTICS_SCHEMA}.gold_daily_sales")
)

# COMMAND ----------

# 2. CATEGORY SALES ANALYTICS

category_sales = (
    fact_sales.alias("s")
    .join(dim_product.alias("p"), F.col("s.product_key") == F.col("p.product_key"), "left")
    .groupBy(
        F.coalesce(F.col("p.category"), F.lit("Unknown")).alias("category"),
        F.coalesce(F.col("p.brand"), F.lit("Unknown")).alias("brand")
    )
    .agg(
        F.countDistinct("s.order_id").alias("total_orders"),
        F.sum("s.quantity").alias("units_sold"),
        F.round(F.sum("s.line_total"), 2).alias("total_revenue"),
        F.round(F.avg("s.unit_price"), 2).alias("avg_unit_price")
    )
    .orderBy(F.col("total_revenue").desc())
)

(
    category_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ANALYTICS_SCHEMA}.gold_category_sales")
)

# COMMAND ----------

# 3. CUSTOMER METRICS ANALYTICS

# Step A: Aggregate sales metrics across all historical customer versions
cust_sales_summary = (
    fact_sales.alias("s")
    .join(dim_customer.alias("c"), F.col("s.customer_key") == F.col("c.customer_key"), "inner")
    .groupBy("c.customer_id")
    .agg(
        F.countDistinct("s.order_id").alias("total_orders"),
        F.sum("s.quantity").alias("total_units_purchased"),
        F.round(F.sum("s.line_total"), 2).alias("lifetime_spend"),
        F.round(F.sum("s.line_total") / F.countDistinct("s.order_id"), 2).alias("avg_order_value"),
        F.min("s.order_date").alias("first_order_date"),
        F.max("s.order_date").alias("last_order_date")
    )
)

# Step B: Retrieve latest/current customer attributes
current_customer_profile = (
    dim_customer
    .filter(F.col("is_current") == True)
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "state"
    )
)

# Step C: Combine metrics with current profile
customer_metrics = (
    cust_sales_summary.alias("m")
    .join(current_customer_profile.alias("p"), F.col("m.customer_id") == F.col("p.customer_id"), "left")
    .select(
        F.col("m.customer_id"),
        F.coalesce(F.col("p.first_name"), F.lit("Unknown")).alias("first_name"),
        F.coalesce(F.col("p.last_name"), F.lit("Unknown")).alias("last_name"),
        F.col("p.email"),
        F.col("p.phone"),
        F.col("p.city"),
        F.col("p.state"),
        F.col("m.total_orders"),
        F.col("m.total_units_purchased"),
        F.col("m.lifetime_spend"),
        F.col("m.avg_order_value"),
        F.col("m.first_order_date"),
        F.col("m.last_order_date")
    )
    .orderBy(F.col("lifetime_spend").desc())
)

(
    customer_metrics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ANALYTICS_SCHEMA}.gold_customer_metrics")
)

# COMMAND ----------

# 4. GEOGRAPHIC SALES ANALYTICS

geographic_sales = (
    fact_sales.alias("s")
    .join(dim_customer.alias("c"), F.col("s.customer_key") == F.col("c.customer_key"), "left")
    .groupBy(
        F.coalesce(F.col("c.country"), F.lit("India")).alias("country"),
        F.coalesce(F.col("c.state"), F.lit("Unknown")).alias("state"),
        F.coalesce(F.col("c.city"), F.lit("Unknown")).alias("city")
    )
    .agg(
        F.countDistinct("c.customer_id").alias("unique_customers"),
        F.countDistinct("s.order_id").alias("total_orders"),
        F.sum("s.quantity").alias("total_units_sold"),
        F.round(F.sum("s.line_total"), 2).alias("total_revenue"),
        F.round(F.sum("s.line_total") / F.countDistinct("c.customer_id"), 2).alias("avg_revenue_per_customer")
    )
    .orderBy(F.col("total_revenue").desc())
)

(
    geographic_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ANALYTICS_SCHEMA}.gold_geographic_sales")
)

# COMMAND ----------

# 5. PAYMENT PERFORMANCE ANALYTICS

payment_performance = (
    fact_payments
    .groupBy("payment_method", "payment_status")
    .agg(
        F.count("payment_id").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_transaction_amount")
    )
    .orderBy("payment_method", "payment_status")
)

(
    payment_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ANALYTICS_SCHEMA}.gold_payment_performance")
)

# COMMAND ----------

# AUDIT & REVENUE RECONCILIATION

fact_rev = fact_sales.agg(F.round(F.sum("line_total"), 2).alias("r")).collect()[0]["r"] or 0.0
daily_rev = spark.table(f"{ANALYTICS_SCHEMA}.gold_daily_sales").agg(F.round(F.sum("total_revenue"), 2).alias("r")).collect()[0]["r"] or 0.0
cat_rev = spark.table(f"{ANALYTICS_SCHEMA}.gold_category_sales").agg(F.round(F.sum("total_revenue"), 2).alias("r")).collect()[0]["r"] or 0.0
geo_rev = spark.table(f"{ANALYTICS_SCHEMA}.gold_geographic_sales").agg(F.round(F.sum("total_revenue"), 2).alias("r")).collect()[0]["r"] or 0.0

# Verify customer metrics grain: exactly 1 row per customer
cust_table = spark.table(f"{ANALYTICS_SCHEMA}.gold_customer_metrics")
cust_rows = cust_table.count()
cust_keys = cust_table.select("customer_id").distinct().count()
assert cust_rows == cust_keys, f"Grain violation in gold_customer_metrics: {cust_rows} rows vs {cust_keys} unique customers"

# Verify revenue reconciliation
assert abs(fact_rev - daily_rev) < 0.01, f"Revenue mismatch: fact {fact_rev} != daily {daily_rev}"
assert abs(fact_rev - cat_rev) < 0.01, f"Revenue mismatch: fact {fact_rev} != category {cat_rev}"
assert abs(fact_rev - geo_rev) < 0.01, f"Revenue mismatch: fact {fact_rev} != geographic {geo_rev}"

print("Gold Analytics Generated Successfully:")
print(f"  gold_daily_sales         : {spark.table(f'{ANALYTICS_SCHEMA}.gold_daily_sales').count()} rows (revenue: {daily_rev})")
print(f"  gold_category_sales      : {spark.table(f'{ANALYTICS_SCHEMA}.gold_category_sales').count()} rows (revenue: {cat_rev})")
print(f"  gold_customer_metrics    : {cust_rows} rows (verified 1 row per customer)")
print(f"  gold_geographic_sales    : {spark.table(f'{ANALYTICS_SCHEMA}.gold_geographic_sales').count()} rows (revenue: {geo_rev})")
print(f"  gold_payment_performance : {spark.table(f'{ANALYTICS_SCHEMA}.gold_payment_performance').count()} rows")
print(f"REVENUE RECONCILIATION PASSED: fact ({fact_rev}) == daily ({daily_rev}) == category ({cat_rev}) == geographic ({geo_rev})")