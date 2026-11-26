from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SCD2_OPEN_TIMESTAMP = "9999-12-31 23:59:59"


def process_scd1(spark, source_df, target_table_name, primary_key):
    """
    Applies SCD Type 1 logic with CDC awareness:
    - INSERT: Insert new row
    - UPDATE: Overwrite existing row in-place
    - DELETE: Remove existing row
    - Same data: Idempotent no-op via Delta MERGE
    """
    # 1. Standardize operation & event timestamp
    if "operation" in source_df.columns:
        source_df = source_df.withColumn("operation", F.upper(F.trim(F.col("operation"))))
    else:
        source_df = source_df.withColumn("operation", F.lit("INSERT"))

    if "event_timestamp" in source_df.columns:
        source_df = source_df.withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))

    # 2. Deduplicate incoming batch by primary key, keeping latest record
    dedup_cols = []
    if "event_timestamp" in source_df.columns:
        dedup_cols.append(F.col("event_timestamp").desc_nulls_last())
    if "updated_at" in source_df.columns:
        dedup_cols.append(F.col("updated_at").desc_nulls_last())
    dedup_cols.append(F.col("_ingestion_timestamp").desc_nulls_last())

    window = Window.partitionBy(primary_key).orderBy(*dedup_cols)
    deduped_source = (
        source_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 3. Apply changes to target Delta table
    if not spark.catalog.tableExists(target_table_name):
        (
            deduped_source
            .filter(F.col("operation") != "DELETE")
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table_name)
        )
        return

    target = DeltaTable.forName(spark, target_table_name)
    exclude_cols = {primary_key, "_ingestion_timestamp"}
    update_set = {c: f"source.{c}" for c in deduped_source.columns if c not in exclude_cols}
    insert_values = {c: f"source.{c}" for c in deduped_source.columns}

    (
        target.alias("target")
        .merge(
            deduped_source.alias("source"),
            f"target.{primary_key} = source.{primary_key}"
        )
        .whenMatchedDelete(condition="source.operation = 'DELETE'")
        .whenMatchedUpdate(condition="source.operation != 'DELETE'", set=update_set)
        .whenNotMatchedInsert(condition="source.operation != 'DELETE'", values=insert_values)
        .execute()
    )


def process_scd2(spark, source_df, target_table_name, primary_key):
    """
    Applies SCD Type 2 logic with CDC awareness:
    - INSERT: Create new active version (effective_end=9999-12-31, is_current=true)
    - UPDATE: Expire old version, insert new active version
    - DELETE: Expire current active version (no new version inserted)
    - RE-INSERT: Insert new active version after deletion
    - NO CHANGE: Idempotent no-op (hash match produces no new version)
    """
    # 1. Standardize operation & event timestamp
    if "operation" in source_df.columns:
        source_df = source_df.withColumn("operation", F.upper(F.trim(F.col("operation"))))
    else:
        source_df = source_df.withColumn("operation", F.lit("INSERT"))

    if "event_timestamp" in source_df.columns:
        source_df = source_df.withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))

    # 2. Add SCD2 tracking columns
    source_df = (
        source_df
        .withColumn(
            "effective_start",
            F.coalesce(
                F.col("event_timestamp") if "event_timestamp" in source_df.columns else F.lit(None).cast("timestamp"),
                F.col("updated_at") if "updated_at" in source_df.columns else F.lit(None).cast("timestamp"),
                F.col("_ingestion_timestamp")
            )
        )
        .withColumn("effective_end", F.to_timestamp(F.lit(SCD2_OPEN_TIMESTAMP)))
        .withColumn("is_current", F.lit(True))
    )

    # 3. Deduplicate batch by primary key, keeping latest record
    dedup_cols = []
    if "event_timestamp" in source_df.columns:
        dedup_cols.append(F.col("event_timestamp").desc_nulls_last())
    if "updated_at" in source_df.columns:
        dedup_cols.append(F.col("updated_at").desc_nulls_last())
    dedup_cols.append(F.col("_ingestion_timestamp").desc_nulls_last())

    window = Window.partitionBy(primary_key).orderBy(*dedup_cols)
    deduped_source = (
        source_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 4. Handle initial load
    if not spark.catalog.tableExists(target_table_name):
        initial_records = deduped_source.filter(F.col("operation") != "DELETE")
        (
            initial_records
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table_name)
        )
        return

    # 5. Define comparison columns and compute deterministic SHA-256 hash
    exclude_cols = {
        primary_key, "operation", "event_timestamp", "effective_start",
        "effective_end", "is_current", "updated_at", "created_at",
        "_rescued_data", "_batch_id", "_ingestion_timestamp",
        "_source_file", "_source_entity"
    }
    comparison_cols = sorted([c for c in deduped_source.columns if c not in exclude_cols])
    hash_expr = F.sha2(F.to_json(F.struct(*[F.col(c).alias(c) for c in comparison_cols])), 256)

    source_hashed = deduped_source.withColumn("_record_hash", hash_expr)
    target_current = (
        spark.table(target_table_name)
        .filter(F.col("is_current") == True)
        .withColumn("_record_hash", hash_expr)
    )

    # 6. Categorize incoming records:
    # A. Deletes: expire current record
    deletes_df = (
        source_hashed
        .filter(F.col("operation") == "DELETE")
        .select(
            F.col(primary_key).alias("pk"),
            F.col("effective_start").alias("expire_timestamp")
        )
    )

    non_deletes = source_hashed.filter(F.col("operation") != "DELETE")

    # B. Inserts: brand-new keys OR re-inserted keys after previous deletion
    new_records_df = (
        non_deletes.alias("source")
        .join(
            target_current.alias("target"),
            F.col(f"source.{primary_key}") == F.col(f"target.{primary_key}"),
            "left_anti"
        )
        .select("source.*")
    )

    # C. Updates: existing keys with modified attributes (hash mismatch)
    changed_records_df = (
        non_deletes.alias("source")
        .join(
            target_current.alias("target"),
            (F.col(f"source.{primary_key}") == F.col(f"target.{primary_key}"))
            & (F.col("source._record_hash") != F.col("target._record_hash")),
            "inner"
        )
        .select("source.*")
    )

    # Note: Unchanged records (identical hash) naturally fall out: no expiration, no insertion.

    # 7. Expire existing active versions in Delta table
    updates_to_expire = changed_records_df.select(
        F.col(primary_key).alias("pk"),
        F.col("effective_start").alias("expire_timestamp")
    )
    all_to_expire = updates_to_expire.unionByName(deletes_df)

    if all_to_expire.count() > 0:
        target_table = DeltaTable.forName(spark, target_table_name)
        (
            target_table.alias("target")
            .merge(
                all_to_expire.alias("exp"),
                f"target.{primary_key} = exp.pk AND target.is_current = true"
            )
            .whenMatchedUpdate(
                set={
                    "effective_end": "exp.expire_timestamp",
                    "is_current": "false"
                }
            )
            .execute()
        )

    # 8. Append new active versions (brand new keys + updated versions)
    records_to_insert = (
        new_records_df
        .unionByName(changed_records_df)
        .drop("_record_hash")
    )

    if records_to_insert.count() > 0:
        (
            records_to_insert
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table_name)
        )


def validate_scd2_integrity(spark, target_table_name, primary_key):
    """
    Validates complete SCD2 correctness invariants:
    1. Exactly 0 or 1 active record per business key
    2. effective_start < effective_end for all rows
    3. Active records have exact open-ended date (9999-12-31 23:59:59)
    4. Historical records are properly closed (< 9999-12-31 23:59:59)
    5. No overlapping validity intervals per business key
    """
    target_df = spark.table(target_table_name)

    # Check 1: Exactly 0 or 1 active record per business key
    dupes = (
        target_df.filter(F.col("is_current") == True)
        .groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    assert dupes == 0, f"SCD2 Check 1 Failed: {dupes} keys have multiple active records in {target_table_name}"

    # Check 2: Validity period: effective_start < effective_end
    invalid_dates = target_df.filter(F.col("effective_end") <= F.col("effective_start")).count()
    assert invalid_dates == 0, f"SCD2 Check 2 Failed: {invalid_dates} rows have effective_end <= effective_start in {target_table_name}"

    # Check 3: Current records have exact open-ended timestamp (9999-12-31 23:59:59)
    open_ts = F.to_timestamp(F.lit(SCD2_OPEN_TIMESTAMP))
    invalid_current = (
        target_df.filter(
            (F.col("is_current") == True)
            & (F.col("effective_end") != open_ts)
        ).count()
    )
    assert invalid_current == 0, f"SCD2 Check 3 Failed: {invalid_current} active records lack exact open-ended date in {target_table_name}"

    # Check 4: Historical records are closed (effective_end < 9999-12-31 23:59:59)
    invalid_historical = (
        target_df.filter(
            (F.col("is_current") == False)
            & (F.col("effective_end") >= open_ts)
        ).count()
    )
    assert invalid_historical == 0, f"SCD2 Check 4 Failed: {invalid_historical} historical records have open-ended date in {target_table_name}"

    # Check 5: No overlapping validity intervals per business key
    lead_window = Window.partitionBy(primary_key).orderBy("effective_start")
    overlaps = (
        target_df
        .withColumn("next_effective_start", F.lead("effective_start").over(lead_window))
        .filter(
            F.col("next_effective_start").isNotNull()
            & (F.col("effective_end") > F.col("next_effective_start"))
        )
        .count()
    )
    assert overlaps == 0, f"SCD2 Check 5 Failed: {overlaps} overlapping intervals found in {target_table_name}"

    print(f"SCD2 Integrity Validation PASSED for {target_table_name} ({target_df.count()} total records, {target_df.filter(F.col('is_current') == True).count()} active).")
