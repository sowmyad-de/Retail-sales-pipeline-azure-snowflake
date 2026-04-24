# =============================================================================
# Retail Sales Pipeline - PySpark Transformation Script
# =============================================================================
# Description : Reads raw retail transaction data from Azure Data Lake Storage,
#               applies data quality rules, standardises formats, derives
#               business metrics, and writes clean output to Delta Lake tables
#               following the Medallion Architecture pattern.
#
# Layers      : Bronze (raw) → Silver (cleaned) → Gold (aggregated)
# Author      : Sowmya D
# Environment : Azure Databricks (Spark 3.x)
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType
)
from delta.tables import DeltaTable
import logging

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RetailPipeline")

# -----------------------------------------------------------------------------
# Spark Session
# Configured for Delta Lake support on Azure Databricks
# -----------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RetailSalesPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session initialised successfully.")

# -----------------------------------------------------------------------------
# Configuration — Paths
# In production these would be passed as Databricks widgets / job parameters
# -----------------------------------------------------------------------------
BRONZE_PATH = "abfss://raw@retaildatalake.dfs.core.windows.net/transactions/"
SILVER_PATH = "abfss://curated@retaildatalake.dfs.core.windows.net/silver/transactions/"
GOLD_PATH   = "abfss://curated@retaildatalake.dfs.core.windows.net/gold/sales_summary/"

# For local/demo runs using the sample CSV included in this repo
SAMPLE_DATA_PATH = "../data/sample_transactions.csv"

# -----------------------------------------------------------------------------
# Schema Definition
# Explicitly defining schema avoids Spark's schema inference cost on large files
# and catches upstream data type changes early
# -----------------------------------------------------------------------------
raw_schema = StructType([
    StructField("order_id",        StringType(),  True),
    StructField("customer_id",     StringType(),  True),
    StructField("product_id",      StringType(),  True),
    StructField("product_name",    StringType(),  True),
    StructField("category",        StringType(),  True),
    StructField("quantity",        IntegerType(), True),
    StructField("unit_price",      DoubleType(),  True),
    StructField("discount_pct",    DoubleType(),  True),
    StructField("order_date",      StringType(),  True),   # String first — mixed formats
    StructField("store_region",    StringType(),  True),
    StructField("payment_method",  StringType(),  True),
    StructField("status",          StringType(),  True),
])

# =============================================================================
# BRONZE LAYER — Raw Ingestion
# Read raw CSV from ADLS, apply schema, add ingestion metadata
# =============================================================================
def read_bronze(path: str):
    logger.info(f"Reading raw data from: {path}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(raw_schema) \
        .csv(path)

    # Add ingestion timestamp for audit and lineage tracking
    df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
           .withColumn("source_file", F.input_file_name())

    row_count = df.count()
    logger.info(f"Bronze layer: {row_count} rows ingested.")
    return df


# =============================================================================
# SILVER LAYER — Cleaning & Standardisation
# Applies data quality rules before writing to curated zone
# =============================================================================
def transform_to_silver(df):
    logger.info("Applying Silver layer transformations...")

    # -------------------------------------------------------------------------
    # 1. Remove duplicate orders
    # Duplicates can arise from API retry logic on the source system
    # We keep the first occurrence based on order_id
    # -------------------------------------------------------------------------
    before = df.count()
    df = df.dropDuplicates(["order_id"])
    after = df.count()
    logger.info(f"Deduplication: removed {before - after} duplicate records.")

    # -------------------------------------------------------------------------
    # 2. Standardise date formats
    # Source sends dates in two formats: yyyy-MM-dd and dd-MM-yyyy
    # We normalise everything to yyyy-MM-dd before casting to DateType
    # -------------------------------------------------------------------------
    df = df.withColumn(
        "order_date_clean",
        F.when(
            F.col("order_date").rlike(r"^\d{2}-\d{2}-\d{4}$"),
            F.to_date(F.col("order_date"), "dd-MM-yyyy")
        ).otherwise(
            F.to_date(F.col("order_date"), "yyyy-MM-dd")
        )
    ).drop("order_date") \
     .withColumnRenamed("order_date_clean", "order_date")

    invalid_dates = df.filter(F.col("order_date").isNull()).count()
    logger.info(f"Date standardisation: {invalid_dates} rows with unparseable dates.")

    # -------------------------------------------------------------------------
    # 3. Handle nulls
    # discount_pct null → 0.0  (no discount applied)
    # store_region null → 'Unknown'  (region not captured at source)
    # -------------------------------------------------------------------------
    df = df.fillna({
        "discount_pct": 0.0,
        "store_region": "Unknown"
    })

    # -------------------------------------------------------------------------
    # 4. Drop rows with critical nulls
    # Records missing order_id, customer_id, or unit_price cannot be used
    # for any downstream analytics — safe to exclude with logging
    # -------------------------------------------------------------------------
    critical_nulls = df.filter(
        F.col("order_id").isNull() |
        F.col("customer_id").isNull() |
        F.col("unit_price").isNull()
    ).count()

    if critical_nulls > 0:
        logger.warning(f"Dropping {critical_nulls} rows with critical null values.")
        df = df.filter(
            F.col("order_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("unit_price").isNotNull()
        )
# -------------------------------------------------------------------------
    # 4b. Filter out return transactions (negative quantity)
    # Returns are logged separately — not deleted from source
    # but excluded from sales analytics
    # -------------------------------------------------------------------------
    returns_df = df.filter(F.col("quantity") < 0)
    logger.info(f"Returns identified: {returns_df.count()} rows separated.")
    df = df.filter(F.col("quantity") > 0)

    # -------------------------------------------------------------------------
    # 4c. Filter out zero or null unit_price
    # Cannot calculate order value without a valid price
    # -------------------------------------------------------------------------
    df = df.filter(
        F.col("unit_price").isNotNull() &
        (F.col("unit_price") > 0)
    )
    logger.info(f"After price filter: {df.count()} rows remaining.")
    # -------------------------------------------------------------------------
    # 5. Derive business metrics
    # total_order_value = quantity × unit_price × (1 - discount_pct / 100)
    # This is the core KPI used in all downstream Gold layer aggregations
    # -------------------------------------------------------------------------
    df = df.withColumn(
        "total_order_value",
        F.round(
            F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100),
            2
        )
    )

    # -------------------------------------------------------------------------
    # 6. Add derived date parts for partitioning and time-series analysis
    # -------------------------------------------------------------------------
    df = df.withColumn("order_year",  F.year("order_date")) \
           .withColumn("order_month", F.month("order_date")) \
           .withColumn("order_day",   F.dayofmonth("order_date"))

    # -------------------------------------------------------------------------
    # 7. Standardise string columns — trim whitespace, title case category
    # -------------------------------------------------------------------------
    df = df.withColumn("category",      F.initcap(F.trim(F.col("category")))) \
           .withColumn("store_region",  F.initcap(F.trim(F.col("store_region")))) \
           .withColumn("payment_method",F.initcap(F.trim(F.col("payment_method"))))

    logger.info(f"Silver layer: {df.count()} clean rows ready for write.")
    return df


# =============================================================================
# GOLD LAYER — Aggregations for BI & Reporting
# Monthly revenue summary by category and region
# =============================================================================
def transform_to_gold(df):
    logger.info("Building Gold layer aggregations...")

    # Monthly revenue by category
    monthly_revenue = df.groupBy("order_year", "order_month", "category") \
        .agg(
            F.sum("total_order_value").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("total_order_value").alias("avg_order_value")
        ) \
        .withColumn("avg_order_value", F.round(F.col("avg_order_value"), 2)) \
        .withColumn("total_revenue",   F.round(F.col("total_revenue"), 2)) \
        .orderBy("order_year", "order_month", "category")

    logger.info(f"Gold layer: {monthly_revenue.count()} summary rows generated.")
    return monthly_revenue


# =============================================================================
# DELTA LAKE WRITE — Silver Layer (MERGE / Upsert pattern)
# Using MERGE INTO to support incremental loads without full rewrites
# This is the same pattern used in production for idempotent pipeline runs
# =============================================================================
def write_silver_delta(df, path: str):
    logger.info(f"Writing Silver Delta table to: {path}")

    if DeltaTable.isDeltaTable(spark, path):
        logger.info("Delta table exists — performing MERGE (upsert).")
        delta_table = DeltaTable.forPath(spark, path)

        delta_table.alias("target").merge(
            df.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        logger.info("MERGE complete.")
    else:
        logger.info("Delta table not found — performing initial full write.")
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .partitionBy("order_year", "order_month") \
          .save(path)

        logger.info("Initial write complete.")


# =============================================================================
# DELTA LAKE WRITE — Gold Layer (overwrite on each run)
# Gold aggregations are always fully recalculated from Silver
# =============================================================================
def write_gold_delta(df, path: str):
    logger.info(f"Writing Gold Delta table to: {path}")

    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save(path)

    logger.info("Gold layer write complete.")


# =============================================================================
# DELTA LAKE TIME TRAVEL — Audit & Recovery
# Demonstrates ability to query previous versions of the table
# Useful for debugging pipeline issues or regulatory audit requirements
# =============================================================================
def demonstrate_time_travel(path: str):
    logger.info("Demonstrating Delta Lake time travel...")

    # Query table as of version 0 (initial load)
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
    logger.info(f"Version 0 row count: {df_v0.count()}")

    # Show full Delta history
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.history().select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)


# =============================================================================
# MAIN PIPELINE EXECUTION
# =============================================================================
def run_pipeline():
    logger.info("=" * 60)
    logger.info("Retail Sales Pipeline — Starting execution")
    logger.info("=" * 60)

    # Step 1: Read Bronze
    bronze_df = read_bronze(SAMPLE_DATA_PATH)
    bronze_df.printSchema()
    bronze_df.show(5, truncate=False)

    # Step 2: Transform to Silver
    silver_df = transform_to_silver(bronze_df)
    silver_df.select(
        "order_id", "customer_id", "category",
        "order_date", "total_order_value", "store_region"
    ).show(10, truncate=False)

    # Step 3: Write Silver to Delta Lake
    write_silver_delta(silver_df, SILVER_PATH)

    # Step 4: Transform to Gold
    gold_df = transform_to_gold(silver_df)
    gold_df.show(20, truncate=False)

    # Step 5: Write Gold to Delta Lake
    write_gold_delta(gold_df, GOLD_PATH)

    # Step 6: Demonstrate Time Travel (audit capability)
    demonstrate_time_travel(SILVER_PATH)

    logger.info("=" * 60)
    logger.info("Pipeline execution completed successfully.")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
