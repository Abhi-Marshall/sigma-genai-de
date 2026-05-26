"""
Sigma DataTech Transaction Analytics Pipeline
Fixed version for Module 5 code review.
This file replaces hardcoded paths with configurable values,
adds schema validation before transformations, and fixes dedup logic.
"""

import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    countDistinct,
    first,
    lit,
    max as max_,
    min as min_,
    row_number,
    sum as sum_,
    when,
)
from pyspark.sql.types import DateType, FloatType, StringType
from pyspark.sql.window import Window

# ── CONFIGURATION ──────────────────────────────────────────────────────────
INPUT_PATH = os.environ.get("SIGMA_INPUT_PATH", "s3://sigma-datatech/raw/transactions.csv")
MERCHANTS_PATH = os.environ.get("SIGMA_MERCHANTS_PATH", "s3://sigma-datatech/raw/merchants.csv")
BRONZE_PATH = os.environ.get("SIGMA_BRONZE_PATH", "s3://sigma-datatech/bronze/transactions")
SILVER_PATH = os.environ.get("SIGMA_SILVER_PATH", "s3://sigma-datatech/silver/transactions")
GOLD_OUTPUT_DIR = os.environ.get("SIGMA_GOLD_PATH", "s3://sigma-datatech/gold")
METADATA_DIR = os.environ.get("SIGMA_METADATA_PATH", "/tmp/sigma_metadata")

EXPECTED_BRONZE_COLUMNS = [
    "transaction_id",
    "amount",
    "status",
    "merchant_id",
    "customer_id",
    "transaction_date",
    "payment_method",
]

EXPECTED_SILVER_COLUMNS = [
    "transaction_id",
    "amount",
    "status",
    "merchant_id",
    "customer_id",
    "transaction_date",
    "payment_method",
    "merchant_name",
    "category",
    "city",
    "quality_flag",
    "ingestion_timestamp",
    "pipeline_run_id",
]


def validate_schema(df, required_columns):
    """Validate that the required columns exist in the DataFrame."""
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required schema columns: {missing}")


def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    transactions_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(input_path)
    )

    validate_schema(transactions_df, EXPECTED_BRONZE_COLUMNS)

    transactions_df = (
        transactions_df.withColumn("ingestion_timestamp", lit(run_date))
        .withColumn("source_file", lit(os.path.basename(input_path)))
        .withColumn("pipeline_run_id", lit(run_id))
    )

    transactions_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)


def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    transactions_df = (
        spark.read.format("parquet")
        .option("basePath", bronze_path)
        .load(f"{bronze_path}/{run_date}")
    )

    validate_schema(transactions_df, EXPECTED_BRONZE_COLUMNS + ["ingestion_timestamp", "pipeline_run_id"])

    transactions_df = transactions_df.withColumn("amount", col("amount").cast(FloatType()))
    transactions_df = transactions_df.withColumn("transaction_date", col("transaction_date").cast(DateType()))
    transactions_df = transactions_df.withColumn("transaction_id", col("transaction_id").cast(StringType()))
    transactions_df = transactions_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))

    transactions_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))

    window_spec = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
    transactions_df = transactions_df.withColumn("row_number", row_number().over(window_spec))
    transactions_df = transactions_df.filter(col("row_number") == 1).drop("row_number")

    merchants_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(merchants_path)
    )
    merchants_df = merchants_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))
    merchants_df = merchants_df.cache()

    transactions_df = transactions_df.join(
        broadcast(merchants_df),
        transactions_df.merchant_id == merchants_df.merchant_id,
        "left_outer",
    )

    transactions_df = transactions_df.withColumn(
        "quality_flag",
        when(merchants_df.merchant_id.isNull(), "UNMATCHED").otherwise("CLEAN"),
    )

    validate_schema(transactions_df, EXPECTED_SILVER_COLUMNS)
    transactions_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)


def build_merchant_performance(spark, silver_path, output_path, run_date, run_metadata):
    silver_df = spark.read.parquet(silver_path)
    completed_txns = silver_df.filter(col("status") == "COMPLETED")

    merchant_performance_df = (
        completed_txns.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date")
        .agg(
            sum_(col("amount")).alias("total_revenue"),
            count("*").alias("txn_count"),
            count(when(col("status") == "FAILED", 1)).alias("failed_txn_count"),
            count("*").alias("total_txn_count"),
        )
        .withColumn(
            "failure_rate_pct",
            (col("failed_txn_count") / col("total_txn_count") * 100).cast(FloatType()),
        )
    )

    merchant_performance_df.repartition("transaction_date").write.mode("overwrite").parquet(output_path)

    run_metadata["tables"]["merchant_performance"] = merchant_performance_df.count()
    run_metadata["row_counts"]["merchant_performance"] = merchant_performance_df.count()


def build_customer_ltv(spark, silver_path, output_path, run_metadata):
    silver_df = spark.read.parquet(silver_path)
    completed_txns = silver_df.filter(col("status") == "COMPLETED")

    customer_ltv_df = (
        completed_txns.groupBy("customer_id")
        .agg(
            sum_(col("amount")).alias("total_spent"),
            count("*").alias("total_txns"),
            avg(col("amount")).alias("avg_txn_value"),
            min_(col("transaction_date")).alias("first_txn_date"),
            max_(col("transaction_date")).alias("last_txn_date"),
            first(col("payment_method"), ignorenulls=True).alias("preferred_payment_method"),
        )
    )

    customer_ltv_df.write.mode("overwrite").parquet(output_path)
    run_metadata["tables"]["customer_ltv"] = customer_ltv_df.count()
    run_metadata["row_counts"]["customer_ltv"] = customer_ltv_df.count()


def build_daily_summary(spark, silver_path, output_path, run_date, run_metadata):
    silver_df = spark.read.parquet(silver_path)

    daily_summary_df = (
        silver_df.groupBy("transaction_date")
        .agg(
            sum_(when(col("status") == "COMPLETED", col("amount")).otherwise(lit(0))).alias("total_revenue"),
            count("*").alias("total_txns"),
            countDistinct(col("customer_id")).alias("unique_customers"),
            countDistinct(col("merchant_id")).alias("unique_merchants"),
            count(when(col("status") == "FAILED", 1)).alias("failed_txn_count"),
            count("*").alias("total_txn_count"),
        )
        .withColumn(
            "failure_rate_pct",
            (col("failed_txn_count") / col("total_txn_count") * 100).cast(FloatType()),
        )
    )

    daily_summary_df.repartition("transaction_date").write.mode("overwrite").parquet(output_path)
    run_metadata["tables"]["daily_summary"] = daily_summary_df.count()
    run_metadata["row_counts"]["daily_summary"] = daily_summary_df.count()


def write_run_metadata(metadata, output_dir, run_date):
    os.makedirs(output_dir, exist_ok=True)
    metadata_path = os.path.join(output_dir, f"run_metadata_{run_date}.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f)


def main():
    spark = SparkSession.builder.appName("Sigma DataTech Transaction Analytics Pipeline").getOrCreate()

    run_date = os.environ.get("SIGMA_RUN_DATE", "2026-05-27")
    run_id = os.environ.get("SIGMA_RUN_ID", f"run_id_{run_date}")

    ingest_bronze(spark, INPUT_PATH, BRONZE_PATH, run_date, run_id)
    transform_silver(spark, f"{BRONZE_PATH}/{run_date}", MERCHANTS_PATH, SILVER_PATH, run_date)

    run_metadata = {
        "run_date": run_date,
        "run_id": run_id,
        "status": "SUCCESS",
        "tables": {
            "merchant_performance": None,
            "customer_ltv": None,
            "daily_summary": None,
        },
        "row_counts": {},
    }

    try:
        build_merchant_performance(spark, f"{SILVER_PATH}/transaction_date={run_date}", f"{GOLD_OUTPUT_DIR}/merchant_performance", run_date, run_metadata)
        build_customer_ltv(spark, SILVER_PATH, f"{GOLD_OUTPUT_DIR}/customer_ltv", run_metadata)
        build_daily_summary(spark, f"{SILVER_PATH}/transaction_date={run_date}", f"{GOLD_OUTPUT_DIR}/daily_summary", run_date, run_metadata)
        write_run_metadata(run_metadata, METADATA_DIR, run_date)
    except Exception as exc:
        run_metadata["status"] = "FAILURE"
        run_metadata["error"] = str(exc)
        write_run_metadata(run_metadata, METADATA_DIR, run_date)
        raise


if __name__ == "__main__":
    main()
