import shutil
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, when, sum, count, avg, min, max, mode
from pyspark.sql.types import StringType, FloatType, DateType
import json
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: Ingest Bronze] Starting ingestion")
        
        # Read raw CSV files with all columns as strings
        transactions_df = (spark.read.option("header", "true")
                          .option("inferSchema", "false")
                          .csv(input_path))
        
        # Add metadata columns
        transactions_df = (transactions_df.withColumn("ingestion_timestamp", lit(run_date))
                           .withColumn("source_file", lit("transactions.csv"))
                          .withColumn("pipeline_run_id", lit(run_id)))
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/{run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write as Parquet partitioned by date
        transactions_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(output_path)
        
        logging.info(f"[Stage: Ingest Bronze] Ingested {transactions_df.count():,} rows into {output_path}/{run_date}")
    except Exception as e:
        logging.error(f"[Stage: Ingest Bronze] Error: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: Transform Silver] Starting transformation")
        
        # Read Bronze Parquet with partition pruning on run_date
        transactions_df = (spark.read.format("parquet")
                          .option("basePath", bronze_path)
                          .load(f"{bronze_path}/{run_date}"))
        
        # Partition pruning — only read today's data, not full history
        transactions_df = transactions_df.filter(col("ingestion_timestamp") == run_date)
        
        logging.info(f"[Stage: Transform Silver] Input count: {transactions_df.count():,} rows")
        
        # Cast columns to correct types
        transactions_df = transactions_df.withColumn("amount", col("amount").cast(FloatType()))
        transactions_df = transactions_df.withColumn("transaction_date", col("transaction_date").cast(DateType()))
        transactions_df = transactions_df.withColumn("transaction_id", col("transaction_id").cast(StringType()))
        transactions_df = transactions_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))
        
        # Filter: remove records where transaction_id is NULL or amount < 0
        transactions_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
        
        logging.info(f"[Stage: Transform Silver] After filter count: {transactions_df.count():,} rows")
        
        # Deduplicate: if same transaction_id appears twice, keep the record with latest ingestion_timestamp
        transactions_df = transactions_df.withColumn("rank", 
                                                   when(col("ingestion_timestamp").isNotNull(), 
                                                        col("ingestion_timestamp").cast("long")).otherwise(0))
        transactions_df = transactions_df.withColumn("row_number", 
                                                    ((col("transaction_id").isNotNull()) & 
                                                     (col("ingestion_timestamp").isNotNull())).cast("long"))
        transactions_df = transactions_df.withColumn("row_number", 
                                                     transactions_df.row_number().over(Window.partitionBy("transaction_id").orderBy(col("row_number").desc())))
        
        transactions_df = transactions_df.filter(col("row_number") == 1).drop("rank", "row_number")
        
        logging.info(f"[Stage: Transform Silver] After dedup count: {transactions_df.count():,} rows")
        
        # Read merchants data and cache it
        merchants_df = (spark.read.option("header", "true")
                       .option("inferSchema", "false")
                       .csv(merchants_path))
        merchants_df = merchants_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))
        merchants_df = merchants_df.cache()
        
        # Enrich: join transactions with merchants on merchant_id
        transactions_df = transactions_df.join(broadcast(merchants_df), 
                                              transactions_df.merchant_id == merchants_df.merchant_id, 
                                              "left_outer")
        
        # Add quality flag: mark records with no matching merchant as 'UNMATCHED'
        transactions_df = transactions_df.withColumn("quality_flag", 
                                                     when(col("merchant_id").isNotNull(), "CLEAN")
                                                    .otherwise("UNMATCHED"))
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/{run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write as Parquet partitioned by date
        transactions_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
        
        logging.info(f"[Stage: Transform Silver] Transformed {transactions_df.count():,} rows into {output_path}/{run_date}")
    except Exception as e:
        logging.error(f"[Stage: Transform Silver] Error: {e}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    run_metadata = {
        "run_date": run_date,
        "status": "SUCCESS",
        "started_at": datetime.now().isoformat(),
        "tables": {
            "merchant_performance": None,
            "customer_ltv": None,
            "daily_summary": None
        },
        "row_counts": {}
    }
    
    try:
        build_merchant_performance(spark, f"{silver_path}/date={run_date}", f"{gold_output_dir}/merchant_performance", run_date, run_metadata)
        build_customer_ltv(spark, f"{silver_path}", f"{gold_output_dir}/customer_ltv", run_metadata)
        build_daily_summary(spark, f"{silver_path}/date={run_date}", f"{gold_output_dir}/daily_summary", run_date, run_metadata)
        
        run_metadata["status"] = "SUCCESS"
        run_metadata["completed_at"] = datetime.now().isoformat()
        
        with open(f"{gold_output_dir}/run_metadata.json", "w") as f:
            json.dump(run_metadata, f)
            
    except Exception as e:
        run_metadata["status"] = "FAILURE"
        run_metadata["error"] = str(e)
        run_metadata["completed_at"] = datetime.now().isoformat()
        
        with open(f"{gold_output_dir}/run_metadata.json", "w") as f:
            json.dump(run_metadata, f)
        
        raise e

def build_merchant_performance(spark, silver_path, output_path, run_date, run_metadata):
    try:
        logging.info("[Stage: Build Merchant Performance] Starting aggregation")
        
        # Read Silver layer data
        silver_df = spark.read.parquet(silver_path)
        
        # Partition pruning — only read today's data, not full history
        silver_df = silver_df.filter(col("transaction_date") == run_date)
        
        logging.info(f"[Stage: Build Merchant Performance] Input count: {silver_df.count():,} rows")
        
        # Filter for completed transactions
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        # Calculate metrics
        merchant_performance_df = completed_txns.groupBy("merchant_id", "merchant_name", "category", "city", "date") \
           .agg(
                sum(col("amount")).alias("total_revenue"),
                count("*").alias("txn_count"),
                count(when(col("status") == "FAILED", 1)).alias("failed_txn_count"),
                count("*").alias("total_txn_count")
            ).withColumn("failure_rate_pct", (col("failed_txn_count") / col("total_txn_count") * 100).cast("float"))
        
        logging.info(f"[Stage: Build Merchant Performance] Output count: {merchant_performance_df.count():,} rows")
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/{run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write output
        merchant_performance_df.repartition("date").write.mode("overwrite").parquet(output_path)
        
        run_metadata["tables"]["merchant_performance"] = merchant_performance_df.count()
        run_metadata["row_counts"]["merchant_performance"] = merchant_performance_df.count()
        
    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path, run_metadata):
    try:
        logging.info("[Stage: Build Customer LTV] Starting aggregation")
        
        # Read Silver layer data
        silver_df = spark.read.parquet(silver_path)
        
        logging.info(f"[Stage: Build Customer LTV] Input count: {silver_df.count():,} rows")
        
        # Filter for completed transactions
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        # Calculate metrics
        customer_ltv_df = completed_txns.groupBy("customer_id") \
           .agg(
                sum(col("amount")).alias("total_spent"),
                count("*").alias("total_txns"),
                avg(col("amount")).alias("avg_txn_value"),
                min(col("transaction_date")).alias("first_txn_date"),
                max(col("transaction_date")).alias("last_txn_date"),
                mode(col("payment_method")).over(Window.partitionBy("customer_id")).alias("preferred_payment_method")
            )
        
        logging.info(f"[Stage: Build Customer LTV] Output count: {customer_ltv_df.count():,} rows")
        
        # Delete existing partition before writing
        partition_path = output_path
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write output
        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        
        run_metadata["tables"]["customer_ltv"] = customer_ltv_df.count()
        run_metadata["row_counts"]["customer_ltv"] = customer_ltv_df.count()
        
    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date, run_metadata):
    try:
        logging.info("[Stage: Build Daily Summary] Starting aggregation")
        
        # Read Silver layer data
        silver_df = spark.read.parquet(silver_path)
        
        # Partition pruning — only read today's data, not full history
        silver_df = silver_df.filter(col("transaction_date") == run_date)
        
        logging.info(f"[Stage: Build Daily Summary] Input count: {silver_df.count():,} rows")
        
        # Calculate metrics
        daily_summary_df = silver_df.groupBy("date") \
           .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(lit(0))).alias("total_revenue"),
                count("*").alias("total_txns"),
                count(col("customer_id")).alias("unique_customers"),
                count(col("merchant_id")).alias("unique_merchants"),
                count(when(col("status") == "FAILED", 1)).alias("failed_txn_count"),
                count("*").alias("total_txn_count")
            ).withColumn("failure_rate_pct", (col("failed_txn_count") / col("total_txn_count") * 100).cast("float"))
        
        logging.info(f"[Stage: Build Daily Summary] Output count: {daily_summary_df.count():,} rows")
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/{run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write output
        daily_summary_df.repartition("date").write.mode("overwrite").parquet(output_path)
        
        run_metadata["tables"]["daily_summary"] = daily_summary_df.count()
        run_metadata["row_counts"]["daily_summary"] = daily_summary_df.count()
        
    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}")
        raise

def main():
    # Initialize SparkSession
    spark = (SparkSession.builder
            .appName("Sigma DataTech Transaction Analytics Pipeline")
             .getOrCreate())
    
    # Define paths and run metadata
    input_path = "s3://sigma-datatech/raw/transactions.csv"
    merchants_path = "s3://sigma-datatech/raw/merchants.csv"
    bronze_path = "s3://sigma-datatech/bronze/transactions"
    silver_path = "s3://sigma-datatech/silver/transactions"
    gold_output_dir = "s3://sigma-datatech/gold"
    run_date = "2026-05-27"
    run_id = "run_id_20260527"
    
    # Ingest Bronze layer
    ingest_bronze(spark, input_path, bronze_path, run_date, run_id)
    
    # Transform Silver layer
    transform_silver(spark, bronze_path, merchants_path, silver_path, run_date)
    
    # Run Gold layer
    run_gold(spark, silver_path, gold_output_dir, run_date)

if __name__ == "__main__":
    main()
