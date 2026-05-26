from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email_smtp
import logging
import json

default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    error_message = context['exception']
    logging.error(f"DAG: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {error_message}")
    send_email_smtp(to='alerts@example.com', subject=f"Airflow Task Failure - {dag_id}", html_content=f"Task {task_id} failed on {execution_date}. Error: {error_message}")

def sla_miss_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    logging.error(f"DAG: {dag_id}, Execution Date: {execution_date} - SLA Miss")
    send_email_smtp(to='alerts@example.com', subject=f"Airflow SLA Miss - {dag_id}", html_content=f"DAG {dag_id} missed SLA on {execution_date}")

def log_task_status(context):
    task_instance = context['task_instance']
    logging.info(f"Task {task_instance.task_id} started for {task_instance.dag_id} on {task_instance.execution_date}")
    if context['status'] == 'success':
        logging.info(f"Task {task_instance.task_id} completed successfully")
    else:
        logging.error(f"Task {task_instance.task_id} failed")

def extract_bronze(**context):
    logging.info("Starting Bronze layer extraction")
    # CSV to Parquet logic here
    logging.info("Bronze layer extraction completed")
    raise Exception("Simulated failure")  # For testing

def transform_silver(**context):
    logging.info("Starting Silver layer transformation")
    # Data cleaning, enrichment, deduplication logic here
    logging.info("Silver layer transformation completed")
    raise Exception("Simulated failure")  # For testing

def build_gold(**context):
    logging.info("Starting Gold layer aggregation")
    # Aggregation logic here
    logging.info("Gold layer aggregation completed")
    raise Exception("Simulated failure")  # For testing

with DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
) as dag:

    extract_bronze_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
        on_failure_callback=on_failure_callback,
        provide_context=True
    )

    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
        on_failure_callback=on_failure_callback,
        provide_context=True
    )

    build_gold_task = PythonOperator(
        task_id='build_gold',
        python_callable=build_gold,
        on_failure_callback=on_failure_callback,
        provide_context=True
    )

    extract_bronze_task >> transform_silver_task >> build_gold_task
