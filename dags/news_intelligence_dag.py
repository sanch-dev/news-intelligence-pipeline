from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'news_intelligence_pipeline',
    default_args=default_args,
    description='Daily news intelligence pipeline with Kafka streaming',
    schedule_interval='0 0 * * *',  # Daily at midnight UTC
    catchup=False,
    tags=['news', 'databricks', 'kafka'],
)

# Databricks configuration
databricks_host = os.getenv('DATABRICKS_HOST')
databricks_token = os.getenv('DATABRICKS_TOKEN')

# Task 1: Bronze Ingestion (NewsAPI + Kafka)
bronze_task = DatabricksSubmitRunOperator(
    task_id='bronze_ingestion',
    databricks_host=databricks_host,
    databricks_token=databricks_token,
    existing_cluster_id='0502-130412-dpe6g4gr',
    notebook_task={
        'notebook_path': '/Workspace/news_pipeline/bronze/01_auto_loader_ingest',
        'base_parameters': {
            'topic': 'news',
            'page_size': '100',
            'language': 'en',
        },
    },
    dag=dag,
)

# Task 2: Silver Transformation (DLT Pipeline)
silver_task = DatabricksSubmitRunOperator(
    task_id='silver_transformation',
    databricks_host=databricks_host,
    databricks_token=databricks_token,
    pipeline_task={
        'pipeline_id': 'news-intelligence-silver',  # Your DLT pipeline ID
    },
    dag=dag,
)

# Task 3: Gold Embedding (OpenAI Embeddings)
gold_task = DatabricksSubmitRunOperator(
    task_id='gold_embedding',
    databricks_host=databricks_host,
    databricks_token=databricks_token,
    existing_cluster_id='0502-130412-dpe6g4gr',
    notebook_task={
        'notebook_path': '/Workspace/news_pipeline/gold/03_embedding_pipeline',
    },
    dag=dag,
)

# Task 4: Log failure (calls the log_failure notebook)
log_failure_task = DatabricksSubmitRunOperator(
    task_id = 'log_pipeline_failure',
    databricks_host = databricks_host,
    databricks_token = databricks_token,
    existing_cluster_id='0502-130412-dpe6g4gr',
    notebook_task={
        'notebook_path': '/Workspace/news_pipeline/utils/log_failure',
        'base_parameters':{
            'status': 'failed',
        }
    },
    trigger_rule = 'one_failed',
    dag=dag,
)    

# Set task dependencies

# Normal flow: bronze → silver → gold
bronze_task >> silver_task >> gold_task

# If silver fails, log the failure
silver_task >> log_failure_task