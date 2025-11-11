from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from datetime import datetime

with DAG(
    dag_id="gcp_connection_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_conn = BigQueryGetDatasetOperator(
        task_id="test_connection",
        dataset_id="flight-intel-raw",
        gcp_conn_id="google_cloud_default",
    )
