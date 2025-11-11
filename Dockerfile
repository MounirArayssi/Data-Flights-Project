FROM apache/airflow:2.9.1

USER root
RUN pip install --no-cache-dir apache-airflow-providers-google google-cloud-bigquery google-cloud-storage
USER airflow
