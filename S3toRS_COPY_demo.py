import datetime as dt

from airflow import DAG
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator

default_args = {
  'owner': 'me',
  'start_date': dt.datetime(2019, 10, 8),
  'retries': 1,
  'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG('S3-RS-COPY-demo',
  default_args=default_args,
  schedule_interval='@once'
)

s3load = S3ToRedshiftOperator(
  task_id="s3load",
  redshift_conn_id="test_rs_conn",
  iam_role="arn:aws:iam::1234:role/testRole",
  region="us-west-1",
  s3_path="s3://account/20191004/stg_account.csv",
  delimiter=",",  
  staging_table="stg_account",
  format_as_json="auto",
  dag=dag
)

s3load
