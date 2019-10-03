import datetime as dt

from airflow import DAG
from airflow.operators.redshift_upsert_plugin import RedshiftUpsertOperator
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator

default_args = {
  'owner': 'me',
  'start_date': dt.datetime(2019, 10, 3),
  'retries': 1,
  'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG('redshift-load-demo',
  default_args=default_args,
  schedule_interval='@once'
)

rsupsert = RedshiftUpsertOperator(
  task_id='upsert',
  src_redshift_conn_id="test_rs_conn",
  dest_redshift_conn_id="test_rs_conn",
  src_table="stg_account",
  dest_table="dim_account",
  src_keys=["account_id"],
  dest_keys=["account_id"],
  dag = dag
)
 
s3load = S3ToRedshiftOperator(
  task_id="load",
  redshift_conn_id="test_rs_conn",
  table="stg_account",
  s3_bucket="bucket_name",
  s3_path="stg_account.csv",
  delimiter=",",
  region="us-west-1",
  dag=dag
)

s3load >> rsupsert
