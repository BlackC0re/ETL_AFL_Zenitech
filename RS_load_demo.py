import datetime as dt

from airflow import DAG
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator
from airflow.operators.redshift_upsert_plugin import RedshiftUpsertOperator

default_args = {
  'owner': 'me',
  'start_date': dt.datetime(2019, 10, 8),
  'retries': 1,
  'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG('redshift-load-demo',
  default_args=default_args,
  schedule_interval='@once'
)

s3load_user = S3ToRedshiftOperator(
  task_id="s3load_user",
  redshift_conn_id="test_rs_conn",
  iam_role="arn:aws:iam::1234:role/testRole",
  region="us-west-1",
  s3_path="s3://kwiff_user/20191008/users.csv",
  delimiter=",",  
  staging_table="stg_user",
  format_as_json="auto",
  dag=dag
)

s3load_user_tag = S3ToRedshiftOperator(
  task_id="s3load_user_tag",
  redshift_conn_id="test_rs_conn",
  iam_role="arn:aws:iam::1234:role/testRole",
  region="us-west-1",
  s3_path="s3://kwiff_user/20191008/user_tags.csv",
  delimiter=",",  
  staging_table="stg_user_tags",
  format_as_json="auto",
  dag=dag
)

s3load_user_mktg = S3ToRedshiftOperator(
  task_id="s3load_user_mktg",
  redshift_conn_id="test_rs_conn",
  iam_role="arn:aws:iam::1234:role/testRole",
  region="us-west-1",
  s3_path="s3://kwiff_user/20191008/user_marketing_preferences.csv",
  delimiter=",",  
  staging_table="stg_user_mktg_prefs",
  format_as_json="auto",
  dag=dag
)

s3load_user_dev = S3ToRedshiftOperator(
  task_id="s3load_user_dev",
  redshift_conn_id="test_rs_conn",
  iam_role="arn:aws:iam::1234:role/testRole",
  region="us-west-1",
  s3_path="s3://kwiff_user/20191008/user_devices.csv",
  delimiter=",",  
  staging_table="stg_user_devices",
  format_as_json="auto",
  dag=dag
)

load_dim_user = RedshiftUpsertOperator(
  task_id='load_dim_user',
  redshift_conn_id="test_rs_conn",
  aws_iam_role="arn:aws:iam::1234:role/myRSRole",
  dest_table="Dim_User",
  sql_query="Query from DimUser.sql",
  dag = dag
)

s3load_user >> s3load_user_tag
s3load_user_tag >> s3load_user_mktg
s3load_user_mktg >> s3load_user_dev
s3load_user_dev >> load_dim_user
