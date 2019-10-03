import logging
 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
 
log = logging.getLogger(__name__)
 
class S3ToRedshiftOperator(BaseOperator):
"""
Executes a LOAD command on a s3 CSV file into a Redshift table
:param redshift_conn_id: reference to a specific redshift database
:type redshift_conn_id: string
 
:param table: reference to a specific table in redshift database
:type table: string
 
:param s3_bucket: reference to a specific S3 bucket
:type s3_bucket: string

:param delimiter: delimiter for CSV data
:type s3_key: string
 
:param region: location of the s3 bucket (eg. 'eu-central-1' or 'us-east-1')
:type s3_key: string
"""
 
@apply_defaults
def __init__(self, redshift_conn_id,table,s3_bucket,s3_path,delimiter,region,*args, **kwargs):
 
  self.redshift_conn_id = redshift_conn_id
  self.table = table
  self.s3_bucket = s3_bucket
  self.s3_path = s3_path
  self.delimiter = delimiter 
  self.region = region
 
  super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
 

def execute(self, context):
  self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
  conn = self.hook.get_conn() 
  cursor = conn.cursor()
  log.info("Connected with " + self.redshift_conn_id)
 
  load_statement = """
    delete from {0};
    copy
    {0}
    from 's3://{1}/{2}'
    delimiter '{3}' region '{4}' """.format(
  self.table, self.s3_bucket, self.s3_path,
  self.delimiter, self.region)
 
  cursor.execute(load_statement)
  cursor.close()
  conn.commit()
  log.info("Load command completed")
 
  return True
 

class S3ToRedshiftOperatorPlugin(AirflowPlugin):
  name = "redshift_load_plugin"
  operators = [S3ToRedshiftOperator]
