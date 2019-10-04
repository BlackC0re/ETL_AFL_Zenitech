import logging
 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
 
log = logging.getLogger(__name__)
 
class S3ToRedshiftOperator(BaseOperator):
"""
Executes a LOAD command on a s3 CSV file into a Redshift table
:param redshift_conn_id: connection reference to a specific Redshift database
:type redshift_conn_id: string
 
:param iam_role: reference to a specific AWS IAM role
:type iam_role: string

:param region: AWS region used (eg. 'eu-west-1')
:type region: string

:param s3_path: reference to a specific S3 path
:type s3_path: string

:param delimiter: delimiter for CSV data
:type delimiter: string
 
:param staging_table: reference to a specific Redshift table
:type staging_table: string

:param format_as_json: format as JSON path
:type format_as_json: string
"""
 
@apply_defaults
def __init__(self,
             redshift_conn_id,
             iam_role="arn:aws:iam::1234:role/myRSRole",
             region="eu-west-1",
             s3_path,
             delimiter,
             staging_table,
             format_as_json="auto",
             *args, **kwargs):
 
  self.redshift_conn_id = redshift_conn_id
  self.iam_role = iam_role
  self.region = region  
  self.s3_path = s3_path
  self.delimiter = delimiter
  self.staging_table = staging_table
  self.format_as_json = format_as_json
 
  super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
 

def execute(self, context):
  self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
  conn = self.hook.get_conn() 
  cursor = conn.cursor()
  log.info("Connected with " + self.redshift_conn_id)
 
  load_statement = """
    COPY {table}
    FROM '{s3_path}'
    iam_role '{iam_role}' 
    region '{region}' 
    FORMAT AS JSON '{json_path}';"""
 
  formatted_sql = load_statement.format(
      table = self.staging_table,
      s3_path = self.s3_path,
      iam_role = self.iam_role,
      region = self.region,
      json_path = self.format_as_json 
  ) 
  cursor.execute(formatted_sql)
  cursor.close()
  conn.commit()
  log.info("COPY command completed!")
 
  return True
 

class S3ToRedshiftOperatorPlugin(AirflowPlugin):
  name = "redshift_load_plugin"
  operators = [S3ToRedshiftOperator]
