from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class RedshiftUpsertOperator(BaseOperator):
 
"""
 
Executes an upsert from one to another table in the same Redshift database by completely replacing the rows of target table with the rows in staging table that contain the same business key
 
:param redshift_conn_id: reference to a specific Redshift database
:type redshift_conn_id: string
 
:param aws_iam_role: reference to a specific IAM role
:type aws_iam_role: string
 
:param dest_table: reference to a specific Redshift table
:type dest_table: string
 
:param sql_query: SQL query to be executed
:type sql_query: string

"""
 
@apply_defaults
def __init__(self,
             redshift_conn_id="my_rs_conn",
             aws_iam_role="arn:aws:iam::1234:role/myRSRole",
             dest_table='Dim_User',
             sql_query='query_text',
             *args, **kwargs):

    super(LoadDimensionOperator, self).__init__(*args, **kwargs)
    self.redshift_conn_id=redshift_conn_id
    self.aws_iam_role=aws_iam_role
    self.dest_table=dest_table
    self.insert_mode=insert_mode
    self.sql_query=sql_query
        
def execute(self, context):
  self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
  conn = self.hook.get_conn()
  cursor = conn.cursor()
  log.info("Connected with " + self.redshift_conn_id)
  cursor.execute(sql_query)
  cursor.close()
  conn.commit()
  log.info("Upsert completed successfully!")
 
class RedshiftUpsertPlugin(AirflowPlugin):
  name = "redshift_upsert_plugin"
  operators = [RedshiftUpsertOperator]
