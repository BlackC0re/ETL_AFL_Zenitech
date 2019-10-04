from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class RedshiftUpsertOperator(BaseOperator):
 
"""
 
Executes an upsert from one to another table in the same Redshift database by completely replacing the rows of target table with the rows in staging table that contain the same business key
 
:param src_redshift_conn_id: reference to a specific Redshift database
:type src_redshift_conn_id: string
 
:param dest_redshift_conn_id: reference to a specific Redshift database
:type dest_redshift_conn_id: string
 
:param src_table: reference to a specific Redshift table
:type src_table: string
 
:param dest_table: reference to a specific Redshift table
:type dest_table: string
 
:param src_keys business keys that are supposed to be matched with dest_keys business keys in the target table
:type src_keys: string
 
:param dest_keys business keys that are supposed to be matched with src_keys business keys in the source table
:type dest_keys: string
"""
 
@apply_defaults
 
def __init__(self, src_redshift_conn_id, dest_redshift_conn_id,
  src_table, dest_table, src_keys, dest_keys, *args, **kwargs):
 
  self.src_redshift_conn_id = src_redshift_conn_id
  self.dest_redshift_conn_id = dest_redshift_conn_id
  self.src_table = src_table
  self.dest_table = dest_table 
  self.src_keys = src_keys
  self.dest_keys = dest_keys
  super(RedshiftUpsertOperator , self).__init__(*args, **kwargs)
 
def execute(self, context):
  self.hook = PostgresHook(postgres_conn_id=self.src_redshift_conn_id)
  conn = self.hook.get_conn()
  cursor = conn.cursor()
  log.info("Connected with " + self.src_redshift_conn_id)
  # build the SQL statement
  sql_statement = "BEGIN TRANSACTION; "
  sql_statement += "DELETE FROM " + self.dest_table + " using " + self.src_table + " where "
  for i in range (0,len(self.src_keys)):
    sql_statement += self.src_table + "." + self.src_keys[i] + " = " + self.dest_table + "." + self.dest_keys[i]
    if(i < len(self.src_keys)-1):
      sql_statement += " and "
      
  sql_statement += "; "
  sql_statement += " INSERT INTO " + self.dest_table + " select * from " + self.src_table + " ; "
  sql_statement += " END TRANSACTION; "
 
  print(sql_statement)
  cursor.execute(sql_statement)
  cursor.close()
  conn.commit()
  log.info("Upsert command completed")
 
class RedshiftUpsertPlugin(AirflowPlugin):
  name = "redshift_upsert_plugin"
  operators = [RedshiftUpsertOperator]
