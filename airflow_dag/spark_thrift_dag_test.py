import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_thrift_jdbc import SparkThriftJdbcOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'fenghuo',
    'depends_on_past': False,
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id='spark_thrift_jdbc_test',
    default_args=default_args,
    description='spark thrift operator for spark sql test'
)

spark_sql = SparkThriftJdbcOperator(
    connection_id='spark_thrift_jdbc',
    is_auth=True,
    sql="show tables",
    task_id="spark_sql_thrift",
    dag=dag
)
