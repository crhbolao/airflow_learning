from typing import Optional, Union, Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.spark.hooks.spark_thrift_jdbc import SparkThriftJdbcHook


class SparkThriftJdbcOperator(BaseOperator):
    """
    SparkBeelineJdbcOperator will execute spark/hive sql by thrift server

    :param connection_id : spark/hive connection id from airflow Connections
           airflow Connections have conn_id,host,port,shcema,login,passwd,extra_dejson
           we will put principal name and keytab path in extra_dejson
    :param is_auth: if spark or hive open kerberos auth
    :param sql： spark/hive sql string
    """

    def __init__(
        self,
        connection_id: Optional[str] = None,
        is_auth: bool = False,
        sql: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.is_auth = is_auth
        self.sql = sql
        self.log.info('the init param in SparkThriftJdbcOperator is %s, %s, %s', self.connection_id, self.is_auth,
                      self.sql)

    def execute(self, context: Any):
        """
        程序真正的执行逻辑
        """

        #  首先需要校验参数
        if not self.connection_id:
            raise AirflowException("Cannot operate without connection_id.")

        if not self.sql:
            raise AirflowException("Cannot operate without sql for SparkThriftJdbcOperator.")

        # 初始化 SparkThriftJdbcOperator 对应的hook

        hook = SparkThriftJdbcHook(self.connection_id, is_auth=True)
        result = hook.execute_sql(self.sql)
        self.log.info("spark thrift operator is done, the result is %s", result)
        return result
