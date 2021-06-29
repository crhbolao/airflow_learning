from typing import Any, List, Optional, Union
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from impala.dbapi import connect
from krbcontext import krbcontext
import os
from contextlib import closing


class SparkThriftJdbcHook(BaseHook):
    def __init__(
        self,
        connection_id: Optional[str] = None,
        is_auth: bool = False,
        timeout: int = 10
    ):
        super().__init__()
        self.connection_id = connection_id
        self.is_auth = is_auth
        self.timeout = timeout
        self.host_ip = None
        self.port = None
        self.principal_name = None
        self.keytab_path = None

        # 参数校验
        if connection_id is None:
            raise AirflowException("Cannot init spark thrift jdbc hook without connection_id.")

        # 根据conn_id，获取airflow connection
        conn = self.get_connection(self.connection_id)

        # 如果开启认真，但是connection中没有设置extra（里面包含有principal, keytab_auth）会报错
        if is_auth and conn.extra is None:
            raise AirflowException("the spark thrift jdbc hook is auth but conn extra is empty, please check!")

        # 初始化相关参数
        if self.host_ip is None:
            self.host_ip = conn.host
        if self.port is None:
            self.port = conn.port
        if conn.extra is not None:
            extra_dejson = conn.extra_dejson

            # 如果开启认证的话，但是没有设置“principal_name”和“keytab_path”，会报错
            if is_auth and ("principal_name" not in extra_dejson or "keytab_path" not in extra_dejson):
                raise AirflowException(
                    "the spark thrift jdbc hook is auth but principal_name or keytab_path is empty, please check!")

            if self.principal_name is None:
                self.principal_name = extra_dejson.get("principal_name")

            if self.keytab_path is None:
                self.keytab_path = extra_dejson.get("keytab_path")

    def get_conn(self) -> connect:

        self.log.info("start to get connection ... ")

        # 根据是否开启认证，拼接spark jdbc 连接串
        if self.is_auth:
            try:
                # todo 目前每次sql在执行时，都执行kinit，需要后续优化。
                # active_str = 'kinit -kt {0} {1}'.format(self.keytab_path, self.principal_name)
                # os.system(active_str)
                with krbcontext(using_keytab=True, principal=self.principal_name, keytab_file=self.keytab_path):
                    return self.init_conn()
            except Exception as e:
                raise AirflowException(e)
        else:
            return self.init_conn()


    def init_conn(self) -> connect:
        """
        返回服务对应的connection
        self.principal_name is : hive/host@DC1.FH.COM
        """
        split_tmp = str(self.principal_name).split("@", 1)[0]
        split_array = split_tmp.split("/", 1)
        kerberos_service_name = split_array[0]
        kerberos_host_name = split_array[1]
        return connect(self.host_ip, self.port, auth_mechanism="GSSAPI", kerberos_service_name=kerberos_service_name,
                       krb_host=kerberos_host_name)

    def execute_sql(self, sql: Optional[str]):

        self.log.info("start to execute sql "+ sql)

        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
