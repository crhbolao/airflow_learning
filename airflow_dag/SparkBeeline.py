import os

import jaydebeapi
from krbcontext import krbcontext
from impala.dbapi import connect

class SparkBeeline:


    database='default'
    driver='org.apache.hive.jdbc.HiveDriver'
    server='173.24.155.11'
    principal='hive/host@DC1.FH.COM'
    keytab_path='/etc/hadoop/hive/hive.keytab'
    driver_args={'hive.server2.authentication.kerberos.principa': principal,
                 'hive.server2.authentication.kerberos.keytab': keytab_path,
                 'hive.metastore.kerberos.keytab.file': principal,
                 'hive.metastore.kerberos.principal': keytab_path,
                 'spark.kerberos.principal': principal,
                 'spark.kerberos.keytab': keytab_path}
    port=10005


    def initEnvParams(self):

        print("init env params ...")

    def run(self):
        """
        程序的执行入口
        """

        url = "jdbc:hive2://" + self.server + ":" + str(self.port) + "/" + self.database + ";principal=" + self.principal + ";"
        print(f"this jdbc url is {url}")

        try:
            active_str = 'kinit -kt {0} {1}'.format(self.keytab_path, self.principal)
            os.system(active_str)
            with krbcontext(using_keytab=True, principal=spark_beeline.principal, keytab_file=spark_beeline.keytab_path):
                connect1 = connect(host=self.server, port=self.port, auth_mechanism='GSSAPI', kerberos_service_name='hive')
                cursor = connect1.cursor()
                sql = "show tables;"
                cursor.execute(sql)
                results = cursor.fetchall()
                print(results)
        except Exception as e:
            raise e

if __name__ == '__main__':
    spark_beeline = SparkBeeline
    spark_beeline.run(spark_beeline)
    print("start to test")
