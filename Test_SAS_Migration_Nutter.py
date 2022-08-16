# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag
import os

# COMMAND ----------

class MyTestFixture(NutterFixture):
# 入力データのセッティングを行う。（今回は何もしない）
    def before_all(self):
        print('before_all')
# 一連のJobsを実行する。
    def run_test_ingest(self):
# NotebookからのJob実行方法が検索しても分からないので8/17に伺う。最悪手動で実行してからこのテストNotebookで一致確認でもいい気がする。
#         dbutils.notebook.run('../Python/Data_Ingest', 0)
        print('run job')
# 出力テーブルの内容が、事前にアップロードしておいたSASデータセットと一致することを確認する。
    def assertion_test_ingest(self):
        df = spark.read.table("sas_migration.sales_fix")
        df_sas = spark.read.format("com.github.saurfang.sas.spark").load(f"file:{os.getcwd()}/work_sales_sas_fix.sas7bdat")
        assert (df_sas.collect() == df.collect())

result = MyTestFixture().execute_tests()
print(result.to_string())
# result.exit(dbutils)

# COMMAND ----------


