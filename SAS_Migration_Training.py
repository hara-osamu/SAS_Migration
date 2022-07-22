# Databricks notebook source
# MAGIC %md
# MAGIC DLTで作成したsas_migration.sales_fixを用いてモデル学習を実施する。

# COMMAND ----------

from pyspark.sql.functions import col
train_df = spark.read.table("sas_migration.sales_fix").filter(col("kbn") == "ACT")

# COMMAND ----------

# AutoMLで実行
import databricks.automl
import logging

# fbprophetの情報レベルのメッセージを無効化
logging.getLogger("py4j").setLevel(logging.WARNING)

summary = databricks.automl.regress(train_df, target_col="SUM_of_数量", time_col="date", timeout_minutes = 10)

# COMMAND ----------

summary

# COMMAND ----------

import mlflow

# 最良モデルの読み込み
model_uri = summary.best_trial.model_path

# テストデータセットの準備
test_df = spark.read.table("sas_migration.sales_fix").filter(col("kbn") == "PRED")

predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)
display(test_df.withColumn("SUM_of_数量_predicted", predict_udf()))

# COMMAND ----------


