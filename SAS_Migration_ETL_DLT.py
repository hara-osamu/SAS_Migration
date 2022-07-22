# Databricks notebook source
# MAGIC %md
# MAGIC 取得したデータを元に学習用データを作成するETL処理  
# MAGIC   
# MAGIC 所感
# MAGIC - 基本的にPythonコードそのまま移植できる。
# MAGIC - タイポでのエラーを数回やらかす。
# MAGIC - EGからそのままなら、基本SQLそのまま移植の方が楽か？
# MAGIC - Pythonと違って上で定義した変数のそのまま利用ができないので、使い回す場合は再計算？グローバル化できる？
# MAGIC - importも使い回せないので頭でまとめて定義しておく方が吉か。
# MAGIC - dlt.readで返ってくるのが厳密にはdataframeではない？そのままaggとか噛ますとうまく動かない（Noneが返ってくる）
# MAGIC - 最後の登録処理や途中でテーブルに書き込む処理の書き方。（defの中にwrite.format("delta").saveAsTable(xxx)を書いてしまって良い？）
# MAGIC - insert命令がグラフに組み込まれていないため順番が狂っている？
# MAGIC - 対応策としてapply_changeを使おうとするが、これはlive tableにしか使えない模様？
# MAGIC - 「元データにないデータのみ取得して処理をして、元データに追記」という処理をしようとした場合、元データが0件になってしまう。原因が分からないので来週聞く。
# MAGIC - SASではありがちな処理であること、単純なNotebookでは起きなかった事からDLTの処理起因っぽいが、、不明。
# MAGIC - 別のNotebookを挟んでETL処理を2段階（＋INSERT）にすればひとまずの解決にはなりそう。

# COMMAND ----------

import dlt
import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import max, min, year, month, col, concat, format_string, lit, to_date, coalesce

# COMMAND ----------

@dlt.table
def sales():
    return(
        spark.read.table("sales")
    )

# COMMAND ----------

@dlt.table
def sales_new():
    sales_transform_df = spark.read.table("sales_transform")
    max_year = sales_transform_df.agg(max("年")).head()[0]
    max_month = sales_transform_df.agg(max("月")).head()[0]
    max_ym = str(max_year or "").zfill(4) + str(max_month or "").zfill(2)
    
    return(
        dlt.read("sales").where(max_ym < concat(format_string("%04d", col('年')),format_string("%02d", col('月'))))
    )

# COMMAND ----------

@dlt.table
def ken_all():
    return(
        spark.read.table("ken_all")
    )

# COMMAND ----------

@dlt.table
def ken_all_name():
    ken_all_name_df = (dlt.read("ken_all")
       .select(["_c2","_c6"])
       .withColumnRenamed("_c2", "送り先郵便番号")
       .withColumnRenamed("_c6", "都道府県名")
      )
    return(
        ken_all_name_df
    )

# COMMAND ----------

@dlt.table
def sell_ken():
    sell_ken_df = dlt.read("sales_new").join(dlt.read("ken_all_name"), ["送り先郵便番号"], "left_outer")
    return(
        sell_ken_df
    )

# COMMAND ----------

@dlt.table
def ken_area():
    return(
        spark.read.table("ken_area")
    )

# COMMAND ----------

@dlt.table
def sell_area():
    sell_area_df = dlt.read("sell_ken").join(dlt.read("ken_area"), ["都道府県名"], "inner")
    return(
        sell_area_df
    )

# COMMAND ----------

@dlt.table
def sell_area_sum():
    sell_area_sum_df = (dlt.read("sell_area")
                        .groupBy("機種名","色","地方","年","月").sum("数量")
                        .withColumn("sum(数量)",col("sum(数量)").cast("int"))
                        .withColumnRenamed("sum(数量)","SUM_of_数量")
                       )
    return(
        sell_area_sum_df
    )

# COMMAND ----------

@dlt.table
def sales_transform():
    sales_transform_df = spark.read.table("sales_transform").unionAll(dlt.read("sell_area_sum")).distinct()
    return(
        sales_transform_df
    )

# COMMAND ----------

@dlt.table
def sell_area_distinct():
    sell_area_distinct_df = (dlt.read("sales_transform")
                             .select("機種名","色","地方")
                             .distinct()
                            )
    return(
        sell_area_distinct_df
    )

# COMMAND ----------

@dlt.table
def ym():
    ym_df = (dlt.read("sales_transform")
             .select("年","月")
             .distinct()
             .withColumn("date", to_date(concat(format_string("%04d", col('年')),format_string("%02d", col('月')),lit("01")),"yyyyMMdd"))
            )
    return(
        ym_df
    )


# COMMAND ----------

@dlt.table
def ym_range_fix():
    dlt.read("ym") #順番操作用
    ym_df = (spark.read.table("sales_transform")
             .select("年","月")
             .distinct()
             .withColumn("date", to_date(concat(format_string("%04d", col('年')),format_string("%02d", col('月')),lit("01")),"yyyyMMdd"))
            )
    max_date = ym_df.agg(max("date")).head()[0]
    min_date = ym_df.agg(min("date")).head()[0]
# これだとエラーになる。。
#     max_date = dlt.read("ym").agg(max("date")).head()[0]
#     min_date = dlt.read("ym").agg(min("date")).head()[0]
    
    ym_range_fix_list = pd.date_range(start=min_date, end=max_date, freq="D").to_frame()
    ym_range_fix_df = (spark
                       .createDataFrame(ym_range_fix_list)
                       .withColumn("year", year("0"))
                       .withColumn("month", month("0"))
                       .withColumn("kbn", lit("ACT"))
                       .select("year", "month", "kbn")
                      )
    return(
         ym_range_fix_df
    )

# COMMAND ----------

@dlt.table
def ym_range():
    ym_range_df = dlt.read("ym_range_fix").distinct()
    return(
        ym_range_df
    )

# COMMAND ----------

@dlt.table
def ym_pred_range():
    dlt.read("ym") #順番操作用
    ym_df = (spark.read.table("sales_transform")
             .select("年","月")
             .distinct()
             .withColumn("date", to_date(concat(format_string("%04d", col('年')),format_string("%02d", col('月')),lit("01")),"yyyyMMdd"))
            )
    max_date = ym_df.agg(max("date")).head()[0]
#     max_date = dlt.read("ym").agg(max("date")).head()[0]
    pred_start = max_date + relativedelta(months=1)
    pred_end = max_date + relativedelta(years=1)
    ym_pred_range_fix_list = pd.date_range(start=pred_start, end=pred_end, freq="D").to_frame()
    ym_pred_range_df = (spark
                        .createDataFrame(ym_pred_range_fix_list)
                        .withColumn("year", year("0"))
                        .withColumn("month", month("0"))
                        .withColumn("kbn", lit("PRED"))
                        .select("year", "month", "kbn")
                        .distinct()
                       )
    return(
        ym_pred_range_df
    )


# COMMAND ----------

@dlt.table
def ym_range_p():
    ym_range_p_df = dlt.read("ym_range").unionAll(dlt.read("ym_pred_range"))
    return(
        ym_range_p_df
    )

# COMMAND ----------

@dlt.table
def sell_area_ym_cross():
    sell_area_ym_cross_df = (dlt.read("sell_area_distinct")
                             .crossJoin(dlt.read("ym_range_p"))
                             .withColumn("SUM_of_数量_Z", lit(0))
                             .withColumnRenamed("year", "年")
                             .withColumnRenamed("month", "月")
                            )
    return(
        sell_area_ym_cross_df
    )

# COMMAND ----------

@dlt.table
def sales_fix():
    sales_fix_df = (dlt.read("sell_area_ym_cross")
                    .join(spark.read.table("default.sales_transform"), ["機種名","色","地方","年","月"], "left_outer")
                    .withColumn("SUM_of_数量", coalesce("SUM_of_数量", "SUM_of_数量_Z"))
                    .drop("SUM_of_数量_Z")
                    .withColumn("date", to_date(concat(format_string("%04d", col('年')),format_string("%02d", col('月')),lit("01")),"yyyyMMdd"))
                   )
    return(
        sales_fix_df
    )
