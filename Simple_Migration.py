# Databricks notebook source
# MAGIC %md
# MAGIC 前回実行時のデータ：default.sales_transform (delta table)
# MAGIC 
# MAGIC 今回取り込むデータ：sales.csv

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 最大の年月の算出   */
# MAGIC 
# MAGIC proc sort data=ARCHIVE.SALES_SAS out=work.max_ym_1;  
# MAGIC 	by DESCENDING '年'n DESCENDING '月'n;  
# MAGIC run;
# MAGIC 
# MAGIC data work.max_ym_2;  
# MAGIC 	set work.max_ym_1;  
# MAGIC 	max_year = put('年'n,Z4.);  
# MAGIC 	max_month = put('月'n,Z2.);  
# MAGIC 	if _n_ = 2 then stop;  
# MAGIC run;
# MAGIC 
# MAGIC data _null_;  
# MAGIC 	set work.max_ym_2;  
# MAGIC 	call symputx("max_ym", cats(max_year, max_month));  
# MAGIC run;
# MAGIC 
# MAGIC %put &=max_ym.;  
# MAGIC 
# MAGIC proc delete data=work.max_ym_1;  
# MAGIC proc delete data=work.max_ym_2;  
# MAGIC run;

# COMMAND ----------

from pyspark.sql.functions import max
sales_transform_df = spark.read.table("default.sales_transform")
max_year = sales_transform_df.agg(max("年")).head()[0]
max_month = sales_transform_df.agg(max("月")).head()[0]
max_ym = str(max_year).zfill(4) + str(max_month).zfill(2)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: データ (sales.csv) のインポート   */
# MAGIC 
# MAGIC 自動生成のため省略

# COMMAND ----------

import os
sales_df = (spark.read.
      format("csv")
      .option("header","true")
      .option("inferschema","true")
      .option('charset', 'shift-jis')
      .load(f"file:{os.getcwd()}/sales.csv")
     )
# display(sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 蓄積データより新しいデータのみ抽出   */
# MAGIC 
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.SALES_NEW AS   
# MAGIC    SELECT t1.'年'n,  
# MAGIC           t1.'月'n,  
# MAGIC           t1.'代表得意先'n,  
# MAGIC           t1.'代表特約店'n,  
# MAGIC           t1.'送り先郵便番号'n,  
# MAGIC           t1.'区分'n,  
# MAGIC           t1.'機種名'n,  
# MAGIC           t1.'間口'n,  
# MAGIC           t1.'奥行'n,  
# MAGIC           t1.'高さ'n,  
# MAGIC           t1.'色'n,  
# MAGIC           t1.'数量'n,  
# MAGIC           t1.'単価'n,  
# MAGIC           t1.'金額'n  
# MAGIC       FROM WORK.SALES t1  
# MAGIC       WHERE "&max_ym." < cats(put(t1.'年'n,Z4.),put(t1.'月'n,Z2.));  
# MAGIC QUIT;

# COMMAND ----------

from pyspark.sql.functions import col, concat, format_string
sales_new_df = sales_df.filter(max_ym < concat(format_string("%04d", col('年')),format_string("%02d", col('月'))))

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 最新の郵便番号データのダウンロード   */
# MAGIC 
# MAGIC data _null_;  
# MAGIC 	call system("bitsadmin /TRANSFER DOWNLOAD_KEN https://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip C:\TEMP\ken_all.zip");  
# MAGIC run;

# COMMAND ----------

import requests

url = 'http://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip'

filename = url.split('/')[-1]
r = requests.get(url, stream=True)
# download the gzipped file
request = requests.get(url)
with open('/dbfs/tmp/' + filename, 'wb') as f:
    f.write(request.content)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: ダウンロードしたzipファイルの解凍   */
# MAGIC data _null_;  
# MAGIC 	call system('call powershell -command "Expand-Archive -Force C:\TEMP\ken_all.zip C:\TEMP"');  
# MAGIC run;  

# COMMAND ----------

import zipfile

target_directory = "/dbfs/tmp/"
zfile = zipfile.ZipFile("/dbfs/tmp/" + filename)
zfile.extractall(target_directory)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 解凍したファイルの読み込み   */  
# MAGIC proc import datafile="C:\TEMP\KEN_ALL.CSV" dbms=csv out=work.ken_all replace;  
# MAGIC 	getnames=no;  
# MAGIC run;

# COMMAND ----------

ken_all_df = (spark.read
       .format("csv")
       .option("header","false")
       .option("inferschema","true")
       .option('charset', 'shift-jis')
       .load("dbfs:/tmp/KEN_ALL.CSV")
      )
# display(ken_all_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 列名の変換   */
# MAGIC 
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.KEN_ALL_NAME AS  
# MAGIC    SELECT t1.VAR1 AS '全国地方公共団体コード'n,  
# MAGIC           t1.VAR2 AS '（旧）郵便番号（5桁）'n,  
# MAGIC           t1.VAR3 AS '郵便番号（7桁）'n,  
# MAGIC           t1.VAR4 AS '都道府県名（カナ）'n,  
# MAGIC           t1.VAR5 AS '市区町村名（カナ）'n,  
# MAGIC           t1.VAR6 AS '町域名（カナ）'n,  
# MAGIC           t1.VAR7 AS '都道府県名'n,  
# MAGIC           t1.VAR8 AS '市区町村名'n,  
# MAGIC           t1.VAR9 AS '町域名'n,  
# MAGIC           t1.VAR10 AS '一町域が二以上の郵便番号で'n,  
# MAGIC           t1.VAR11 AS '小字毎に番地が起番'n,  
# MAGIC           t1.VAR12 AS '丁目を有する町域の場合の表示'n,  
# MAGIC           t1.VAR13 AS '一つの郵便番号で二以上の町域'n,  
# MAGIC           t1.VAR14 AS '更新の表示'n,  
# MAGIC           t1.VAR15 AS '変更理由'n  
# MAGIC       FROM WORK.KEN_ALL t1; 
# MAGIC QUIT;  

# COMMAND ----------

# PySparkは結合のため列名を統一する必要あり
ken_all_name_df = (ken_all_df
       .select(["_c2","_c6"])
       .withColumnRenamed("_c2", "送り先郵便番号")
       .withColumnRenamed("_c6", "都道府県名")
      )
# display(ken_all_name_df)

# COMMAND ----------

# MAGIC %md  
# MAGIC /*   開始ノード: 都道府県名の付与   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.SELL_KEN AS   
# MAGIC    SELECT t1.'年'n,   
# MAGIC           t1.'月'n,   
# MAGIC           t1.'送り先郵便番号'n,   
# MAGIC           t2.'都道府県名'n,   
# MAGIC           t1.'機種名'n,   
# MAGIC           t1.'色'n,   
# MAGIC           t1.'数量'n,   
# MAGIC           t1.'単価'n,   
# MAGIC           t1.'金額'n  
# MAGIC       FROM WORK.SALES_NEW t1  
# MAGIC            LEFT JOIN WORK.KEN_ALL_NAME t2 ON (t1.'送り先郵便番号'n = t2.'郵便番号（7桁）'n);  
# MAGIC QUIT; 

# COMMAND ----------

sell_ken_df = sales_new_df.join(ken_all_name_df, ["送り先郵便番号"], "left_outer")
# display(sell_ken_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: データ (ken_area.xlsx[Sheet1]) のインポート   */  
# MAGIC 自動生成のため省略

# COMMAND ----------

import pandas
import io
import os

df = (spark
      .read
      .format("binaryFile")
      .load(f"file:{os.getcwd()}/ken_area.xlsx")
     )

bstream = io.BytesIO(df.select("content").collect()[0].content)
pdf = pandas.read_excel(bstream)
ken_area_df = spark.createDataFrame(pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 地方の付与   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.SELL_AREA AS   
# MAGIC    SELECT t1.'年'n,   
# MAGIC           t1.'月'n,   
# MAGIC           t1.'送り先郵便番号'n,   
# MAGIC           t1.'都道府県名'n,   
# MAGIC           t1.'機種名'n,   
# MAGIC           t1.'色'n,   
# MAGIC           t1.'数量'n,   
# MAGIC           t1.'単価'n,   
# MAGIC           t1.'金額'n,   
# MAGIC           t2.'地方'n  
# MAGIC       FROM WORK.SELL_KEN t1  
# MAGIC            INNER JOIN WORK.KEN_AREA t2 ON (t1.'都道府県名'n = t2.'都道府県名'n);  
# MAGIC QUIT;

# COMMAND ----------

sell_area_df = sell_ken_df.join(ken_area_df, ["都道府県名"], "inner")
# display(sell_area_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 地方ごと年月ごと機種販売数量   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.SELL_AREA_SUM AS   
# MAGIC    SELECT t1.'機種名'n,   
# MAGIC           t1.'色'n,   
# MAGIC           t1.'地方'n,   
# MAGIC           t1.'年'n,   
# MAGIC           t1.'月'n,   
# MAGIC           /* SUM_of_数量 */  
# MAGIC             (SUM(t1.'数量'n)) FORMAT=BEST12. AS 'SUM_of_数量'n  
# MAGIC       FROM WORK.SELL_AREA t1  
# MAGIC       GROUP BY t1.'機種名'n,  
# MAGIC                t1.'色'n,  
# MAGIC                t1.'地方'n,  
# MAGIC                t1.'年'n,  
# MAGIC                t1.'月'n;  
# MAGIC QUIT;

# COMMAND ----------

sell_area_sum_df = (sell_area_df
                    .groupBy("機種名","色","地方","年","月").sum("数量")
                    .withColumnRenamed("sum(数量)","SUM_of_数量")
                    )
# display(sell_area_sum_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: データの追加   */  
# MAGIC proc append base=ARCHIVE.SALES_SAS data=work.sell_area_sum;  
# MAGIC run;  
# MAGIC 
# MAGIC Sparkの場合wright,insertなら元データをメモリ経由せずに行けるかも？

# COMMAND ----------

sales_transform_df = sales_transform_df.unionAll(sell_area_sum_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: キー項目のみ重複なし抽出   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.SELL_AREA_DISTINCT AS   
# MAGIC    SELECT DISTINCT t1.'機種名'n,   
# MAGIC           t1.'色'n,   
# MAGIC           t1.'地方'n  
# MAGIC       FROM ARCHIVE.SALES_SAS t1;  
# MAGIC QUIT;

# COMMAND ----------

sell_area_distinct_df = (sales_transform_df
                         .select("機種名","色","地方")
                         .distinct()
                        )
# display(sell_area_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 年月を日付値に変換   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.YM_SAS AS   
# MAGIC    SELECT DISTINCT t1.'年'n,   
# MAGIC           t1.'月'n,   
# MAGIC           /* date */  
# MAGIC             (input(cats(put(t1.'年'n,Z4.),put(t1.'月'n,Z2.),"01"), yymmdd8.)) FORMAT=YYMMDDN8. AS date  
# MAGIC       FROM ARCHIVE.SALES_SAS t1;  
# MAGIC QUIT;

# COMMAND ----------

from pyspark.sql.functions import lit, to_date
ym_df = (sales_transform_df
         .select("年","月")
         .distinct()
         .withColumn("date", to_date(concat(format_string("%04d", col('年')),format_string("%02d", col('月')),lit("01")),"yyyyMMdd"))
        )
# display(ym_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 日付の最小値、最大値を算出   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.YM_SAS_RANGE AS   
# MAGIC    SELECT /* MIN_of_date */  
# MAGIC             (MIN(t1.date)) FORMAT=YYMMDDN8. AS MIN_of_date,   
# MAGIC           /* MAX_of_date */  
# MAGIC             (MAX(t1.date)) FORMAT=YYMMDDN8. AS MAX_of_date  
# MAGIC       FROM WORK.YM_SAS t1;  
# MAGIC QUIT;  

# COMMAND ----------

from pyspark.sql.functions import min
max_date = ym_df.agg(max("date")).head()[0]
min_date = ym_df.agg(min("date")).head()[0]
# display(max_date)
# display(min_date)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 最小値と最大値の間を埋めてデータ期間をリストアップする   */  
# MAGIC data _null_;  
# MAGIC 	set work.YM_SAS_RANGE;  
# MAGIC 	call symputx("min_date", MIN_of_date);  
# MAGIC 	call symputx("max_date", MAX_of_date);  
# MAGIC run;  
# MAGIC   
# MAGIC %put &=min_date.;  
# MAGIC %put &=max_date.;  
# MAGIC   
# MAGIC data work.ym_range_fix;  
# MAGIC 	do date = &min_date. to &max_date.;  
# MAGIC 		year = year(date);  
# MAGIC 		month = month(date);  
# MAGIC 		kbn = "ACT";  
# MAGIC 		output;  
# MAGIC 	end;  
# MAGIC run;

# COMMAND ----------

from pyspark.sql.functions import year, month
ym_range_fix_list = pandas.date_range(start=min_date, end=max_date, freq="D").to_frame()
ym_range_fix_df = (spark
                   .createDataFrame(ym_range_fix_list)
                   .withColumn("year", year("0"))
                   .withColumn("month", month("0"))
                   .withColumn("kbn", lit("ACT"))
                   .select("year", "month", "kbn")
                  )
# display(ym_range_fix_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 期間の重複除去   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.YM_RANGE AS   
# MAGIC    SELECT DISTINCT t1.year,   
# MAGIC           t1.month,   
# MAGIC           t1.kbn  
# MAGIC       FROM WORK.YM_RANGE_FIX t1;  
# MAGIC QUIT;  

# COMMAND ----------

ym_range_df = ym_range_fix_df.distinct()
# display(ym_range_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 予測範囲のデータを作成   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.YM_PRED__RANGE AS   
# MAGIC    SELECT t1.month,   
# MAGIC           /* year */  
# MAGIC             ((MAX(t1.year)) + 1) AS year,   
# MAGIC           /* kbn */  
# MAGIC             ("PRED") AS kbn  
# MAGIC       FROM WORK.YM_RANGE t1  
# MAGIC       GROUP BY t1.month,  
# MAGIC                (CALCULATED kbn);  
# MAGIC QUIT;  

# COMMAND ----------

from dateutil.relativedelta import relativedelta
pred_start = max_date + relativedelta(months=1)
pred_end = max_date + relativedelta(years=1)
ym_pred_range_fix_list = pandas.date_range(start=pred_start, end=pred_end, freq="D").to_frame()
ym_pred_range_df = (spark
                 .createDataFrame(pred_range_fix_list)
                 .withColumn("year", year("0"))
                 .withColumn("month", month("0"))
                 .withColumn("kbn", lit("PRED"))
                 .select("year", "month", "kbn")
                 .distinct()
                )
# display(ym_pred_range_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: テーブルの追加   */  
# MAGIC PROC SQL;  
# MAGIC CREATE TABLE WORK.YM_RANGE_P AS   
# MAGIC SELECT * FROM WORK.YM_RANGE  
# MAGIC  OUTER UNION CORR   
# MAGIC SELECT * FROM WORK.YM_PRED__RANGE  
# MAGIC ;  
# MAGIC Quit;  

# COMMAND ----------

ym_range_p_df = ym_range_df.unionAll(ym_pred_range_df)
# display(ym_range_p_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: キー項目と期間をクロス結合し、数量を0埋め   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.SELL_AREA_YM_CROSS AS   
# MAGIC    SELECT t1.'機種名'n,   
# MAGIC           t1.'色'n,   
# MAGIC           t1.'地方'n,   
# MAGIC           t2.year,   
# MAGIC           t2.month,   
# MAGIC           /* SUM_of_数量 */  
# MAGIC             (0) AS 'SUM_of_数量'n,   
# MAGIC           t2.kbn  
# MAGIC       FROM WORK.SELL_AREA_DISTINCT t1  
# MAGIC            CROSS JOIN WORK.YM_RANGE_P t2;  
# MAGIC QUIT;

# COMMAND ----------

# PySparkは結合のため列名を統一する必要あり
# coalesce利用列の列名を変えておく
sell_area_ym_cross_df = (sell_area_distinct_df
                         .crossJoin(ym_range_p_df)
                         .withColumn("SUM_of_数量_Z", lit(0))
                         .withColumnRenamed("year", "年")
                         .withColumnRenamed("month", "月")
                        )
# display(sell_area_ym_cross_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: 抜けているレコードを補完する   */  
# MAGIC PROC SQL;  
# MAGIC    CREATE TABLE WORK.SALES_SAS_FIX AS   
# MAGIC    SELECT t2.'機種名'n,   
# MAGIC           t2.'色'n,   
# MAGIC           t2.'地方'n,   
# MAGIC           t2.year AS '年'n,   
# MAGIC           t2.month AS '月'n,   
# MAGIC           /* SUM_of_数量 */  
# MAGIC             (COALESCE(t1.'SUM_of_数量'n,t2.'SUM_of_数量'n)) AS 'SUM_of_数量'n,   
# MAGIC           t2.kbn  
# MAGIC       FROM ARCHIVE.SALES_SAS t1  
# MAGIC            RIGHT JOIN WORK.SELL_AREA_YM_CROSS t2 ON (t1.'機種名'n = t2.'機種名'n) AND (t1.'色'n = t2.'色'n) AND (t1.'地方'n = t2.  
# MAGIC           '地方'n) AND (t1.'年'n = t2.year) AND (t1.'月'n = t2.month);  
# MAGIC QUIT;  

# COMMAND ----------

# 時系列に投げ込むために列追加
from pyspark.sql.functions import coalesce
sales_fix_df = (sell_area_ym_cross_df
                .join(sales_transform_df, ["機種名","色","地方","年","月"], "left_outer")
                .withColumn("SUM_of_数量", coalesce("SUM_of_数量", "SUM_of_数量_Z"))
                .drop("SUM_of_数量_Z")
                .withColumn("date", to_date(concat(format_string("%04d", col('年')),format_string("%02d", col('月')),lit("01")),"yyyyMMdd"))
               )
# display(sales_fix_df)

# COMMAND ----------

# MAGIC %md
# MAGIC /*   開始ノード: モデル学習   */  
# MAGIC /* 今回はSASライセンスの関係でモデルは適当に重回帰に放り込んで作成しているので非掲載。 */  

# COMMAND ----------

train_df = sales_fix_df.filter(col("kbn") == "ACT")

# COMMAND ----------

# AutoMLで実行
import databricks.automl
import logging

# fbprophetの情報レベルのメッセージを無効化
logging.getLogger("py4j").setLevel(logging.WARNING)

summary = databricks.automl.regress(train_df, target_col="SUM_of_数量", time_col="date", timeout_minutes = 10)

# COMMAND ----------

help(summary)

# COMMAND ----------

model_uri = summary.best_trial.model_path

import mlflow

# テストデータセットの準備
# 今回はtrain_dfをそのまま利用
test_df = train_df

# features = mlflow.pyfunc.load_model(model_uri).metadata.get_input_schema()
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)
display(test_df.withColumn("SUM_of_数量_predicted", predict_udf()))

