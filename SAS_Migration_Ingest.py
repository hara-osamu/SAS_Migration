# Databricks notebook source
# MAGIC %md
# MAGIC 下記データの取得、テーブル化  
# MAGIC - sales.csv  
# MAGIC 　販売実績テーブル。本来は外部DB等からの取得になると思われる。  
# MAGIC - ken_area.xlsx  
# MAGIC 　都道府県と地方の対応マスタ情報。SASではお客様が直接触れるような外部の定義ファイルを読み込んでいるケースが結構あるのでそれを模した。  
# MAGIC 　直接触れる形式であればExcelじゃなくても問題ないはずなので、ケース次第で要検討。  
# MAGIC - ken_all.zip  
# MAGIC 　https://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip のデータのダウンロード、解凍、読み込み。  

# COMMAND ----------

import os
# 入力データ(前回実行分を模したもの)の準備
spark.read.format("csv").option("header","true").option("inferschema","true").option('charset', 'shift-jis').load(f"file:{os.getcwd()}/sales_transform.csv").write.format("delta").mode("overwrite").saveAsTable("sas_migration.sales_transform")

# COMMAND ----------

import os
df = (spark.read.
      format("csv")
      .option("header","true")
      .option("inferschema","true")
      .option('charset', 'shift-jis')
      .load(f"file:{os.getcwd()}/sales.csv")
     )
df.write.format("delta").mode("overwrite").saveAsTable("sas_migration.sales")

# COMMAND ----------

# クラスターにMavenからcom.crealytics:excel_2.12をインストールする必要あり
import os
df = (spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .load(f"file:{os.getcwd()}/ken_area.xlsx")
     )
df.write.format("delta").mode("overwrite").saveAsTable("sas_migration.ken_area")

# COMMAND ----------

import requests

url = 'http://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip'

filename = url.split('/')[-1]
r = requests.get(url, stream=True)
# download the gzipped file
request = requests.get(url)
with open('/dbfs/tmp/' + filename, 'wb') as f:
    f.write(request.content)

import zipfile

target_directory = "/dbfs/tmp/"
zfile = zipfile.ZipFile("/dbfs/tmp/" + filename)
zfile.extractall(target_directory)

df = (spark.read
       .format("csv")
       .option("header","false")
       .option("inferschema","true")
       .option('charset', 'shift-jis')
       .load("dbfs:/tmp/KEN_ALL.CSV")
      )
df.write.format("delta").mode("overwrite").saveAsTable("sas_migration.ken_all")
