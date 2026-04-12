# Databricks notebook source
# MAGIC %sql
# MAGIC create volume workspace.raw.volume
# MAGIC   
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/volume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/flights/data/`

# COMMAND ----------

