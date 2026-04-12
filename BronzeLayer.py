# Databricks notebook source
# MAGIC %md
# MAGIC ##Incremental Data Ingestion##

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get("src")
src_value

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
                    .option("cloudFiles.format", "csv")\
                    .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
                    .option("cloudFiles.schemaEvolutionMode", "rescue")\
                    .load(f"/Volumes/workspace/raw/volume/rawdata/{src_value}/")

# COMMAND ----------

df.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
    .outputMode("append")\
    .trigger(availableNow=True)\
    .start(f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")