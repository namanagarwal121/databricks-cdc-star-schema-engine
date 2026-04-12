# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# #calalog name
# catalog = 'workspace'

# # key columns list
# val = "['passenger_id']"
# key_cols_list = eval(val)

# #cdc column
# cdc_col = 'modifiedDate'

# #backdated refresh
# backdated_refresh= ''

# #source object
# source_object ='silver_passengers'

# #schema
# source_schema = 'silver'

# #target object
# target_object ='DimPassengers'

# #schema
# target_schema = 'gold'


# #surrogate key
# surrogate_key = 'DimPassengersKey'


# COMMAND ----------

# #calalog name
# catalog = 'workspace'

# # key columns list
# val = "['airport_id']"
# key_cols_list = eval(val)

# #cdc column
# cdc_col = 'modifiedDate'

# #backdated refresh
# backdated_refresh= ''

# #source object
# source_object ='silver_airports'

# #schema
# source_schema = 'silver'

# #target object
# target_object ='DimAirports'

# #schema
# target_schema = 'gold'


# #surrogate key
# surrogate_key = 'DimAirportsKey'


# COMMAND ----------

#calalog name
catalog = 'workspace'

# key columns list
val = "['flight_id']"
key_cols_list = eval(val)

#cdc column
cdc_col = 'modifiedDate'

#backdated refresh
backdated_refresh= ''

#source object
source_object ='silver_flight'

#schema
source_schema = 'silver'

#target object
target_object ='dimflights'

#schema
target_schema = 'gold'


#surrogate key
surrogate_key = 'DimFlightsKey'


# COMMAND ----------

# MAGIC %md
# MAGIC #Incremental data ingestion

# COMMAND ----------

if len(backdated_refresh) == 0:
  
  if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
      
      last_load = spark.sql(f"select max({cdc_col}) from {catalog}.{target_schema}.{target_object}").collect()[0][0]

  else:
      last_load = '1900-01-01 00:00:00'
      
else:
  last_load = backdated_refresh


# COMMAND ----------

df_src = spark.sql(f"select * from {catalog}.{source_schema}.{source_object} where {cdc_col} >= '{last_load}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Old Vs New Records
# MAGIC

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    key_cols_str = ', '.join(key_cols_list)
    df_tgt = spark.sql(f"select {key_cols_str} , {surrogate_key} , create_date , update_date from {catalog}.{target_schema}.{target_object}")
else:
    key_cols_str_init = [f"'' As {key_col}" for key_col in key_cols_list]
    key_cols_str = ', '.join(key_cols_str_init)
    df_tgt = spark.sql(f"select {key_cols_str} , 0 as {surrogate_key} , cast('1900-01-01 00:00:00' as timestamp) as create_date , cast('1900-01-01 00:00:00' as timestamp) as update_date where 1 = 0")

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_tgt.createOrReplaceTempView("tgt")

join_condition = ' AND '.join([f"src.{key_col} == tgt.{key_col}" for key_col in key_cols_list])

df_join = spark.sql(f"""
          select src.*
          ,tgt.{surrogate_key} as {surrogate_key}
          ,tgt.create_date as create_date
          ,tgt.update_date as update_date
          from src
          left join tgt
          on {join_condition}
          """)

# COMMAND ----------

df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())
df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Preparing DF_OLD

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing DF_new

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"select max({surrogate_key}) from {catalog}.{target_schema}.{target_object}").collect()[0][0]
    df_new_enr = df_new.withColumn(f'{surrogate_key}', monotonically_increasing_id() + lit(1) + max_surrogate_key)\
        .withColumn('create_date',current_timestamp())\
        .withColumn('update_date',current_timestamp())
else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}', monotonically_increasing_id() + lit(1) + max_surrogate_key)\
        .withColumn('create_date',current_timestamp())\
        .withColumn('update_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Unioning old and new records**

# COMMAND ----------

df_new_enr_final = df_new_enr.withColumn(f'{surrogate_key}', col(f'{surrogate_key}').cast('string'))
df_union = df_old_enr.unionByName(df_new_enr_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("tgt").merge(
        df_union.alias("src"),
        f"tgt.{surrogate_key} = src.{surrogate_key}"
    ).whenMatchedUpdateAll(condition= f"src.{cdc_col} >= tgt.{cdc_col}").whenNotMatchedInsertAll().execute()

else:
    df_union.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

