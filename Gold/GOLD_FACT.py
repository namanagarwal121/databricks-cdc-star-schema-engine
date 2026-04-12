# Databricks notebook source

catalog = 'workspace'

cdc_col = 'modifiedDate'
backdated_refresh = ''

source_object = 'silver_bookings'
source_schema = 'silver'

target_object = 'factbookings'   

target_schema = 'gold'

#fct key cols list
fact_key_cols = "['DimPassengersKey', 'DimFlightsKey', 'DimAirportsKey']"
fact_keys = eval(fact_key_cols)

fact_table = f"{catalog}.{source_schema}.{source_object}"

# COMMAND ----------

dimensions = [
    {
        "table": "workspace.gold.dimpassengers",
        "alias": "dp",
        "join_keys": [("passenger_id", "passenger_id")],
        "surrogate_key": "DimPassengersKey"
    },
    {
        "table": "workspace.gold.dimflights",
        "alias": "df",
        "join_keys": [("flight_id", "flight_id")],
        "surrogate_key": "DimFlightsKey"
    },
    {
        "table": "workspace.gold.dimairports",
        "alias": "da",
        "join_keys": [("airport_id", "airport_id")],
        "surrogate_key": "DimAirportsKey"
    }
]

fact_columns = ["amount", "booking_date", "modifiedDate"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last Load Data

# COMMAND ----------

if not backdated_refresh:

    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

        last_load = spark.sql(
            f"""
            SELECT COALESCE(MAX({cdc_col}), TIMESTAMP('1900-01-01 00:00:00'))
            FROM {catalog}.{target_schema}.{target_object}
            """
        ).collect()[0][0]

    else:
        last_load = "1900-01-01 00:00:00"

else:
    last_load = backdated_refresh


# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_column, last_load):

    fact_alias = "f"

    # Select base fact columns
    select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    join_clauses = []

    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]

        surrogate_key = f"{alias}.{dim['surrogate_key']}"
        select_cols.append(surrogate_key)

        on_conditions = [
            f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]
        ]

        join_clause = f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_conditions)
        join_clauses.append(join_clause)

    select_clause = ",\n    ".join(select_cols)
    joins = "\n".join(join_clauses)

    where_clause = f"{fact_alias}.{cdc_column} > TIMESTAMP('{last_load}')"

    query = f"""
    SELECT
        {select_clause}
    FROM {fact_table} {fact_alias}
    {joins}
    WHERE {where_clause}
    """.strip()

    return query


# COMMAND ----------

query = generate_fact_query_incremental(
    fact_table,
    dimensions,
    fact_columns,
    cdc_col,
    last_load
)

df_fact = spark.sql(query)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable 

# COMMAND ----------

fact_key_cols_str = " And ".join([f"tgt.{col} = src.{col}" for col in fact_keys]) 

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("tgt").merge(
        df_fact.alias("src"),
        fact_key_cols_str
    ).whenMatchedUpdateAll(condition= f"src.{cdc_col} >= tgt.{cdc_col}").whenNotMatchedInsertAll().execute()

else:
    df_fact.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

