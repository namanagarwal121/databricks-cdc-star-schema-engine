import dlt
from pyspark.sql.functions import col, current_timestamp

# BOOKINGS PIPELINE

rules = {
    "rule01": "booking_id IS NOT NULL",
    "rule02": "passenger_id IS NOT NULL",
}

@dlt.view(name="stage_booking")
@dlt.expect_all_or_drop(rules)
def stage_booking():
    return spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")

@dlt.view(name="trans_booking")
def trans_booking():
    return (
        dlt.read_stream("stage_booking")
        .drop("_rescued_data")
        .withColumn("amount", col("amount").cast("decimal(18,2)"))
        .withColumn("booking_date", col("booking_date").cast("date"))
        .withColumn("modifiedDate", current_timestamp())
    )

@dlt.table(name="silver_bookings")
def silver_bookings():
    return dlt.read_stream("trans_booking")

# FLIGHTS PIPELINE (CDC)

@dlt.view(name="stage_flight")
def stage_flight():
    return spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")

@dlt.view(name="vw_transFlights")
def vw_transFlights():
    return (
        dlt.read_stream("stage_flight")
        .drop("_rescued_data")
        .withColumn("flight_date", col("flight_date").cast("date"))
        .withColumn("modifiedDate", current_timestamp())
    )

dlt.create_streaming_table(name="silver_flight")

dlt.create_auto_cdc_flow(
    target="silver_flight",
    source="vw_transFlights",
    keys=["flight_id"],
    sequence_by="modifiedDate",
    stored_as_scd_type=1
)

# PASSENGERS PIPELINE (CDC)

@dlt.view(name="stage_customers")
def stage_customers():
    return spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")


@dlt.view(name="vw_transCustomers")
def vw_transCustomers():
    return (
        dlt.read_stream("stage_customers")
        .drop("_rescued_data")
        .withColumn("modifiedDate", current_timestamp())
    )

dlt.create_streaming_table(name="silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="vw_transCustomers",
    keys=["passenger_id"],
    sequence_by="modifiedDate",
    stored_as_scd_type=1
)

# AIRPORTS PIPELINE (CDC)

@dlt.view(name="stage_airports")
def stage_airports():
    return spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")

@dlt.view(name="vw_transAirports")
def vw_transAirports():
    return (
        dlt.read_stream("stage_airports")
        .drop("_rescued_data")
        .withColumn("modifiedDate", current_timestamp())
    )

dlt.create_streaming_table(name="silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="vw_transAirports",
    keys=["airport_id"],
    sequence_by="modifiedDate",
    stored_as_scd_type=1
)