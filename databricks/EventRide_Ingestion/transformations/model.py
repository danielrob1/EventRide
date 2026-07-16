from pyspark import pipelines as dp
# Dim Passenger

@dp.view
def dim_passenger_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("passenger_id","passenger_name","passenger_email","passenger_phone")
    df = df.dropDuplicates(subset=["passenger_id"])
    return df


dp.create_streaming_table("dim_passenger")
dp.create_auto_cdc_flow(
    target ="dim_passenger",
    source="dim_passenger_view",
    keys=["passenger_id"],
    sequence_by="passenger_id",
    stored_as_scd_type=1,

)

# Dim driver
@dp.view
def dim_driver_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("driver_id","driver_name","driver_rating","driver_phone", "driver_license")
    df = df.dropDuplicates(subset=["driver_id"])
    return df


dp.create_streaming_table("dim_driver")
dp.create_auto_cdc_flow(
    target ="dim_driver",
    source="dim_driver_view",
    keys=["driver_id"],
    sequence_by="driver_id",
    stored_as_scd_type=1,
)