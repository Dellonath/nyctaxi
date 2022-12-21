import sys
from awsglue.transforms import *
from pyspark.sql import functions as f
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "YEAR_FILE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

nyctaxi_year = args["YEAR_FILE"]

df_nyctaxi = spark.read.option("multiline", "false").json(f's3://dadosfera-dev/datalake/raw/nyctaxy/data-nyctaxi-trips-{nyctaxi_year}.json')
df_payment_lookup = spark.read.option('header', 'true').csv('s3://dadosfera-dev/datalake/raw/lookup/data-payment_lookup.csv')
df_vendor_lookup = spark.read.option('header', 'true').csv('s3://dadosfera-dev/datalake/raw/lookup/data-vendor_lookup.csv')

df_nyctaxi = df_nyctaxi.join(df_payment_lookup, how = 'left', on = 'payment_type')
df_nyctaxi = df_nyctaxi.drop('payment_type')

df_nyctaxi = df_nyctaxi.join(df_vendor_lookup, how = 'left', on = 'vendor_id')

df_nyctaxi = df_nyctaxi.withColumnRenamed('dropoff_datetime', 'dt_dropoff') \
                       .withColumnRenamed('dropoff_latitude', 'nr_dropoff_latitude') \
                       .withColumnRenamed('dropoff_longitude', 'nr_dropoff_longitude') \
                       .withColumnRenamed('fare_amount', 'vl_fare_amount') \
                       .withColumnRenamed('passenger_count', 'qt_passenger') \
                       .withColumnRenamed('payment_lookup', 'nm_payment_type') \
                       .withColumnRenamed('pickup_datetime', 'dt_pickup') \
                       .withColumnRenamed('dropoff_latitude', 'nr_dropoff_latitude') \
                       .withColumnRenamed('pickup_longitude', 'nr_pickup_longitude') \
                       .withColumnRenamed('rate_code', 'cd_rate') \
                       .withColumnRenamed('store_and_fwd_flag', 'in_store_and_fwd') \
                       .withColumnRenamed('surcharge', 'vl_surcharge') \
                       .withColumnRenamed('tip_amount', 'vl_tip') \
                       .withColumnRenamed('pickup_latitude', 'nr_pickup_latitude') \
                       .withColumnRenamed('trip_distance', 'nr_trip_distance') \
                       .withColumnRenamed('tolls_amount', 'vl_tolls') \
                       .withColumnRenamed('total_amount', 'vl_total') \
                       .withColumnRenamed('vendor_id', 'cd_vendor') \
                       .withColumnRenamed('name', 'nm_vendor_name') \
                       .withColumnRenamed('address', 'nm_vendor_address') \
                       .withColumnRenamed('city', 'nm_vendor_city') \
                       .withColumnRenamed('state', 'nm_vendor_state') \
                       .withColumnRenamed('zip', 'cd_vendor_zip') \
                       .withColumnRenamed('country', 'nm_vendor_country') \
                       .withColumnRenamed('contact', 'ds_vendor_contact') \
                       .withColumnRenamed('current', 'in_vendor_current')

df_nyctaxi = df_nyctaxi.withColumn('dt_dropoff', f.date_format('dt_dropoff', 'yyyy-MM-dd HH:mm:ss'))
df_nyctaxi = df_nyctaxi.withColumn('dt_pickup', f.date_format('dt_pickup', 'yyyy-MM-dd HH:mm:ss'))
                       
df_nyctaxi.coalesce(1).write.mode('overwrite').parquet(f"s3://dadosfera-dev/datalake/trusted/nyctaxy/nyctaxi-trips-{nyctaxi_year}")

job.commit()