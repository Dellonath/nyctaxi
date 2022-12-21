import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df_nyctaxi_2009 = spark.read.parquet("s3://dadosfera-dev/datalake/trusted/nyctaxy/nyctaxi-trips-2009/")
df_nyctaxi_2010 = spark.read.parquet("s3://dadosfera-dev/datalake/trusted/nyctaxy/nyctaxi-trips-2010/")
df_nyctaxi_2011 = spark.read.parquet("s3://dadosfera-dev/datalake/trusted/nyctaxy/nyctaxi-trips-2011/")
df_nyctaxi_2012 = spark.read.parquet("s3://dadosfera-dev/datalake/trusted/nyctaxy/nyctaxi-trips-2012/")

df_nyctaxi = df_nyctaxi_2009.unionByName(df_nyctaxi_2010)
df_nyctaxi = df_nyctaxi.unionByName(df_nyctaxi_2011)
df_nyctaxi = df_nyctaxi.unionByName(df_nyctaxi_2012)

df_nyctaxi = df_nyctaxi.select('dt_dropoff', 
                               'nr_dropoff_latitude', 
                               'nr_dropoff_longitude', 
                               'vl_fare_amount', 
                               'qt_passenger', 
                               'dt_pickup', 
                               'nr_pickup_latitude', 
                               'nr_pickup_longitude',
                               'cd_vendor', 
                               'nm_vendor_name',
                               'vl_tip',
                               'vl_total',
                               'nr_trip_distance',
                               'nm_payment_type')

df_nyctaxi.coalesce(1).write.mode('overwrite').parquet("s3://dadosfera-dev/datalake/refined/nyctaxy/nyctaxi-trips.parquet")

job.commit()