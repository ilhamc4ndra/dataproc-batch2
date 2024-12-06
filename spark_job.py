import argparse
import pyspark
import gcsfs
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, DoubleType
from pyspark.sql.functions import lit, col

def fix_schema(taxi_type, data):
  if "green" in taxi_type:
    schemas = StructType(
      [
        StructField('VendorID', IntegerType(), True),
        StructField('pickup_datetime', TimestampType(), True),
        StructField('dropoff_datetime', TimestampType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('RatecodeID', IntegerType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True),
        StructField('passenger_count', IntegerType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('fare_amount', DoubleType(), True),
        StructField('extra', DoubleType(), True),
        StructField('mta_tax', DoubleType(), True),
        StructField('tip_amount', DoubleType(), True),
        StructField('tolls_amount', DoubleType(), True),
        StructField('ehail_fee', DoubleType(), True),
        StructField('improvement_surcharge', DoubleType(), True),
        StructField('total_amount', DoubleType(), True),
        StructField('payment_type', IntegerType(), True),
        StructField('trip_type', IntegerType(), True),
        StructField('congestion_surcharge', DoubleType(), True)
      ]
    )
    service_type = "green"
  elif "yellow" in taxi_type:
    schemas = StructType(
      [
        StructField('VendorID', IntegerType(), True),
        StructField('pickup_datetime', TimestampType(), True),
        StructField('dropoff_datetime', TimestampType(), True),
        StructField('passenger_count', IntegerType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('RatecodeID', IntegerType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True),
        StructField('payment_type', IntegerType(), True),
        StructField('fare_amount', DoubleType(), True),
        StructField('extra', DoubleType(), True),
        StructField('mta_tax', DoubleType(), True),
        StructField('tip_amount', DoubleType(), True),
        StructField('tolls_amount', DoubleType(), True),
        StructField('improvement_surcharge', DoubleType(), True),
        StructField('total_amount', DoubleType(), True),
        StructField('congestion_surcharge', DoubleType(), True),
        StructField('Airport_fee', DoubleType(), True)
      ]
    )
    service_type = "yellow"
  else:
    return

  common_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
  ]

  read_schema = data.schema
  temp_df = data
  for c in temp_df.columns:
    if c == "airport_fee":
      col_name = "Airport_fee"
    else:
      col_name = c

    if read_schema[c].dataType != schemas[col_name].dataType: 
      temp_df = temp_df.withColumn(c, col(c).cast(schemas[col_name].dataType).alias(col_name))
  
  return temp_df.select(common_columns).withColumn('service_type', lit(service_type)) 


def main(args):
  input_green = args.input_green
  input_yellow = args.input_yellow
  output = args.output
  temp_bucket = args.temp_bucket
  write_mode = args.write_mode

  spark = SparkSession.builder \
      .appName('DemoProcess') \
      .getOrCreate()

  spark.conf.set('temporaryGcsBucket', temp_bucket)
  spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

  fs = gcsfs.GCSFileSystem()

  df_green = None
  for idx, input_path in enumerate(fs.glob(input_green)):
    temp_df = spark.read.parquet(f"gs://{input_path}")
    temp_df = temp_df \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
    temp_df = fix_schema(input_path, temp_df)

    if idx == 0:
      df_green = temp_df
    else:
      df_green = df_green.unionAll(temp_df)

  df_yellow = None
  for idx, input_path in enumerate(fs.glob(input_yellow)):
    temp_df = spark.read.parquet(f"gs://{input_path}")
    temp_df = temp_df \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
    temp_df = fix_schema(input_path, temp_df)

    if idx == 0:
      df_yellow = temp_df
    else:
      df_yellow = df_yellow.unionAll(temp_df)

  df_trips_data = df_green.unionAll(df_yellow)
  df_trips_data.createOrReplaceTempView('trips_data')

  df_result = spark.sql("""
  SELECT 
      -- Reveneue grouping 
      PULocationID AS revenue_zone,
      date_trunc('month', pickup_datetime) AS revenue_month, 
      service_type, 

      -- Revenue calculation 
      SUM(fare_amount) AS revenue_monthly_fare,
      SUM(extra) AS revenue_monthly_extra,
      SUM(mta_tax) AS revenue_monthly_mta_tax,
      SUM(tip_amount) AS revenue_monthly_tip_amount,
      SUM(tolls_amount) AS revenue_monthly_tolls_amount,
      SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
      SUM(total_amount) AS revenue_monthly_total_amount,
      SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

      -- Additional calculations
      AVG(passenger_count) AS avg_montly_passenger_count,
      AVG(trip_distance) AS avg_montly_trip_distance
  FROM
      trips_data
  GROUP BY
      1, 2, 3
  """)

  df_result.write.format('bigquery') \
      .option('table', output) \
      .mode(write_mode) \
      .save()

if __name__=="__main__":
  parser = argparse.ArgumentParser()

  parser.add_argument('--input_green', required=True)
  parser.add_argument('--input_yellow', required=True)
  parser.add_argument('--output', required=True)
  parser.add_argument('--temp_bucket', required=True)
  parser.add_argument('--write_mode', default="append")

  args = parser.parse_args()

  main(args)
      


