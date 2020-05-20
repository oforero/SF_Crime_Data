import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True),
])

def run_spark_job(spark):

    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers','localhost:9092') \
        .option('group.id', "call_aggregator_01") \
        .option('subscribe', 'police.service.calls') \
        .option('startingOffsets', 'earliest') \
        .option('maxOffsetsPerTrigger', 200) \
        .option("maxRatePerPartition", 100) \
        .load()

    # Show schema for the incoming resources for checks
    print("df Schema ...")
    df.printSchema()

    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")
    print("kafka_df Schema ...")
    kafka_df.printSchema()

    # Load the JSON in to a Datframe format
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    print("service_table Schema ...")
    service_table.printSchema()

    # select original_crime_type_name and disposition
    distinct_table = service_table \
        .select('original_crime_type_name', 'disposition', 'call_date_time') \
        .distinct() 
    print("distinct_table Schema ...")
    distinct_table.printSchema()

    # count the number of original crime type
    agg_df = distinct_table \
        .select('original_crime_type_name') \
        .groupby('original_crime_type_name') \
        .agg({'original_crime_type_name': 'count'}) \
        .orderBy('count(original_crime_type_name)', ascending=True)
    print("agg_df Schema ...")
    agg_df.printSchema()

    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    #     write output stream
    query = agg_df \
        .writeStream \
        .queryName("aggregate_calls") \
        .format('console') \
        .option("truncate", "false") \
        .outputMode('Complete') \
        .start()

    # attach a ProgressReporter
    query.awaitTermination()

    # Get the radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .queryName("join_radio_code") \
        .format("console") \
        .outputMode('Complete') \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("spark://localhost:7077") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
