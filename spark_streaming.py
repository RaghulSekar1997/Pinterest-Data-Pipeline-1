import findspark
import os
from json import loads 
from kafka import KafkaConsumer
from pyspark.sql import SparkSession

findspark.init() 

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
spark_home = os.environ.get('SPARK_HOME', None)
os.environ['JAVA_HOME']
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.10 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = 'Pintrest-Batch-Process'
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'


spark = SparkSession \
        .builder \
        .appName("KafkaStreaming") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

consumer = KafkaConsumer(
     kafka_topic_name,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))
     
# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")

# outputting the messages to the console 
stream_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination(180)