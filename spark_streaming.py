import findspark
import os
import pyspark.sql.functions as PysparkSQLFunctions 
from pyspark.sql import SparkSession
from pyspark.sql.types  import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col

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

     
# Create a dataframe which reads from the kafka_topic_name 
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")

# Set the datatypes of the json messages 

jsonSchema = StructType([StructField("index", IntegerType()),
                        StructField("unique_id", StringType()),
                        StructField("title", StringType()),
                        StructField("description", StringType()),
                        StructField("follower_count", StringType()),
                        StructField("tag_list", StringType()),
                        StructField("poster_name", StringType()),
                        StructField("is_image_or_video", StringType()),
                        StructField("image_src", StringType()),
                        StructField("downloaded", IntegerType()),
                        StructField("save_location", StringType()),
                        StructField("category", StringType()),
                       ])

# Create a stream_df using the schema highlighted above
 
stream_df = stream_df.withColumn("value", PysparkSQLFunctions.from_json(stream_df["value"], jsonSchema)).select(col("value.*"))

# Output messages to the terminal (for debugging purposes)
stream_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination(180)