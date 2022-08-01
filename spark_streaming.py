import findspark
import os
import pyspark.sql.functions as PysparkSQLFunctions 
from pyspark.sql import SparkSession
from pyspark.sql.types  import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col, regexp_replace

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

# EXTRACT: Create a stream_df using the schema highlighted above
 
stream_df = stream_df.withColumn("value", PysparkSQLFunctions.from_json(stream_df["value"], jsonSchema)).select(col("value.*"))


# TRANSFORM: Clean the dataframe

'''
Data Cleaning Strategy 

1. Clean the follower count by stripping the prefixes  

2. Drop Save Location and Downloaded columns 


'''
# Drop the "downloaded" and "save_location" columns 

stream_df = stream_df.drop("downloaded", "save_location")

# Remove and replace the suffixes from the follower_count and replace them with their numerical value
# e.g. 15k is 15000 where k = 1000 and 6M will be 6000000 as M = 1000000
stream_df = stream_df.withColumn('follower_count', regexp_replace('follower_count','k','000'))
stream_df = stream_df.withColumn('follower_count', regexp_replace('follower_count','M','000000'))

# Remove and replace 'no value style' strings with nulls 

stream_df = stream_df.withColumn('follower_count', regexp_replace('follower_count','User Info Error','null'))
stream_df = stream_df.withColumn('description', regexp_replace('description','No description available Story format','null'))
stream_df = stream_df.withColumn('title', regexp_replace('title', 'No Title Data Available', 'null'))
stream_df  = stream_df.withColumn('tag_list', regexp_replace('tag_list', 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e','null'))
stream_df = stream_df.withColumn('image_src', regexp_replace('image_src', 'Image src error','null' ))


# Cast the follower_count column to an integer 
stream_df = stream_df.withColumn('follower_count', col('follower_count').cast('integer'))
# Re-arrange the order of the columns 
stream_df = stream_df.select(
            'index',
            'unique_id',
            'category',
            'title',
            'follower_count',
            'tag_list',
            'is_image_or_video',
            'image_src',
            'description'
        )

# Output messages to the terminal (for debugging purposes)
stream_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination(180)