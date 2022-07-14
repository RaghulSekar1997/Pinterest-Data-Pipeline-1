

import findspark
import pyspark
import boto3
import json 
from json import load 
import findspark
import pyspark
import multiprocessing
import os 
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window  
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType

#TODO: Clean up the variable names for the dataframes and make some comments on the code
os.environ['PYSPARK_SUBMIT_ARGS']='--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'

'''
DataCleaning Strategy: 


# Drop the downloaded column (or change it to True/False)

# Drop the save location column 

# Remove and replace the suffixes from the follower_count column 


# Cleaning the follower count column 

# Steps 

# 1. Replace 'User Info Error' with null 

# 2. convert 'k' and 'M' into '000' and '000000'

# 3. Convert the column into an integer

# convert the 'follower_count' column in the dataframe to an integer 

For other columns, change instances of strings saying 'no data' to null 

Drop the index column 
'''
class SparkCleaning:
    '''
    A class to read and clean a Spark dataframe. 
    The cleaned dataframe is then sent to a cassandra database 

    Methods: 
    start_spark_session

    set_s3_bucket_resource

    create_and_read_data_into_dataframe

    create_dataframe

    send_to_cassandra 

    Attributes 

    self.findspark : obj
    Instantiates the findspark object 

    self.cfg : obj 
    Sets the configuration of the spark environment 


    '''
    def __init__(self):
        self.findspark = findspark
        
        
    
       
        findspark.init(os.environ["SPARK_HOME"])

        self.cfg = (
   		 pyspark.SparkConf()
    		# Setting where master node is located [cores for multiprocessing]
    		.setMaster(f"local[{multiprocessing.cpu_count()}]")
    		# Setting application name
    		.setAppName("TestApp")
    		# Setting config value via string
    		.set("spark.eventLog.enabled", False)
    		# Setting environment variables for executors to use
    		.setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    		# Setting memory if this setting was not set previously
    		.setIfMissing("spark.executor.memory", "1g")
		)


    def start_spark_session(self):
        # Starting the SparkSession
        self.sc = SparkContext(conf = self.cfg)
        self.spark = SparkSession(self.sc)
		




# Import the data from the S3 Bucket 
# Store it in an object 

    def set_s3_bucket_resource(self, bucket_name : str):
        '''
        Method to set the s3_bucket_resource 

        Parameters
        bucket_name: The name of your s3 bucket. 
        (Please configure the other settings via awscli) 

        '''
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket(bucket_name)


# Reading in the first row of the dataframe 

    def create_dataframe(self, bucket_key : str):
        '''
        Method to create the spark_dataframe 

        Parameters: 
        bucket_key : the name of a file inside your s3 bucket 
        '''
        object = self.bucket.Object(key=bucket_key)
        get_body = load(object.get()['Body'])
        self.df = self.spark.createDataFrame([list(get_body.values())], list(get_body.keys()))




    def create_and_read_data_into_dataframe(self, number_of_records : int , bucket_key : str):
        '''
        Method to create a spark dataframe, and append any other dataframes to it

        Parameters: 
        number_of_records : int 
        The number of records the user wants to add 

        bucket_key : str
        The name of a file inside your s3 bucket 

        returns: 
        self.df : DataFrame 
        A dataframe with length equal to the number of records requested. 
        '''
        object = self.bucket.Object(key=bucket_key)
        get_body = load(object.get()['Body'])
        self.df = self.spark.createDataFrame([list(get_body.values())], list(get_body.keys()))
        # append to dataframe outside of loop so that it updates accordingly. 
        for i in range(0,number_of_records):
            object = self.bucket.Object(key=f'json-data_{i}.json')
            rest_of_data = load(object.get()['Body'])
            df_read = self.spark.createDataFrame([list(rest_of_data.values())], list(rest_of_data.keys()))
            self.df = self.df.union(df_read)
            print(self.df.count())
            self.df.show()
        return self.df




    def clean_spark_dataframe_columns(self):

        '''
        Method to clean the columns of the spark dataframe 

        Returns: 
        cleaned_rows_final_reordered : DataFrame
        A cleaned spark dataframe via the following transformations 

        '''
        # Drop the 'downloaded', 'save_location' and 'index columns' 
        cleaned_rows = self.df.drop('downloaded', 'save_location', 'index')
        # Add a new column called id
        clean_id  = cleaned_rows.withColumn(
            'id', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1
            )
        # Remove and replace the suffixes from the follower count and convert them to their numerical values 
        clean_follower_count_k = clean_id.withColumn('follower_count', regexp_replace('follower_count','k','000'))
        clean_follower_count_M = clean_follower_count_k.withColumn('follower_count', regexp_replace('follower_count','M','000000'))
        # Remove and replace strings with nulls using regexp 
        clean_follower_count_null = clean_follower_count_M.withColumn('follower_count', regexp_replace('follower_count','User Info Error','null'))
        clean_description_field = clean_follower_count_null.withColumn('description', regexp_replace('description','No description available Story format','null'))
        clean_title_field = clean_description_field.withColumn('title', regexp_replace('title', 'No Title Data Available', 'null'))
        clean_tag_list  = clean_title_field.withColumn('tag_list', regexp_replace('tag_list', 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e','null'))
        clean_image_src = clean_tag_list.withColumn('image_src', regexp_replace('image_src', 'Image src error','null' ))


        # Cast the follower_count column to an integer 
        cleaned_rows_final = clean_image_src.withColumn('follower_count', col('follower_count').cast('integer'))
        # Re-arrange the order of the columns 
        cleaned_rows_final_reordered = cleaned_rows_final.select(
            'id',
            'unique_id',
            'category',
            'title',
            'follower_count',
            'tag_list',
            'is_image_or_video',
            'image_src',
            'description'
        )
        cleaned_rows_final_reordered.show() 
        return cleaned_rows_final_reordered


	
    def send_to_cassandra(self, cleaned_df):
        '''
        Method to send the cleaned dataframe to a Cassandra Database 

        Parameters: 
        cleaned_df : DataFrame
        A cleaned dataframe from the previous method: 
        clean_spark_dataframe_columns()

        '''
        # Process: 
        # Write the data to cassandra in the set format 
        # If the table exists already, overwrite it. 
        # Set the host and connection port of the database 
        # Place the data inside the correct keyspace in the Cassandra database 
        # Lastly set that the data will be a table, by the name 'pinterest_data'
        # save the process 
        cleaned_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .option("confirm.truncate", "true") \
        .option("spark.cassandra.connection.host", "127.0.0.1") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace" , "pinterest") \
        .option("table", "pinterest_data") \
        .save()

if __name__ == "__main__":
    new_dataframe = SparkCleaning()
    new_dataframe.start_spark_session()
    new_dataframe.set_s3_bucket_resource('wr95-pintrest-data-bucket')
    new_dataframe.create_and_read_data_into_dataframe(20, 'json-data_0.json')
    cleaned_df = new_dataframe.clean_spark_dataframe_columns()
    new_dataframe.send_to_cassandra(cleaned_df)
   
		



 # %%
# from pyspark.sql import SQLContext 

# # ''.join(string.split()) in order to write a multi-line JSON string here.

# data_source_format = 'org.apache.spark.sql.execution.datasources.hbase'
# catalog = ''.join("""{
#     "table":{"namespace":"default", "name":"testtable"},
#     "rowkey":"key",
#     "columns":{
#         "category":{"cf":"rowkey", "col":"category", "type":"string"},
#         "description":{"cf":"cf", "col":"description", "type":"string"},
#         "follower_count":{"cf":"cf", "col":"follower_count", "type":"integer"},
        
        
#     }
# }""".split())