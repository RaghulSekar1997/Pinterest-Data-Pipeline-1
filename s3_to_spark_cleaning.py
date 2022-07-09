

import findspark
import pyspark
import boto3
import json 
from json import load 
import findspark
import pyspark
import multiprocessing
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window  
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType

#TODO: Clean up the variable names for the dataframes and make some comments on the code

class SparkCleaning:
    #TODO: debug this code 
    def __init__(self):
        self.findspark = findspark
        
        
    
       
        findspark.init()

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

    def set_s3_bucket_resource(self, bucket_name):
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket(bucket_name)


# Reading in the first row of the dataframe 

    def create_dataframe(self, bucket_key):
        object = self.bucket.Object(key=bucket_key)
        get_body = load(object.get()['Body'])
        self.df = self.spark.createDataFrame([list(get_body.values())], list(get_body.keys()))




    def create_and_read_data_into_dataframe(self, number_of_records, bucket_key):
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





# # Things to clean: 

# Drop the downloaded column (or change it to True/False)

# Drop the save location column 

# Maybe do something about the follower count?

# Set the index as the index column 



    def clean_spark_dataframe_rows(self):
        cleaned_rows = self.df.drop('downloaded', 'save_location', 'index')
        clean_id  = cleaned_rows.withColumn(
            'id', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1
            )
        clean_follower_count_k = clean_id.withColumn('follower_count', regexp_replace('follower_count','k','000'))
        clean_follower_count_M = clean_follower_count_k.withColumn('follower_count', regexp_replace('follower_count','M','000000'))
        clean_follower_count_null = clean_follower_count_M.withColumn('follower_count', regexp_replace('follower_count','User Info Error','null'))
        clean_description_field = clean_follower_count_null.withColumn('description', regexp_replace('description','No description available Story format','null'))
        clean_title_field = clean_description_field.withColumn('title', regexp_replace('title', 'No Title Data Available', 'null'))
        clean_tag_list  = clean_title_field.withColumn('tag_list', regexp_replace('tag_list', 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e','null'))


        # s3_df_clean = s3_df_clean.withColumn('follower_count', col('follower_count').cast('int'))

        cleaned_rows_final = clean_tag_list.withColumn('follower_count', col('follower_count').cast('integer'))
        cleaned_rows_final.show()
        return cleaned_rows_final



    def add_monotonically_increasing_id(self):
        df_id = self.cleaned_rows_df.withColumn(
            'id', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1
            )
        return df_id 



# Cleaning the follower count column 

# Steps 

# 1. Replace 'User Info Error' with null 

# 2. convert 'k' and 'M' into '000' and '000000'

# 3. Convert the column into an integer




# Replacing each string in the column conditionally. 
    def clean_follower_count(self,column_name):
        df_clean_follower_count = self.df.withColumn(
            column_name,
            when(self.df.endswith('k'),regexp_replace(self.df,'k','000')) \
            .when(self.df.endswith('M'),regexp_replace(self.df,'M','000000')) \
            .when(self.df.endswith('User Info Error'),regexp_replace(self.df,'User Info Error','null'))
            .otherwise(self.df))

        return df_clean_follower_count

        


# convert the 'follower_count' column in the dataframe to an integer 

    def convert_to_integer(self, column_name):
        df_clean_follower_count = self.df.withColumn(column_name, self.df.cast('integer'))
        df_clean_follower_count.show()
        return df_clean_follower_count



    def clean_spark_dataframe(self):
        cleaned_df = self.clean_spark_dataframe_rows()\
            .add_monotonically_increasing_id()\
            .clean_follower_count("follower_count")\
            .convert_to_integer("follower_count")

        return cleaned_df 
	
    
if __name__ == "__main__":
    new_dataframe = SparkCleaning()
    new_dataframe.start_spark_session()
    new_dataframe.set_s3_bucket_resource('wr95-pintrest-data-bucket')
    new_dataframe.create_and_read_data_into_dataframe(20, 'json-data_0.json')
    new_dataframe.clean_spark_dataframe_rows()
    # new_dataframe.add_monotonically_increasing_id()
    # new_dataframe.clean_follower_count('follower_count')
    # new_dataframe.convert_to_integer('follower_count')
    # new_dataframe.clean_spark_dataframe()
		



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