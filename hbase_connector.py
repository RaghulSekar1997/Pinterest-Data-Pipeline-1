from s3_to_spark_cleaning import SparkCleaning 
from pyspark import SparkConf
from pyspark.sql import SparkSession

# from loghandler import logger 

# NOTE: Start hbase daemons first



        

def write_to_hbase():
        
        
        new_dataframe = SparkCleaning()
        new_dataframe.start_spark_session()
        new_dataframe.set_s3_bucket_resource('wr95-pintrest-data-bucket')
        new_dataframe.create_and_read_data_into_dataframe(20, 'json-data_0.json')
        new_dataframe.clean_spark_dataframe_rows()
        
        conf = (SparkConf().setAppName("RW_from_Hbase"))

        spark = SparkSession.builder.appName("  ") \
                .config(conf=conf) \
                .getOrCreate()

        
        catalog = ("""{
        "table":{"namespace":"default", "name":"Test_Table"},
        "rowkey":"key",
        "columns":{
        "category":{"cf":"rowkey", "col":"category", "type":"string"},
        "unique_id":{"cf":"rowkey", "col":"unique_id", "type":"integer"}
        "title":{"cf":"rowkey", "col":"title", "type":"string"},
        "description":{"cf":"rowkey", "col":"description", "type":"string"},
        "follower_count":{"cf":"rowkey", "col":"follower_count", "type":"integer"},
        "tag_list":{"cf":"rowkey", "col":"tag_list", "type":"string"},
        "is_image_or_video":{"cf":"rowkey", "col":"is_image_or_video", "type":"string"},
        "image_src":{"cf":"rowkey", "col":"image_src", "type":"string"}""")
        
        new_dataframe = df 

        df = spark.write.format("org.apache.hadoop.hbase.spark")\
        .option("hbase.columns.mapping", catalog) \
        .option("hbase.spark.pushdown.columfilter", True) \
        .option("hbase.table", "Test_Table") \
        .option("hbase.spark.use.hbasecontext", False).load()
        
        new_dataframe.show()
       



if __name__ == '__main__':
   write_to_hbase()