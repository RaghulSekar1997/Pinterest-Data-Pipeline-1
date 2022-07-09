from kafka import KafkaConsumer
from json import loads
import json 
import boto3 
import tempfile
import time 
import shutil
import os 

consumer = KafkaConsumer(
    'Pintrest-Batch-Process',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))



# INFO: Consumer produces messages 
s3_client =  boto3.client('s3')

# Steps:

# Create an S3 Bucket on AWS

# Give access crenditals to your S3 Bucket on AWS 

# Set up AWS Crenditals in Terminal using awscli. 
# If this package is not present run: 
# $ pip install awscli 
# In the terminal

# Iterate through the messages



def upload_to_s3(bucket_name:str):
    for i,message in enumerate(consumer):
        message = message.value
        print(message) 
        file_name = 'json-data'
        

        with tempfile.TemporaryDirectory() as temp_dir:
            with open(f'{temp_dir}/{file_name}', mode='a+', encoding='utf-8-sig') as f:
                json.dump(message, f, indent=4, ensure_ascii=False) 
                f.write('\n') 
                f.flush()
                time.sleep(3)
                s3_client.upload_file(f'{temp_dir}/{file_name}', bucket_name,f'{file_name}_{i}.json')
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
   
if __name__ == "__main__":
    upload_to_s3('wr95-pintrest-data-bucket')