import pandas as pd
import boto3
import os
# AWS credentials (replace with your own)
AWS_ACCESS_KEY_ID = 'AKIA4DYTX5DQH5EG32VZ'
AWS_SECRET_ACCESS_KEY ='8rBiYfa99TNvIZN0ERuXnvBaP74wJMwJlAEen/sl'

# Set up S3 client
s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# Set up file and bucket information
bucket_name = 'reddit--data'
folder_path = '/home/ubuntu/Downloads'

# Loop through files in folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        file_key = filename

        # Upload file to S3
        s3.upload_file(file_path, bucket_name, file_key)

        print(f"Uploaded {file_path} to S3 bucket {bucket_name} as {file_key}")