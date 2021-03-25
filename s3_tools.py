import boto3
from botocore.exceptions import NoCredentialsError
from botocore.config import Config
from boto3.s3.transfer import S3Transfer
import os
             
def check_bucket_exist(bucket_name):
    s3 = boto3.resource('s3')
    return s3.Bucket(bucket_name) in s3.buckets.all()

def get_file_url(client, file_name, bucket_name):
    return 'File S3: %s/%s/%s' % (client.meta.endpoint_url, bucket_name, file_name)

def get_last_csv_link(bucket_name, folder):
    s3 = boto3.client('s3')
    if check_bucket_exist(bucket_name):
        print ("OK")
        print ("Looking for the last file uploaded...", end="")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        if 'Contents' in response:
            all = response['Contents']        
            latest = max(all, key=lambda x: x['LastModified'])
            print ("OK")
            if 'Key' in  latest:
                return get_file_url(s3, latest['Key'], bucket_name)
            else:
                return "File not founded"
        else:
            return "Folder not founded"              
    else:
        return "Bucket noty founded"