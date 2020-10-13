import json
import os
import boto3
from pprint import pprint

def lambda_handler(event, context):
    """
    A small lambda function for taking the contents of a folder in a connected
    bucket, uploading those contents to an EFS mounted folder, and deleting the 
    file from the s3 bucket upload folder.
    """
    
    # print()
    # print('Incoming event:')
    # pprint(event)
    
    # SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
    # SOURCE_BUCKET_SUBFOLDER = os.environ['SOURCE_BUCKET_SUBFOLDER']
    EFS_PACKAGES_DIR = os.environ['EFS_PACKAGES_DIR']
    
    key = event['Records'][0]['s3']['object']['key']
    keyfolder = os.path.dirname(key)
    filename = os.path.basename(key)
    bucketname = event['Records'][0]['s3']['bucket']['name']
    
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname)

    # create /mnt/efs/packages if missing
    if not os.path.exists(EFS_PACKAGES_DIR):
        os.makedirs(EFS_PACKAGES_DIR)
    
    # make target be /mnt/efs/packages/file.ext
    target = os.path.join(EFS_PACKAGES_DIR, filename)
    
    bucket.download_file(key, target)
    print(f'Downloaded {filename} from {bucketname}/{keyfolder} to {EFS_PACKAGES_DIR}')
    
    
       
    print()  
    print(f"CONTENTS OF {EFS_PACKAGES_DIR}: ")
    print("--------------------------------------------")
    os.chdir(EFS_PACKAGES_DIR)       
    # print(f'Files in {os.getcwd()}:')
    print(os.listdir())
    print()
  
    return {
        'statusCode': 200,
        'body': json.dumps('Job complete!')
    }

