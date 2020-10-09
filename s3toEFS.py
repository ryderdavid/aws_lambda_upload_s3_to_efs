import json
import os
import boto3


def lambda_handler(event, context):
    """
    A small lambda function for taking the contents of a folder in a connected
    bucket, uploading those contents to an EFS mounted folder, and deleting the 
    file from the s3 bucket upload folder.
    """
    
    SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
    SOURCE_BUCKET_SUBFOLDER = os.environ['SOURCE_BUCKET_SUBFOLDER']
    DOWNLOAD_PATH = os.environ['DOWNLOAD_PATH']
    
    s3 = boto3.resource('s3')

    bucket = s3.Bucket(SOURCE_BUCKET)
    
    def download_s3_folder(bucket_name, s3_folder, local_dir=None):
        """
        Download the contents of a folder directory
        Args:
            bucket_name: the name of the s3 bucket
            s3_folder: the folder path in the s3 bucket
            local_dir: a relative or absolute directory path in the local file system
        """
        bucket = s3.Bucket(bucket_name)
        
        if not s3_folder.endswith('/'):
            s3_folder = s3_folder + '/'
        
        for obj in bucket.objects.filter(Prefix = s3_folder):
            target = obj.key if local_dir is None \
                else os.path.join(local_dir, os.path.basename(obj.key))
            if not os.path.exists(os.path.dirname(target)):
                os.makedirs(os.path.dirname(target))
            if target.endswith("/"):
                continue
            
            bucket.download_file(obj.key, target)
            print(f"Downloaded {obj.key} to {target}." )
            
            print(f"Deleting {obj.key} from bucket.")
            obj.delete()
    
    # delete all existing files in dir
    # print()
    # print("Deleting all files present in destination folder")
    # os.chdir(DOWNLOAD_PATH)
    # [os.remove(os.path.join(os.getcwd(), f)) for f in os.listdir()]
    
    print()    
    download_s3_folder(bucket_name = SOURCE_BUCKET, 
                      s3_folder = SOURCE_BUCKET_SUBFOLDER,
                      local_dir = DOWNLOAD_PATH)

       
    print()         
    os.chdir(DOWNLOAD_PATH)       
    print(f'Files in {os.getcwd()}:')
    print(os.listdir())
    print()
  
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

