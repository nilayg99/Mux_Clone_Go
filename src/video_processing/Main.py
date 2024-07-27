import boto3
import sys
import os
import json
from dotenv import load_dotenv

load_dotenv()
Raw_videos_destination_path = os.getenv('Raw_videos_destination_path')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
def download_video (bucket_name,s3_object_key):
    s3_client = boto3.resource('s3',
                         aws_access_key_id= AWS_ACCESS_KEY_ID,
                         aws_secret_access_key= AWS_SECRET_ACCESS_KEY,
                         region_name='us-east-1')
    if "/" in s3_object_key :

        object_name = s3_object_key.split("/")[-1]
        Raw_videos_destination_full_path = (os.path.join(Raw_videos_destination_path, object_name))
        s3_client.Object(bucket_name, s3_object_key).download_file(Raw_videos_destination_full_path)
    else :
        Raw_videos_destination_full_path = (os.path.join(Raw_videos_destination_path, s3_object_key))
        s3_client.Object(bucket_name, s3_object_key).download_file(Raw_videos_destination_full_path)

def process_message(message):
    data = json.loads(message)
    # Access the desired value
    bucket_name = data['Records'][0]['s3']['bucket']['name']
    s3_object_key = data['Records'][0]['s3']['object']['key']
    download_video(bucket_name,s3_object_key)

    # Your processing logic here

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python Main.py <message>")
        sys.exit(1)
    message = sys.argv[1]
    for mess in message :
        process_message(message)
        break


