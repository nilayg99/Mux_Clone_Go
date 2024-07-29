import boto3
import sys
import os
import json
from dotenv import load_dotenv
import ffmpeg
import time

load_dotenv()
Raw_videos_destination_path = os.getenv('Raw_videos_destination_path')
processed_videos_destination_path = os.getenv('processed_videos_destination_path')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


# Video resolution change
def change_video_resolution(input_path, output_path, resolution = "1280x720"):
        
        print ("Inside change_video_resolution()")
        
        try:
            # Split the resolution string into width and height
            width, height = resolution.split('x')
            
            # Use ffmpeg to change the video resolution
            (
                ffmpeg
                .input(input_path)
                .output(output_path, vf=f'scale={width}:{height}')
                .run(overwrite_output=True)
            )
            print(f'Successfully converted {input_path} to {output_path} with resolution {resolution}')
        except Exception as e:
            print(f'Error converting video: {e}')



def process_videos_in_folder():
    print ("Inside process_videos_in_folder()")
    
    # Iterate through all files in the input folder
    for filename in os.listdir(Raw_videos_destination_path):
        input_path = os.path.join(Raw_videos_destination_path, filename).replace('\\', '/')
        
        # Skip directories and non-video files
        if not os.path.isfile(input_path) or not filename.lower().endswith(('.mp4', '.mkv', '.avi')):
            continue
        
        # Define the output path and change the resolution
        name, ext = os.path.splitext(filename)
        output_path = os.path.join(processed_videos_destination_path, f'{name}_720p{ext}').replace('\\', '/')
        #print(f"Processing {input_path} to {output_path}")
        change_video_resolution(input_path, output_path)

# Download video from S3
def download_video (bucket_name,s3_object_key):
    s3_client = boto3.resource('s3',
                         aws_access_key_id= AWS_ACCESS_KEY_ID,
                         aws_secret_access_key= AWS_SECRET_ACCESS_KEY,
                         region_name='us-east-1')
    if "/" in s3_object_key :

        object_name = s3_object_key.split("/")[-1]
        Raw_videos_destination_full_path = (os.path.join(Raw_videos_destination_path, object_name))
        s3_client.Object(bucket_name, s3_object_key).download_file(Raw_videos_destination_full_path)
        #time.sleep(60) # wait for 60 sec
        print ("120 sec Completed, pogressing")
        process_videos_in_folder()
    else :
        object_name = s3_object_key
        Raw_videos_destination_full_path = (os.path.join(Raw_videos_destination_path, object_name))
        s3_client.Object(bucket_name, object_name).download_file(Raw_videos_destination_full_path)
        #time.sleep(60) # wait for 60 sec
        print ("120 sec Completed, pogressing")
        process_videos_in_folder()

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
    process_message(message)


