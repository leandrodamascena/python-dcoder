import os
from os import listdir
import tempfile
import gzip
import base64
import json
import pandas as pd 
import datetime as dt
from google.cloud import storage

def split_project(data):

    '''
    This function splits the series to forecast into different files, saving them into
    a bucket and providing a JSON file with the paths to each of these files
    Arguments:
        - data: decoded pub/sub message
    Returns:
        - Nothing
    '''

    file_data = data
    storage_client = storage.Client()
    
    file_name = file_data["name"]
    bucket_name = file_data["bucket"]

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob_uri = f"gs://{bucket_name}/{file_name}"



    _, temp_local_filename = tempfile.mkstemp()

    # Download file from bucket.
    blob.download_to_filename(temp_local_filename)
    print(f"File {file_name} was downloaded to {temp_local_filename}.")


    # opening and decoding the .gz file received by the API/frontend



    f=gzip.open(temp_local_filename,'rb')
    file_content=f.read()
    decoded_file = base64.b64decode(file_content)
    obj = json.loads(gzip.decompress(decoded_file))


    # extracting project properties
    project_id = obj['project_id']
    user_email = obj['user_email']
    model_spec = obj['model_spec']
    start_time = [dt.datetime.now().strftime("%Y%m%d_%H%M%S")]


    data_df = [pd.DataFrame(obj['data_list'][x]) for x in obj['data_list'].keys()]
    data_list_colnames = [list(x.columns) for x in data_df]


    json_paths = list()

    # saving gzip files and adding their paths to the map list
    for i in range(0, len(obj['data_list'])):
        y = list(obj['data_list'].keys())[i]
        process = [
            list(obj['data_list'].values())[i],
            y,
            model_spec,
            user_email,
            [i+1],
            project_id,
            data_list_colnames[i],
            start_time
        ]
	
	# saving files and sending them to the output bucket
        output_path = f'/tmp/{project_id[0]}-{y}.json.gz'
        output = gzip.open(output_path, 'wt')
        output.write(json.dumps(process))
        output.close()
        # previous bucket name: OUTPUT_BUCKET_{project_id[0]}
        output_bucket_name = os.getenv(f"OUTPUT_BUCKET")
        output_bucket = storage_client.bucket(output_bucket_name)
        new_blob = output_bucket.blob(f'{project_id[0]}-{y}.json.gz')
        new_blob.upload_from_filename(output_path)
        print(f"Output file uploaded to: gs://{output_bucket_name}/{project_id[0]}-{y}.json.gz")
        json_paths.append(f"gs://{output_bucket_name}/{project_id[0]}-{y}.json.gz")

 #   with open(f'./outputs/{project_id[0]}-map.json', 'w') as mapfile:
 #       mapfile.write(json.dumps(json_paths))
