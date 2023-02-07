from azure.storage.blob import BlobClient
import glob
import os
import json
import tqdm

sas_info_file = "azure_info.json"

with open(sas_info_file) as f:
    sas_token_info = json.load(f)

qa_files = ["/path/to/gmw_alerts_qa_202210.geojson"]

for qa_file in tqdm.tqdm(qa_files):
    basename = os.path.basename(qa_file)
    file_url = os.path.join(sas_token_info["url"], "monthly_qa_edit_vecs", basename)
    file_url_signed = f"{file_url}?{sas_token_info['sas_token']}"
    #print(file_url_signed)
    client = BlobClient.from_blob_url(file_url_signed)
    with open(qa_file, 'rb') as data:
        client.upload_blob(data)
    #print("\tFile Uploaded")
