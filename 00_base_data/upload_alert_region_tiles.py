from azure.storage.blob import BlobClient
import os
import json

sas_info_file = "/home/jovyan/azure_info.json"

with open(sas_info_file) as f:
    sas_token_info = json.load(f)

vec_file = "alert_region_tiles.geojson"

file_url = os.path.join(sas_token_info["url"], vec_file)
file_url_signed = f"{file_url}?{sas_token_info['sas_token']}"

client = BlobClient.from_blob_url(file_url_signed)
with open(vec_file, 'rb') as data:
    client.upload_blob(data)

