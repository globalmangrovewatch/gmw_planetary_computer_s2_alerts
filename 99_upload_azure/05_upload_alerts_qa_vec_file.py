from azure.storage.blob import BlobClient
import glob
import os
import json
import tqdm
import geopandas

sas_info_file = "azure_info.json"

with open(sas_info_file) as f:
    sas_token_info = json.load(f)

qa_vec_file = "gmw_alerts_qa_201901_202301.geojson"
qa_vec_gdf = geopandas.read_file(qa_vec_file)

month = 2
year = 2023

c_month_str = zero_pad_num_str(month, str_len=2)
alerts_qa_vec_lyr = f"gmw_alerts_qa_{year}{c_month_str}"
alerts_qa_vec_file = f"{alerts_qa_vec_lyr}.geojson"
alerts_qa_vec_file_path = os.path.join(out_tmp_dir, alerts_qa_vec_file)
qa_vec_gdf.to_file(alerts_qa_vec_file_path, driver="GeoJSON")

file_url = os.path.join(sas_token_info["url"], "monthly_qa_edit_vecs", alerts_qa_vec_file)
file_url_signed = f"{file_url}?{sas_token_info['sas_token']}"
#print(file_url_signed)
client = BlobClient.from_blob_url(file_url_signed)
with open(alerts_qa_vec_file_path, 'rb') as data:
    client.upload_blob(data, overwrite=overwrite_azure)
print("\tFile Uploaded")
