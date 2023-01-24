from azure.storage.blob import BlobClient
import glob
import os
import json
import tqdm

sas_info_file = "/home/jovyan/azure_info.json"

with open(sas_info_file) as f:
    sas_token_info = json.load(f)

gmw_tif_tiles = glob.glob("/home/jovyan/PlanetaryComputerExamples/gmw_alerts_plantary_computer_tests/gmw_2018_revised_ext_opt_s1/*.tif")

for img_file in tqdm.tqdm(gmw_tif_tiles):
    basename = os.path.basename(img_file)
    file_url = os.path.join(sas_token_info["url"], "gmw_2018_revised", basename)
    file_url_signed = f"{file_url}?{sas_token_info['sas_token']}"
    #print(file_url_signed)
    client = BlobClient.from_blob_url(file_url_signed)
    with open(img_file, 'rb') as data:
        client.upload_blob(data)
    #print("\tFile Uploaded")
