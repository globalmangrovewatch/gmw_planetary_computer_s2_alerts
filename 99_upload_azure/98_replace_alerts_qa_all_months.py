from azure.storage.blob import BlobClient
import glob
import os
import json
import tqdm
os.environ['USE_PYGEOS'] = '0'
import geopandas
import numpy

def zero_pad_num_str(
    num_val: float,
    str_len: int = 3,
    round_num: bool = False,
    round_n_digts: int = 0,
    integerise: bool = False,
    absolute: bool = False,
    gain: float = 1,
) -> str:
    if absolute:
        num_val = abs(num_val)
    if round_num:
        num_val = round(num_val, round_n_digts)
    if integerise:
        num_val = int(num_val * gain)

    num_str = "{}".format(num_val)
    num_str = num_str.zfill(str_len)
    return num_str

sas_info_file = "azure_info.json"
overwrite_azure = True

with open(sas_info_file) as f:
    sas_token_info = json.load(f)

qa_vec_file = "/Users/pete/Dropbox/University/Research/Projects/GlobalMangroveWatch/GMW_Alerts_Planetary_Computer/202303_qa_edits/gmw_alerts_qa_201901_202301.geojson"
qa_vec_gdf = geopandas.read_file(qa_vec_file)

years = [2019, 2020, 2021, 2022, 2023]
months = {2019:numpy.arange(1,13, 1), 2020:numpy.arange(1,13, 1), 2021:numpy.arange(1,13, 1), 2022:numpy.arange(1,13, 1), 2023:[1]}

out_tmp_dir = "lcl_qa_edits"
if not os.path.exists(out_tmp_dir):
    os.mkdir(out_tmp_dir)

for year in tqdm.tqdm(years, leave=True):
    for month in tqdm.tqdm(months[year], leave=False):
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
        #print("\tFile Uploaded")



qa_vec_gdf = None