import tqdm
import os
import json
os.environ['USE_PYGEOS'] = '0'
import geopandas
from azure.storage.blob import BlobClient

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


sas_info_file = "/Users/pete/azure_info_rm.json"
with open(sas_info_file) as f:
    sas_token_info = json.load(f)

# Define the tiles to be processed.
tiles_gdf = geopandas.read_file("../00_base_data/alert_region_tiles.geojson")
tiles = tiles_gdf["tile"].values

month = 1
year = 2023


month_str = zero_pad_num_str(month, str_len = 2, round_num = False, round_n_digts = 0)
date_str = f"{year}{month_str}"

azure_img_dir = "monthly_change_imgs"

for tile in tqdm.tqdm(tiles):

    alerts_img_file = f"gmw_{tile}_{date_str}_chg_alerts.tif"
    alerts_img_file_url = os.path.join(sas_token_info["url"], azure_img_dir, alerts_img_file)
    alerts_img_file_url_signed = f"{alerts_img_file_url}?{sas_token_info['sas_token']}"
    alerts_img_client =  BlobClient.from_blob_url(alerts_img_file_url_signed)
    if alerts_img_client.exists():
        alerts_img_client.delete_blob()
    alerts_img_client = None
    
    meta_img_file = f"gmw_{tile}_{date_str}_chg_alerts_meta.tif"
    meta_img_file_url = os.path.join(sas_token_info["url"], azure_img_dir, meta_img_file)
    meta_img_file_url_signed = f"{meta_img_file_url}?{sas_token_info['sas_token']}"
    meta_img_client =  BlobClient.from_blob_url(meta_img_file_url_signed)
    if meta_img_client.exists():
        meta_img_client.delete_blob()
    meta_img_client = None

