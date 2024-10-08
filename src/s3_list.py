import os
import boto3
import pyorc
import pandas as pd
from datetime import timedelta, datetime
import json



s3resource = boto3.client('s3')
BUCKET_NAME = "fs-upper-body-gan-dataset"
KEY_FORMAT = "report/fs-upper-body-gan-dataset/FS-Inventory-Request/{0}T01-00Z/manifest.json"

def list_s3_from_root(root_name, root_path="./"):
    ORC_FILE = os.path.join(root_path, "s3_list_orc.orc")
    JSON_FILE = os.path.join(root_path, "s3_list_json.json")
    
    download_orc = False
    current_date = datetime.now()
    while not download_orc:
        current_datetime = current_date.strftime('%Y-%m-%d')

        key = KEY_FORMAT.format(current_datetime)

        try:
            s3resource.download_file(Bucket=BUCKET_NAME, Key=key, Filename=JSON_FILE)
            print("Downloaded json data list from", key)
        except Exception as e:
            current_date -= timedelta(days=1)
            print(key, e)
            continue

        with open(JSON_FILE, "r") as f:
            json_data = json.load(f)
        data_key = json_data["files"][0]["key"]
        s3resource.download_file(Bucket=BUCKET_NAME, Key=data_key, Filename=ORC_FILE)
        print("Downloaded orc data list")

        download_orc = True

    with open(ORC_FILE, 'rb') as data:
        reader = pyorc.Reader(data)

        columns = reader.schema.fields
        columns = [col_name for col_idx, col_name in sorted([
            (reader.schema.find_column_id(c), c) for c in columns
        ])]
        df = pd.DataFrame(reader, columns=columns)

    relevant_keys = []
    
    for i in range(df.shape[0]):
        if root_name in df.loc[i, "key"]:
            relevant_keys.append(df.loc[i, "key"])
    print(f"Found {len(relevant_keys)} from {root_name} in the orc file")
    
    os.remove(ORC_FILE)
    os.remove(JSON_FILE)
    print("Cleaned up s3 list files")
        
    return relevant_keys


if __name__ == "__main__":
    print(list_s3_from_root("raw"))
