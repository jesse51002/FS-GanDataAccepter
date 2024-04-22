import os
import math
import shutil
import boto3
from threading import Thread, Lock
from s3_list import list_s3_from_root

# s3 boto documentation
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

# Test it on a service (yours may be different)
s3resource = boto3.client('s3')
BUCKET_NAME = "fs-upper-body-gan-dataset"
MAX_FOLDER_COUNT_DOWNLOAD = 10000000


def zip_parser(abs_folder_path, zip_file):
    if not os.path.isdir(abs_folder_path):
        os.makedirs(abs_folder_path)

    shutil.unpack_archive(zip_file, abs_folder_path)
    os.remove(zip_file)

    print(f"Finsihed parsing {abs_folder_path}")


def download_aws_folder(abs_folder_path, s3_key):
    if not os.path.isdir(abs_folder_path):
        os.makedirs(abs_folder_path)
    
    zip_file_pth = abs_folder_path+".zip"

    print(f"Starting {s3_key} download")
    s3resource.download_file(Bucket=BUCKET_NAME, Key=s3_key, Filename=zip_file_pth)
    print(f"Downloaded {s3_key}")

    print(f"Starting zip parsing for {s3_key}")
    zip_process_thread = Thread(target=zip_parser, args=(abs_folder_path, zip_file_pth))
    zip_process_thread.start()

    return zip_process_thread
    

def get_download_folders(prefix, clean_dir, accept_dir, reject_dir, index=0, total_count=1):
    keys_in_s3 = list_s3_from_root(prefix, clean_dir)
    if prefix in keys_in_s3:
        keys_in_s3.remove(prefix)
    if prefix + ".ipynb_checkpoints.zip" in keys_in_s3:
        keys_in_s3.remove(prefix + ".ipynb_checkpoints.zip")
    keys_in_s3.sort()
    
    amount_per = math.ceil(len(keys_in_s3) / total_count)
    start_idx = index * amount_per
    end_idx = min((index+ 1) * amount_per, len(keys_in_s3))
    
    keys_in_s3 = keys_in_s3[start_idx:end_idx]
    
    
    # Gets folders that have already been downloaded
    finished_download = os.listdir(clean_dir) + os.listdir(accept_dir) + os.listdir(reject_dir) 

    i = 0
    split_keys = []
    while i < len(keys_in_s3):
        key = keys_in_s3[i].split("/")[-1][:-4]
        split_keys.append(key)
        for finished in finished_download:
            if finished == key:
                keys_in_s3.pop(i)
                i -= 1
                break
        
        i += 1

    started_directories = set(os.listdir(accept_dir) + os.listdir(reject_dir))
    split_keys = set(split_keys)
    not_done_directories = set(os.listdir(clean_dir))
    
    not_in_key_dirs = list((not_done_directories - started_directories) - split_keys)
    print("Removing:", not_in_key_dirs)
    
    for remove_dir in not_in_key_dirs:
        shutil.rmtree(os.path.join(clean_dir, remove_dir))
        back_rem_clean_dir = "/".join(os.path.split(clean_dir)[:-1]) + "/clean_images_background_removed"
        shutil.rmtree(os.path.join(back_rem_clean_dir, remove_dir))
    
    return keys_in_s3[:min(MAX_FOLDER_COUNT_DOWNLOAD, len(keys_in_s3))]
            
                
def download_from_aws(split_dir, accept_dir, reject_dir, index=0, total_count=1):
    rel_base = split_dir.replace("\\", "/").split("/")[-1] + "/"
    
    download_folders = get_download_folders(rel_base, split_dir, accept_dir, reject_dir, index, total_count)

    threads = []
    
    for key in download_folders:
        abs_folder_path = os.path.join(split_dir, key[len(rel_base):-4])

        zip_process_thread = download_aws_folder(abs_folder_path, key)
        threads.append(zip_process_thread)
        
        back_rem_key = "clean_images_background_removed" + "/" + "/".join(key.split("/")[1:])
        
        back_rem_abs_pth = os.path.join("/".join(os.path.split(split_dir)[:-1]) + "/clean_images_background_removed", key[len(rel_base):-4])
        try:
            zip_process_thread = download_aws_folder(back_rem_abs_pth, back_rem_key)
        except Exception as e:
            print(e)
            
        threads.append(zip_process_thread)

    for t in threads:
        t.join()
    

if __name__ == "__main__":
    if not os.path.isdir("./data/clean_images"):
        os.makedirs("./data/clean_images")
    if not os.path.isdir("./data/clean_images_background_removed"):
        os.makedirs("./data/clean_images_background_removed")
    
    download_from_aws("./data/clean_images", index=0, total_count=1000)
    