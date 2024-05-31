import os
import sys
from download_annotator_data import DATA_FOLDER

folder_name = "full"
data_folder = os.path.join(DATA_FOLDER, folder_name)


img_types = ['png', 'jpeg', 'jpg', 'webp']

# Finds all the images in a directory
def find_images(path: str):
    img_list = []
    
    if os.path.isdir(path):
        for sub in os.listdir(path):
            if ".ipynb_checkpoints" in sub:
                continue
            img_list += find_images(os.path.join(path, sub))
    else:
        name = os.path.basename(path)
        file_type = name.split(".")[-1]
        if file_type in img_types:
            img_list.append(path)
        
    return img_list


accept_count = len(find_images(os.path.join(data_folder, "accepted")))
print("accepted body images:", accept_count)
reject_count = len(find_images(os.path.join(data_folder, "rejected")))
print("raw body images:", reject_count)
clean_count = len(find_images(os.path.join(data_folder, "clean")))
print("clean body images:", clean_count)

print(f"Accept percentage: {100 * accept_count / (accept_count + reject_count)}%")



