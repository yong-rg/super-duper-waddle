"""
Start with exporting Zeiss AMZ Attribution reports at the Product level. Files should save in Downloads folder by default.
https://advertising.amazon.com/attribution/campaigns?entityId=ENTITY22J5BMAIHR9X4

"""
import duckdb
import pandas as pd
import os
import shutil
from datetime import datetime
from datetime import date

# Create folder and file variables
downloads = 'C:/Users/24G/Downloads'
dl_files = os.listdir(downloads)
gdrive = 'G:/Shared drives/Paid Search and Social/Client Folders/Zeiss/Reporting/XLSX'

# Get AMZ Attr Reports in Downloads folder
# Find downloads and get target file names
name = 'Amazon_Attribution_campaign_products'
date = date.today()
attr_files = []

# Loop through download folder
def get_file_names(dl_files, downloads, date, name, gdrive):
    for file in dl_files:
        file_path = f'{downloads}/{file}'
        
        file_time = os.path.getctime(file_path)
        created_at = date.fromtimestamp(file_time)
        if name in file and created_at.month >= date.month:
            attr_files.append(file_path) #file_path for targeting whole file path
            shutil.copy(file_path, gdrive)
            print(f'copying {file} to {gdrive}!')
    print(attr_files)

# Clear G Drive Folder
def archive_gdrive(gdrive):
    # Archive existing GDrive
    g_files = os.listdir(gdrive)

    # Move files to gdrive folder
    for g in g_files:
        src = os.path.join(gdrive, g)
        archive = os.path.join(gdrive,'archive')
        shutil.move(src, archive)
        print(f'moving {g} to {archive}!')

# Run
archive_gdrive(gdrive)
get_file_names(dl_files, downloads, date, name, gdrive)
