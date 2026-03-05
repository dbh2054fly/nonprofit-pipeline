import duckdb
from streamer import process_xml
import zipfile_deflate64 as zipfile
from tqdm import tqdm
import os
from dotenv import load_dotenv

def insert(irs_data_path, foundations_batchsize=1000, grants_batchsize=10000):
    foundations_batch = []
    grants_batch = []
    for zip_name in tqdm(os.listdir(irs_data_path), desc="Processing zip files"):
        if zip_name.endswith(".zip"):
            file_path = os.path.join(irs_data_path, zip_name)
            with zipfile.ZipFile(file_path) as z:
                for file in tqdm(z.namelist(), desc=f"Processing XMLs in {zip_name}", leave=False):

                    if not file.endswith(".xml"):
                        continue

                    with z.open(file) as f:
                        foundation, grants = process_xml(f)

                    foundations_batch.append(foundation)
                    grants_batch.extend(grants)

                    if len(foundations_batch) >= foundations_batchsize:
                        #insertion logic

                    if len(grants_batch) >= grants_batchsize:
                        #insertion logic
                        
    bad = []
    for r in foundations_batch:
        ein = r[0]
        if ein is None or len(ein) != 9 or not ein.isdigit():
            bad.append(ein)

    print("bad count:", len(bad))
    print("examples:", bad[:20])
                    
load_dotenv()
insert(os.getenv("IRS_DATA_PATH"))

