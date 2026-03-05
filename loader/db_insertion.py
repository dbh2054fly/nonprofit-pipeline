import duckdb
from streamer import process_xml
import zipfile_deflate64 as zipfile
from tqdm import tqdm
import os
from dotenv import load_dotenv
import pandas as pd

def insert(irs_data_path, db_con, foundations_batchsize=1000, grants_batchsize=10000):
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
                        foundations_df = pd.DataFrame(foundations_batch, columns=[
                            "ein",
                            "business_name",
                            "address",
                            "city",
                            "state",
                            "zipcode",
                            "url",
                            "mission",
                            "does_grants",
                            "assets",
                            "revenue",
                            "expenses",
                            "grants_paid",
                            "return_type"
                        ])

                        db_con.register("foundations_df", foundations_df)

                        db_con.execute("""
                            INSERT INTO foundations
                            SELECT * FROM foundations_df
                            ON CONFLICT (ein, return_type) DO NOTHING
                        """)

                        foundations_batch.clear()
                    if len(grants_batch) >= grants_batchsize:
                        grants_df = pd.DataFrame(grants_batch, columns=[
                            "ein",
                            "recipient",
                            "amount",
                            "purpose"
                        ])

                        db_con.register("grants_df", grants_df)

                        db_con.execute("""
                            INSERT INTO grants
                            SELECT * FROM grants_df
                            ON CONFLICT (ein, recipient, amount) DO NOTHING
                        """)

                        grants_batch.clear()

    if foundations_batch:
        foundations_df = pd.DataFrame(foundations_batch, columns=[
            "ein","business_name","address","city","state","zipcode",
            "url","mission","does_grants","assets","revenue","expenses",
            "grants_paid","return_type"
        ])

        db_con.register("foundations_df", foundations_df)

        db_con.execute("""
            INSERT INTO foundations
            SELECT * FROM foundations_df
            ON CONFLICT (ein, return_type) DO NOTHING
        """)
        foundations_batch.clear()



    if grants_batch:

        grants_df = pd.DataFrame(grants_batch, columns=[
            "ein","recipient","amount","purpose"
        ])

        db_con.register("grants_df", grants_df)

        db_con.execute("""
            INSERT INTO grants
            SELECT * FROM grants_df
        """)
        grants_batch.clear()
                    

