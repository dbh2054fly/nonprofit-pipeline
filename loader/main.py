import duckdb
from db_insertion import insert
from streamer import process_xml
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
data_path = os.getenv("IRS_DATA_PATH")

conn = duckdb.connect("irs990.duckdb")
with open("sql/schema.sql", "r") as f:
    schema_sql = f.read()
    conn.execute(schema_sql)

insert(data_path, conn)
