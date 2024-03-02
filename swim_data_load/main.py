import os
import duckdb
import PyPDF2
import datetime
import pandas

# Load environment variables
source_data_directory = os.environ.get("SOURCE_DATA_DIRECTORY")
if source_data_directory is None:
    raise KeyError("No environment variable set for 'SOURCE_DATA_DIRECTORY'")

database_directory = os.environ.get("DUCKDB_DIRECTORY")
if database_directory is None:
    raise KeyError("No environment variable set for 'DUCKDB_DIRECTORY'")

# Connect to duckDB and create blank source table to load into
database = duckdb.connect(database=f"{database_directory}\\swim_data.duckdb")

database.execute("""
    DROP SCHEMA IF EXISTS isl_raw CASCADE;
    CREATE SCHEMA isl_raw;
""")

with open("./table_definitions/isl_raw/pdf_page.sql", "r") as create_source_table_file:
    create_source_table_stmt = create_source_table_file.read()

database.execute(create_source_table_stmt)

# Read PDF files and create pandas dataframe in the correct shape for the source table
data_to_insert = []

pdf_files = os.scandir(source_data_directory)
load_datetime = datetime.datetime.now()

for pdf_file in pdf_files:
    reader = PyPDF2.PdfReader(pdf_file.path)

    for page_num, page in enumerate(reader.pages):
        data_to_insert.append({
            "file_name": pdf_file.name,
            "page_number": page_num + 1,
            "page_text": page.extract_text(),
            "loaded_datetime": load_datetime
        })

dataframe_to_insert = pandas.DataFrame(data_to_insert)

# Insert data from pandas dataframe into the source table in duckDB
duckdb.register("dataframe_to_insert", dataframe_to_insert)

database.execute("""
    INSERT INTO isl_raw.pdf_page
    SELECT
        *
    FROM
        dataframe_to_insert;
""")
