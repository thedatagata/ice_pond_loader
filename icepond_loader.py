import dlt 
from dlt.sources.filesystem import filesystem, read_csv 
from dlt.destinations import athena
import pandas as pd

athena_pipeline = dlt.pipeline(pipeline_name="iceberg_delivery", destination="athena", dataset_name="raw_data", staging="filesystem")

@dlt.resource(table_format="iceberg")
def ice_machine(ice_chunk):
    yield pd.read_csv(ice_chunk).to_dict(orient='records')

raw_files = filesystem(bucket_url=dlt.config["sources.filesystem.file_directory"], file_glob='*.csv')
for raw_file in raw_files:
    file_name = raw_file['file_name']
    print(file_name)
    athena_pipeline.run(ice_machine(raw_file['file_url']), table_name=file_name)