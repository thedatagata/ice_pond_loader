import dlt 
from dlt.sources.filesystem import filesystem
from dlt.destinations.adapters import athena_partition, athena_adapter
import duckdb 
import pandas as pd
import enlighten

athena_pipeline = dlt.pipeline(pipeline_name="iceberg_delivery", destination="athena", dataset_name="ice_cubes", staging="filesystem")


@dlt.resource(table_format="iceberg", write_disposition="append")
def ice_dispensar(ice_file_data):
    progress = 0 
    manager = enlighten.get_manager()
    ice_file_url = ice_file_data['file_url'].replace("file://", "")
    row_count = duckdb.query(f"SELECT COUNT(*) FROM '{ice_file_url}'").fetchone()[0]
    pbar = manager.counter(total=row_count, desc='Basic', unit='ticks')
    for ice_df in pd.read_csv(ice_file_url, chunksize=100000):
        progress += ice_df.shape[0]
        pbar.update(ice_df.shape[0])
        yield ice_df.to_dict(orient="records")
    progress = 0
    pbar.close()

athena_adapter(
    ice_dispensar,
    partition=[
        # Partition per year and month
        athena_partition.year("tpep_pickup_datetime"),
        athena_partition.month("tpep_pickup_datetime"),
        athena_partition.day("tpep_pickup_datetime"),
        athena_partition.year("tpep_dropoff_datetime"),
        athena_partition.month("tpep_dropoff_datetime"),
        athena_partition.day("tpep_dropoff_datetime"),
    ],
)

for f in filesystem(bucket_url=dlt.config["sources.filesystem.file_directory"], file_glob='*.csv'):
    run_info = athena_pipeline.run(ice_dispensar(f), table_name='yellow_cab_data')
    print(run_info)