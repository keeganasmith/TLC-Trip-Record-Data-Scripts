import duckdb

duckdb.sql("""
COPY (
  SELECT * 
  FROM read_parquet(['yellow_tripdata_2017-03.parquet', 'yellow_tripdata_2017-04.parquet'], union_by_name=true)
)
TO 'yellow_all.parquet'
(FORMAT PARQUET);
""")

