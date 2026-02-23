import duckdb

duckdb.sql("""
COPY (
  SELECT * 
  FROM
  read_parquet(['yellow_tripdata_2011-*.parquet','yellow_tripdata_2010-*.parquet','yellow_tripdata_2012-*.parquet'], union_by_name=true)
)
TO 'yellow_all.parquet'
(FORMAT PARQUET);
""")

