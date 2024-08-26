from queries import q1
import os

import pyarrow.parquet as pq
import polars as pl
import dask.dataframe as dd

lineitem = os.path.join('data', 'lineitem.parquet')

print(q1.query(pq.read_table(lineitem)))
print(q1.query(pl.scan_parquet(lineitem)).collect())
print(q1.query(dd.read_parquet(lineitem)).compute())

