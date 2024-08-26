import duckdb
import os
import pyarrow.parquet as pq
import pyarrow as pa
import tqdm
import sys

if not os.path.exists('data'):
    os.mkdir('data')

con = duckdb.connect(database=':memory:')
con.execute("INSTALL tpch; LOAD tpch")
con.execute("CALL dbgen(sf=1)")
tables = ["lineitem", "customer", "nation", "orders", "part", "partsupp", "region", "supplier"]
for t in tqdm.tqdm(tables):
    res = con.query("SELECT * FROM " + t)
    res_arrow = res.to_arrow_table()
    new_schema = []
    for field in res_arrow.schema:
        if isinstance(field.type, type(pa.decimal128(1))):
            new_schema.append(pa.field(field.name, pa.float64()))
        elif field.type == pa.date32():
            new_schema.append(pa.field(field.name, pa.timestamp('ns')))
        else:
            new_schema.append(field)
    res_arrow = res_arrow.cast(pa.schema(new_schema))
    pq.write_table(res_arrow, os.path.join('data', f"{t}.parquet"))

