from queries import q2
import os

import pandas as pd
import polars as pl
import pyarrow.parquet as pq
import dask.dataframe as dd

pd.options.mode.copy_on_write = True
pd.options.future.infer_string = True

region = os.path.join("data", "region.parquet")
nation = os.path.join("data", "nation.parquet")
supplier = os.path.join("data", "supplier.parquet")
part = os.path.join("data", "part.parquet")
partsupp = os.path.join("data", "partsupp.parquet")

IO_FUNCS = {
    "pandas": lambda x: pd.read_parquet(x, engine="pyarrow"),
    "pandas[pyarrow]": lambda x: pd.read_parquet(
        x, engine="pyarrow", dtype_backend="pyarrow"
    ),
    "polars[eager]": lambda x: pl.read_parquet(x),
    "polars[lazy]": lambda x: pl.scan_parquet(x),
    "pyarrow": lambda x: pq.read_table(x),
    "dask": lambda x: dd.read_parquet(x, engine="pyarrow", dtype_backend="pyarrow"),
}


tool = "pandas[pyarrow]"
fn = IO_FUNCS[tool]
print(
    q2.query(
        fn(region),
        fn(nation),
        fn(supplier),
        fn(part),
        fn(partsupp),
    )
)
tool = "polars[lazy]"
fn = IO_FUNCS[tool]
print(
    q2.query(
        fn(region),
        fn(nation),
        fn(supplier),
        fn(part),
        fn(partsupp),
    ).collect()
)
tool = "pyarrow"
fn = IO_FUNCS[tool]
print(
    q2.query(
        fn(region),
        fn(nation),
        fn(supplier),
        fn(part),
        fn(partsupp),
    )
)
tool = "dask"
fn = IO_FUNCS[tool]
print(
    q2.query(
        fn(region),
        fn(nation),
        fn(supplier),
        fn(part),
        fn(partsupp),
    ).compute()
)
