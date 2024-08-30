from queries import q10
import os

import pandas as pd
import polars as pl

pd.options.mode.copy_on_write = True
pd.options.future.infer_string = True

customer = os.path.join("data", "customer.parquet")
nation = os.path.join("data", "nation.parquet")
lineitem = os.path.join("data", "lineitem.parquet")
orders = os.path.join("data", "orders.parquet")

IO_FUNCS = {
    'pandas': lambda x: pd.read_parquet(x, engine='pyarrow'),
    'pandas[pyarrow]': lambda x: pd.read_parquet(x, engine='pyarrow', dtype_backend='pyarrow'),
    'polars[eager]': lambda x: pl.read_parquet(x),
    'polars[lazy]': lambda x: pl.scan_parquet(x),
}

tool = 'pandas'
fn = IO_FUNCS[tool]
print(
    q10.query(
        fn(customer),
        fn(nation),
        fn(lineitem),
        fn(orders)
    )
)

tool = 'pandas[pyarrow]'
fn = IO_FUNCS[tool]
print(
    q10.query(
        fn(customer),
        fn(nation),
        fn(lineitem),
        fn(orders)
    )
)

tool = 'polars[eager]'
fn = IO_FUNCS[tool]
print(
    q10.query(
        fn(customer),
        fn(nation),
        fn(lineitem),
        fn(orders)
    )
)

tool = 'polars[lazy]'
fn = IO_FUNCS[tool]
print(
    q10.query(
        fn(customer),
        fn(nation),
        fn(lineitem),
        fn(orders)
    ).collect()
)
