# Narwhals TPC-H queries

Utilities for running the TPC-H queries via Narwhals.

Here's an (opinionated) guide to how to get setup:

## Environment setup:

- [install uv](https://github.com/astral-sh/uv?tab=readme-ov-file#installation)
- install Python3.12: `uv python install 3.12`
- create a virtual environment: `uv venv -p 3.12 --seed`
- activate it as per instructions shown on the terminal
- install requirements: `uv pip install -r requirements.txt`

## Generate data

Run `python generate_data.py`.

## Run queries

To run Q1, you can run `python -m execute.q1`.

Please add query definitions in `queries`, and scripts to execute them
in `execute` (see `queries/q1.py` and `execute/q1.py` for examples).

