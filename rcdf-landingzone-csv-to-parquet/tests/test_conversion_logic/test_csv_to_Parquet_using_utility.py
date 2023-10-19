import os
import pandas as pd
import numpy as np
from src.conversion_logic.csv_to_parquet_using_utility import csv_to_parquet_command_line, \
    csv_to_parquet_using_library_module

def test_csv_to_parquet_command_line():
    data = {
        'name': ['John', 'Doe', 'Anna'],
        'age': [25, 30, 24],
        'department': ['HR', 'Operations', 'HR']
    }
    csv_df = pd.DataFrame(data)

    source_csv = "tests/test_source/test_employee_source.csv"
    target_parquet = "tests/test_target/test_employee_target.parquet"

    csv_df.to_csv(source_csv, index=False)

    csv_to_parquet_command_line(source_csv, target_parquet)

    parquet_df = pd.read_parquet(target_parquet)

    csv_values = csv_df.astype({'age': 'float64'}).values
    parquet_values = parquet_df.astype({'age': 'float64'}).values

    np.testing.assert_array_equal(csv_values, parquet_values)

    os.remove(source_csv)
    os.remove(target_parquet)

def test_csv_to_parquet_using_library_module():
    data = {
        'name': ['John', 'Doe', 'Anna'],
        'age': [25, 30, 24],
        'department': ['HR', 'Operations', 'HR']
    }
    csv_df = pd.DataFrame(data)

    source_csv = "tests/test_source/test_employee_source.csv"
    target_parquet = "tests/test_target/test_employee_target.parquet"

    csv_df.to_csv(source_csv, index=False)

    csv_to_parquet_using_library_module(source_csv, target_parquet)

    parquet_df = pd.read_parquet(target_parquet)

    csv_values = csv_df.astype({'age': 'float64'}).values
    parquet_values = parquet_df.astype({'age': 'float64'}).values

    np.testing.assert_array_equal(csv_values, parquet_values)

    os.remove(source_csv)
    os.remove(target_parquet)


