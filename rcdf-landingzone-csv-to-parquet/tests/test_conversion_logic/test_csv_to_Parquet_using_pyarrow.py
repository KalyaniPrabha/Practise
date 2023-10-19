import os
import pandas as pd
from src.conversion_logic.csv_to_Parquet_using_pyarrow import csv_to_parquet

def test_csv_to_parquet():
    data = {
        'name': ['John', 'Doe', 'Anna'],
        'age': [25, 30, 24],
        'department': ['HR', 'Operations', 'HR']
    }
    csv_df = pd.DataFrame(data)

    source_csv = "tests/test_source/test_employee_source.csv"
    target_parquet = "tests/test_target/test_employee_target.parquet"

    csv_df.to_csv(source_csv, index=False)

    csv_to_parquet(source_csv, target_parquet)

    parquet_df = pd.read_parquet(target_parquet)

    pd.testing.assert_frame_equal(csv_df, parquet_df)

    os.remove(source_csv)
    os.remove(target_parquet)


