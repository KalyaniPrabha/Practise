from pyarrow import csv as arrow_csv
from pyarrow import parquet as pq

def csv_to_parquet(source_csv_path,target_parquet_path):
    with open(source_csv_path, 'rb') as f:
        table = arrow_csv.read_csv(f)

    pq.write_table(table, target_parquet_path)

if __name__ == "__main__":
    csv_to_parquet('../source/employee_command_line.csv','..\\lz_target\\employee_using_pyarrow.parquet')
