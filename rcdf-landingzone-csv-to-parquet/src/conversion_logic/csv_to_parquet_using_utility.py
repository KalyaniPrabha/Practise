import subprocess
import csv2parquet.csv2parquet as csv

def csv_to_parquet_command_line(source_csv_path, target_parquet_path):
    subprocess.run(["csv2parquet", source_csv_path, "--output", target_parquet_path])


def csv_to_parquet_using_library_module(source_csv_path, target_parquet_path):
    csv.convert(csv_file=source_csv_path, output_file=target_parquet_path, row_group_size = 4000, codec = 'snappy', max_rows = None, rename = [], include = [], exclude = [], raw_types = [])

if __name__ == "__main__":
    csv_to_parquet_command_line('../source/employee_command_line.csv', '..\\lz_target\\employee_using_command_line.parquet')
    csv_to_parquet_using_library_module('../source/employee_lib_module.csv', '..\\lz_target\\employee_using_library_module.parquet')
