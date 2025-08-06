import pandas as d
from deltalake import DeltaTable
import argparse
import sys

def read_and_display_delta_table(table_path: str):
    try:
        print(f"Reading Delta Table from: '{table_path}'")
        
        dt = DeltaTable(table_path)
        df = dt.to_pandas()
        
        print("\n--- Table Metadata ---")
        print(f"Version: {dt.version()}")
        print(f"Schema: {dt.schema()}")
        
        print("\n--- Content ---")
        print(df)

    except Exception as e:
        print(f"\nError reading table: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    args = parser.parse_args()
    read_and_display_delta_table(args.path)
