# save_to_delta.py
import os
from datetime import datetime
import polars as pl
from deltalake import DeltaTable

# Function to save data to Delta Lake format
def save_to_delta_table(data: pl.DataFrame, path: str, mode):
    # Ensure the path exists, or create it (you could use pathlib for this)
    os.makedirs(path, exist_ok=True)
    
    # Create a file path within the directory
    file_path = os.path.join(path, "")
    print("Starting to write into Delta Parquet: ")
    print(datetime.now())
    # Check if the table exists and handle mode appropriately
    if os.path.exists(file_path):
        if mode == "overwrite":
            print("Overwriting the existing Delta Lake table.")
            data.write_delta(file_path,mode="overwrite")
            print(f"Data successfully written to {file_path} in {mode} mode.")
        elif mode == "append":
            print("Appending to the existing Delta Lake table.")
            data.write_delta(file_path,mode="append")
            return
        else:
            raise ValueError("Invalid mode: Choose either 'overwrite' or 'append'.")
    else:
        print("Creating a new Delta Lake table.")