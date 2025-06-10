import os
import pandas as pd
import shutil
from pathlib import Path

# Clear and create samples directory
samples_dir = Path("private/samples")
if samples_dir.exists():
    shutil.rmtree(samples_dir)
samples_dir.mkdir()

# Get all CSV files from datasets directory
datasets_dir = Path("private/datasets")
csv_files = list(datasets_dir.glob("*.csv"))

for csv_file in csv_files:
    # Read the CSV with dtype=str to avoid warnings and speed up
    df = pd.read_csv(csv_file, dtype=str, low_memory=False)

    if len(df) <= 500:
        # Copy entire file with copy_ prefix
        output_path = samples_dir / f"copy_{csv_file.name}"
        df.to_csv(output_path, index=False)
    else:
        # Take first 500 rows with sample_ prefix
        output_path = samples_dir / f"sample_{csv_file.name}"
        df.head(500).to_csv(output_path, index=False)
