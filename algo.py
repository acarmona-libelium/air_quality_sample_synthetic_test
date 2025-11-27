import pandas as pd
import os
import json
import sys

def get_input_file(input_dir='/data/inputs'):
    """
    Ocean stores inputs in a structure like: /data/inputs/{did}/{service_id}/file.csv
    We must walk the directory to find the CSV regardless of the DID.
    """
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".csv"):
                return os.path.join(root, file)
    return None

def run_analysis():
    print("Starting Air Quality Analysis...", file=sys.stderr) # Logs go to stderr/stdout

    # 1. Locate File
    csv_path = get_input_file()
    if not csv_path:
        print("❌ No CSV file found in /data/inputs", file=sys.stderr)
        return

    # 2. Load Data (Handling semicolon delimiter)
    try:
        df = pd.read_csv(csv_path, delimiter=';')
        print("✅ Data loaded successfully.", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}", file=sys.stderr)
        return

    # 3. Processing: Find Max for each numeric column and keep the time
    results = {}
    
    # Identify value columns (exclude 'time')
    value_cols = [c for c in df.columns if c != 'time']
    
    for col in value_cols:
        try:
            # Find the index of the max value
            max_idx = df[col].idxmax()
            max_row = df.loc[max_idx]
            
            results[col] = {
                "max_value": float(max_row[col]),
                "timestamp": str(max_row['time'])
            }
        except Exception as e:
            results[col] = f"Error: {str(e)}"

    # 4. Save Results to /data/outputs
    # The user gets whatever is in this folder.
    output_path = '/data/outputs/max_values_report.json'
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=4)
    
    print(f"✅ Results saved to {output_path}", file=sys.stderr)

if __name__ == "__main__":
    run_analysis()