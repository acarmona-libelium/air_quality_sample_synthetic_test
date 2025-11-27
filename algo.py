%%writefile algo.py
import csv
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
    print("Starting Air Quality Analysis (Standard Lib)...", file=sys.stderr)

    # 1. Locate File
    csv_path = get_input_file()
    if not csv_path:
        print("❌ No CSV file found in /data/inputs", file=sys.stderr)
        return

    # 2. Process Data using 'csv' module instead of 'pandas'
    # Initialize a dictionary to hold the max record for each column
    # Structure: {'column_name': {'max_value': -float('inf'), 'timestamp': ''}}
    results = {}
    
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            # Handle the semicolon delimiter
            reader = csv.DictReader(f, delimiter=';')
            
            # Identify value columns (exclude 'time')
            if not reader.fieldnames:
                print("❌ CSV is empty or has no header.", file=sys.stderr)
                return

            value_cols = [c for c in reader.fieldnames if c != 'time']
            
            # Initialize results with negative infinity for comparison
            for col in value_cols:
                results[col] = {"max_value": float('-inf'), "timestamp": None}

            # Iterate row by row (Standard Python approach vs Pandas Vectorization)
            for row in reader:
                current_time = row['time']
                
                for col in value_cols:
                    try:
                        # Convert string to float
                        val = float(row[col])
                        
                        # Check if this is the new maximum
                        if val > results[col]['max_value']:
                            results[col]['max_value'] = val
                            results[col]['timestamp'] = current_time
                            
                    except ValueError:
                        # Handle cases where data might be missing or malformed
                        continue

    except Exception as e:
        print(f"❌ Error processing CSV: {e}", file=sys.stderr)
        return

    # 3. Save Results to /data/outputs
    output_path = '/data/outputs/max_values_report.json'
    try:
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=4)
        print(f"✅ Results saved to {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error writing output: {e}", file=sys.stderr)

if __name__ == "__main__":
    run_analysis()
