import csv
import os
import json
import sys

def get_input_file(input_dir='/data/inputs'):
    """
    Ocean stores inputs in a structure like: /data/inputs/{did}/{service_id}/{index}
    We must find the data file but ignore system files like 'algoCustomData.json'.
    To ensure it's the correct CSV, we check the first line for the expected header/delimiter.
    """
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            # 1. Ignore hidden files
            if file.startswith("."):
                continue
            
            # 2. Ignore Ocean Protocol configuration files
            if file == "algoCustomData.json":
                print(f"DEBUG: Skipping config file: {file}", file=sys.stderr)
                continue

            # 3. Validation: Peek inside to check for CSV signature
            full_path = os.path.join(root, file)
            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    # Read just the first line to check headers
                    header = f.readline()
                    
                    # We know the specific shape from the prompt: "time;no2_hourly_avg..."
                    # Check for the delimiter ';' and the first column 'time'
                    if ";" in header and "time" in header:
                        print(f"DEBUG: Found valid CSV input file: {file} at {root}", file=sys.stderr)
                        return full_path
                    else:
                        print(f"DEBUG: Skipping file {file} (Header mismatch: {header.strip()[:50]}...)", file=sys.stderr)
            
            except Exception as e:
                print(f"DEBUG: Could not read file {file}: {e}", file=sys.stderr)
                continue

    return None

def run_analysis():
    print("Starting Air Quality Analysis...", file=sys.stderr)

    # 1. Locate File
    csv_path = get_input_file()
    if not csv_path:
        print("❌ No valid input file found in /data/inputs", file=sys.stderr)
        return

    # 2. Process Data
    results = {}
    
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            # Handle the semicolon delimiter
            reader = csv.DictReader(f, delimiter=';')
            
            if not reader.fieldnames:
                print("❌ File is empty or has no header.", file=sys.stderr)
                return

            value_cols = [c for c in reader.fieldnames if c != 'time']
            
            # Initialize results
            for col in value_cols:
                results[col] = {"max_value": float('-inf'), "timestamp": None}

            # Iterate rows
            for row in reader:
                current_time = row.get('time')
                for col in value_cols:
                    try:
                        val = float(row[col])
                        if val > results[col]['max_value']:
                            results[col]['max_value'] = val
                            results[col]['timestamp'] = current_time
                    except (ValueError, TypeError):
                        continue

    except Exception as e:
        print(f"❌ Error processing file: {e}", file=sys.stderr)
        return

    # 3. Save Results to Output File
    output_path = '/data/outputs/max_values_report.json'
    try:
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=4)
        print(f"✅ Results saved to {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error writing output: {e}", file=sys.stderr)

    # 4. Print Results to Logs
    print("\n=== FINAL RESULTS ===")
    print(json.dumps(results, indent=4))
    print("=====================")

if __name__ == "__main__":
    run_analysis()
