import csv
import os
import json
import sys

def get_input_file(input_dir='/data/inputs'):
    """
    Ocean stores inputs in a structure like: /data/inputs/{did}/{service_id}/{index}
    The file is often named '0' (the index) without an extension.
    We must find the file regardless of its name.
    """
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            # Ignore hidden files
            if file.startswith("."):
                continue
            
            # Found the input file
            print(f"DEBUG: Found input file: {file} at {root}", file=sys.stderr)
            return os.path.join(root, file)
    return None

def run_analysis():
    print("Starting Air Quality Analysis...", file=sys.stderr)

    # 1. Locate File
    csv_path = get_input_file()
    if not csv_path:
        print("❌ No input file found in /data/inputs", file=sys.stderr)
        return

    # 2. Process Data
    results = {}
    
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
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

    # 3. Save Results to Output File (Required for consumer download)
    output_path = '/data/outputs/max_values_report.json'
    try:
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=4)
        print(f"✅ Results saved to {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error writing output: {e}", file=sys.stderr)

    # ---------------------------------------------------------
    # 4. PRINT RESULTS (Added per request)
    # This prints the 'results' variable to the logs (Standard Output)
    # ---------------------------------------------------------
    print("\n=== FINAL RESULTS ===")
    print(json.dumps(results, indent=4))
    print("=====================")

if __name__ == "__main__":
    run_analysis()
