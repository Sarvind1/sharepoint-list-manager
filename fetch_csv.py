import requests
import csv
import json
import sys
from io import StringIO

# Power Automate webhook URL
url = "https://default0922decaaf3c4870acea84b9557b04.6a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/44beb61d9191460db68d59f67b7c87b3/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=y72LE0O_3WwR5RidMhN4MM6ZrHdegK6wayOf7Vt2EIw"

def fetch_csv_data(list_name):
    """
    Fetch CSV data from the Power Automate webhook.

    Args:
        list_name: The SharePoint list name to fetch data from

    Returns the response content.
    """
    try:
        # Prepare request body with list name
        payload = {"list_name": list_name}
        headers = {"Content-Type": "application/json"}

        # Make POST request to the URL
        response = requests.post(url, json=payload, headers=headers)

        # Check if request was successful
        response.raise_for_status()

        # Parse JSON response
        json_response = response.json()

        # Extract CSV data from the JSON response
        if 'csv_data' in json_response:
            return json_response['csv_data']
        else:
            print("Warning: 'csv_data' key not found in response")
            return response.text

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None

def parse_csv(csv_content):
    """
    Parse CSV content and return as list of dictionaries.
    """
    if not csv_content:
        return []

    csv_reader = csv.DictReader(StringIO(csv_content))
    return list(csv_reader)

def main():
    # Get list name from command line argument
    if len(sys.argv) < 2:
        print("Usage: python fetch_csv.py <list_name>")
        print("Example: python fetch_csv.py OT_3")
        sys.exit(1)

    list_name = sys.argv[1]
    print(f"Fetching CSV data from Power Automate for list: {list_name}...")

    # Fetch the CSV data
    csv_content = fetch_csv_data(list_name)

    if csv_content:
        print("\nCSV data received successfully!")

        # Parse and display the data
        data = parse_csv(csv_content)

        if data:
            print("\n" + "="*50)
            print(f"Total Records: {len(data)}")
            print("="*50)

            # Display first 3 rows as sample
            for i, row in enumerate(data[:3], 1):
                print(f"\nRow {i}:")
                for key, value in row.items():
                    print(f"  {key}: {value}")

            if len(data) > 3:
                print(f"\n... and {len(data) - 3} more rows")

        # Save to file
        with open('output.csv', 'w', encoding='utf-8', newline='') as f:
            f.write(csv_content)
        print("\n\nCSV saved to 'output.csv'")
    else:
        print("Failed to fetch CSV data.")

if __name__ == "__main__":
    main()
