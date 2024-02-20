import requests
import json
from datetime import datetime
import pandas as pd
import time
from tqdm import tqdm

# API setup
api_key = 'fka_04x4dEPWrjGUcrcle1pr4SrCHskGOZ67B3'  # Replace with your actual API Key


def fetch_people(api_key):
    base_url = 'https://api.followupboss.com/v1/people'
    params = {'limit': 100, 'fields': 'allFields', 'includeTrash': 'true'}

    all_people = []
    total_fetched = 0
    retry_delay = 1
    max_retries = 5

    pbar = tqdm(desc="Fetching first 200000 people", total=200000, unit=" people")

    try:
        while total_fetched < 200000:
            try:
                response = requests.get(base_url, auth=(api_key, ''), params=params)

                if response.status_code == 429:
                    if max_retries == 0:
                        raise Exception("Maximum retries reached. Aborting operation.")
                    print(f"Rate limit hit. Retrying after {retry_delay} seconds.")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    max_retries -= 1
                    continue

                retry_delay = 1
                max_retries = 5

                response.raise_for_status()

                data = response.json()
                people = data.get('people', [])
                all_people.extend(people[:max(0, 200000 - total_fetched)])
                fetched_this_round = len(people[:max(0, 200000 - total_fetched)])
                total_fetched += fetched_this_round
                pbar.update(fetched_this_round)

                if '_metadata' in data and 'next' in data['_metadata']:
                    params['next'] = data['_metadata']['next']
                else:
                    break

            except requests.RequestException as e:
                print(f"An error occurred: {e}")
                break

    except KeyboardInterrupt:
        print("\nData fetching interrupted by user. Proceeding with the data collected so far.")

    pbar.close()
    return all_people[:200000]

# Uncomment below lines to use the function and save data to CSV



fetched_people_data = fetch_people(api_key)
df = pd.DataFrame(fetched_people_data)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f'first_200000_people_data_{timestamp}.csv'
df.to_csv(filename, index=False)
print(f"First 200,000 people data fetched and saved to {filename}")

# Save the raw JSON data to a file
json_filename = f'first_200000_people_data_{timestamp}.json'
with open(json_filename, 'w') as json_file:
    json.dump(fetch_people, json_file, indent=4)
print(f"First 200,000 people raw data saved to {json_filename}")