import os
import requests
import pandas as pd
import yaml
from dotenv import load_dotenv
from db_utils import get_engine, get_latest_date, load_to_sqlite

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv('EIA_API_KEY')

# Load configuration settings from config.yaml
def load_config(config_path="config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

"""
Initial test function to work out API call to the EIA for data retrieval.
def test_extraction():
    # This URL targets "Retail sales of electricity"
    url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
    
    # These parameters tell the API exactly what we want
    params = {
        "api_key": API_KEY,
        "frequency": "monthly",
        "data[0]": "price",
        "facets[stateid][]": "NY",  # Let's look at New York data
        "length": 5                 # Just get 5 rows for now to test
    }
    
    print("Connecting to EIA API...")
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        # The actual data rows are nested inside 'response' -> 'data'
        records = data['response']['data']
        print("Success! Here is a sample of the raw data:")
        for record in records:
            print(record)
    else:
        print(f"Failed. Error code: {response.status_code}")
        print(response.text)
"""


#Extract data from EIA API, transform it into a clean DataFrame, and print the first few rows to verify everything works end-to-end.
def extract_data(state, config, last_month=None):
    url = config['api']['base_url']
    params = {
        "api_key": API_KEY,
        "frequency": config['api']['frequency'],
        "data[0]": "price",
        "facets[stateid][]": state,# Use the passed state parameter
        "length": config['batch_settings']['length']# Get more data for actual processing
    }

    # Start from the last month we already have in the DB to save API resources
    if last_month:
        params["start"] = last_month

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data['response']['data'] #the actual data rows are nested inside 'response' -> 'data'
    else:
        print(f"Error fetching {state}: {response.status_code}")
        return []


#Load raw data into a pandas DataFrame, and performs all necessary transformations to clean and structure the data for analysis.
def transform_data(raw_data):
    if not raw_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)

    #Rename and only keep neccesarry columns for our analysis
    columns_mapping = {
        'period': 'month',
        'stateid': 'state',
        'sectorName': 'sector',
        'price': 'price',
        'price-units': 'units'
    }

    df = df[list(columns_mapping.keys())].rename(columns=columns_mapping)

    #Convert data types: price should be numeric, month should be datetime
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['month'] = pd.to_datetime(df['month'])


    #Handle missing or 'None' values
    df = df.dropna(subset=['price', 'month']) #Drop rows where price or month is missing
    df =  df[df['price'] > 0] #Filter out any negative prices which are likely errors

    """
    Simple data quality report to help pick/justify the dropping of rows with missing values.
    
    # Count missing values BEFORE dropping
    missing_price = df['price'].isna().sum()
    missing_month = df['month'].isna().sum()
    missing_both = (df['price'].isna() & df['month'].isna()).sum()
    initial_count = len(df)
    
    #AFTER dropping rows
    dropped_count = initial_count - len(df) 
    print("\nData Quality Report")
    print("-------------------")
    print(f"Missing price:  {missing_price}")
    print(f"Missing month:  {missing_month}")
    print(f"Missing both:   {missing_both}")
    print(f"Total dropped:  {dropped_count}\n")
    """

    df['month'] = df['month'].dt.strftime('%Y-%m') #Convert to monthly period for cleaner analysis

    df = df.reset_index(drop=True) #Reset index after dropping rows to keep it clean

    return df


if __name__ == "__main__":
    config = load_config()
    engine = get_engine(config['database']['db_name'])
    table = config['database']['table_name']

    #check the latest month we have in the database to avoid redundant API calls
    latest_month = get_latest_date(engine, table)
    print(f"Latest month in database: {latest_month}")

    all_data_frames = []
    for state in config['api']['facets']['state']:
        print(f"\Extracting data for state: {state}")
        
        #Extract Raw Data via API call
        raw_data = extract_data(state, config, latest_month)

        clean_df = transform_data(raw_data)
        """
        The below combined with the data quality report indicated that all the 'other' sector data
        was dropped due to missing/incomplete price values, which is a useful insight about this dataset.

        print(f"\nSector counts:\n {clean_df['sector'].value_counts()}\n")
        print("Data transformation complete. First 20 rows:")
        print(clean_df.head(20))
        """
        
        if not clean_df.empty:
            # 2. Safety filter: only keep rows strictly NEWER than last_date
            if latest_month:
                clean_df = clean_df[clean_df['month'] > latest_month]

        all_data_frames.append(clean_df)

    if all_data_frames:
        # Combine all states into one master DataFrame
        master_df = pd.concat(all_data_frames, ignore_index=True)

        """
        Use for SQL databases with a native DATETIME storage class.
        SQLite doesn't have a true DATETIME type, so we store it as TEXT in 'YYYY-MM' format for simplicity.
        master_df['month'] = pd.to_datetime(master_df['month'], errors='coerce')
        """
        master_df = master_df.dropna(subset=['month'])
        
        load_to_sqlite(master_df, config['database']['table_name'], engine)
        
        """
        #Quick check to confirm data is in the database
        query_result = pd.read_sql("SELECT * FROM electricity_prices LIMIT 5", engine)
        print("\nVerification - First 5 rows in Database:")
        print(query_result)
        """

        print(f"\nIncremental ETL Job finished. Records Updated: {len(master_df)}")

    else:
        print("No data extracted. Check your API key as it expires within 7 days!")