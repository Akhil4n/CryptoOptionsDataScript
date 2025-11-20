#todo: clean data for missing vals, sort data based on some standard, exeption handling and logging for script
import requests
import configparser
import pandas as pd
import datetime as dt
import time
import json
import os
from re import I
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def upload_to_drive(file_path, folder_id, keep_last=7):
    """
    Upload file to Google Drive using OAuth credentials
    """
    try:
        # Load credentials from environment variable
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
        if not creds_json:
            print("Warning: GOOGLE_DRIVE_CREDENTIALS not found, skipping upload")
            return None
        
        creds_dict = json.loads(creds_json)
        credentials = Credentials(
            token=creds_dict.get('token'),
            refresh_token=creds_dict.get('refresh_token'),
            token_uri=creds_dict.get('token_uri'),
            client_id=creds_dict.get('client_id'),
            client_secret=creds_dict.get('client_secret'),
            scopes=creds_dict.get('scopes')
        )
        
        # Refresh token if expired
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        
        service = build('drive', 'v3', credentials=credentials)
        
        file_name = os.path.basename(file_path)
        
        # Get all BTC snapshot files, sorted by creation time (newest first)
        query = f"name contains 'BTC_snapshots_' and '{folder_id}' in parents and trashed=false"
        results = service.files().list(
            q=query, 
            fields="files(id, name, createdTime)",
            orderBy="createdTime desc"
        ).execute()
        items = results.get('files', [])
        
        # Delete files beyond the keep limit
        # We keep (keep_last - 1) because we're about to upload a new one
        files_to_delete = items[keep_last-1:] if len(items) >= keep_last else []
        
        for item in files_to_delete:
            service.files().delete(fileId=item['id']).execute()
            print(f"✓ Deleted old file: {item['name']}")
        
        # Upload new file
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink'
        ).execute()
        
        print(f"✓ File uploaded to Google Drive: {file.get('name')}")
        print(f"✓ View link: {file.get('webViewLink')}")
        
        return file.get('id')
    
    except Exception as e:
        print(f"✗ Error uploading to Google Drive: {e}")
        import traceback
        traceback.print_exc()
        return None

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    OUTPUT_DIR = os.path.join(BASE_DIR, "output")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Making AlpacaAPI call for BTC options data
    API_KEY = os.environ.get('ALPACA_API_KEY', '').strip()
    API_SECRET = os.environ.get('ALPACA_API_SECRET', '').strip()

    if not API_KEY or not API_SECRET:
        print("Environment variables not found, attempting to read from config file...")
        CONFIG_PATH = os.path.join(BASE_DIR, "Alpaca.cfg")
        
        if not os.path.exists(CONFIG_PATH):
            raise ValueError(
                "API credentials not found. Please either:\n"
                "1. Set ALPACA_API_KEY and ALPACA_API_SECRET environment variables, or\n"
                "2. Create Alpaca.cfg file with credentials"
            )
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH)
        
        API_KEY = config['alpaca']['APCA_API_KEY_ID'].strip()
        API_SECRET = config['alpaca']['APCA_API_SECRET_KEY'].strip()
        print("✓ Loaded credentials from config file")
    else:
        print("✓ Loaded credentials from environment variables")

    name = "BTC"

    url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{name}?feed=indicative&limit=1000"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET
    }

    def safe_get(url, headers, attempts=5):
        for i in range(attempts):
            try:
                return requests.get(url, headers=headers, timeout=10)
            except Exception as e:
                print(f"Attempt {i+1} failed: {e}")
                time.sleep(5)
        raise Exception("All retries failed.")

    response = safe_get(url, headers)

    data = json.loads(response.text)

    if 'snapshots' not in data:
        print("No snapshots returned:", data)
        exit(1)

    opt_data = []

    # extract features
    for symbol, details in data['snapshots'].items():
        opt_row = {'symbol' : symbol}

        if 'impliedVolatility' in details:
            opt_row['impliedVolatility'] = details['impliedVolatility']

        if 'greeks' in details:
            for k, v in details['greeks'].items():
                opt_row[f'greeks_{k}'] = v

        if 'dailyBar' in details:
            for k, v in details['dailyBar'].items():
                opt_row[f'dailyBar_{k}'] = v

        if 'latestQuote' in details:
            for k, v in details['latestQuote'].items():
                opt_row[f'latestQuote_{k}'] = v

        if 'latestTrade' in details:
            for k, v in details['latestTrade'].items():
                opt_row[f'latestTrade_{k}'] = v

        if 'minuteBar' in details:
            for k, v in details['minuteBar'].items():
                opt_row[f'minuteBar_{k}'] = v

        if 'prevDailyBar' in details:
            for k, v in details['prevDailyBar'].items():
                opt_row[f'prevDailyBar_{k}'] = v

        date_start = len(name)
        price_start = len(name) + 7

        exp_year = int("20" + symbol[date_start : date_start + 2])
        exp_month = int(symbol[date_start + 2 : date_start + 4])
        exp_day = int(symbol[date_start + 4 : date_start + 6])

        exp_date = dt.date(exp_year, exp_month, exp_day)
        opt_row['expires'] = exp_date
        opt_row['price'] = int(symbol[price_start :])

        opt_data.append(opt_row)

    df = pd.DataFrame(opt_data)

    # Save to CSV
    timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    prefix = f"{name}_snapshots"

    for f in os.listdir(OUTPUT_DIR):
        if f.startswith(prefix):
            os.remove(os.path.join(OUTPUT_DIR, f))

    csv_path = os.path.join(OUTPUT_DIR, f"{name}_snapshots_{timestamp}.csv")
    df.to_csv(csv_path, index=False)
    print(f"✓ Saved snapshots to: {csv_path}")
    print(f"✓ Total options captured: {len(df)}")
    
    # Upload to Google Drive
    folder_id = os.environ.get('GOOGLE_DRIVE_FOLDER_ID')
    if folder_id:
        upload_to_drive(csv_path, folder_id)
    else:
        print("Warning: GOOGLE_DRIVE_FOLDER_ID not set, skipping Google Drive upload")
    
except Exception as e:
    print(f"✗ Error {e} has occurred.")
    import traceback
    traceback.print_exc()
    exit(1)