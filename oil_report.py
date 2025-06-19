import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re # Import regular expression module

# Assuming config.py is in the same directory and contains these variables
from config import EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_SENDER, EMAIL_RECEIVER, EIA_API_KEY

# --- Configuration ---
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

# --- Helper Functions ---
def get_past_dates(days=3):
    today = datetime.today()
    # This function returns the last 'days' calendar dates.
    # The output from your previous run (2025-06-14, 2025-06-15, 2025-06-16) indicates
    # your execution environment or a custom `get_past_business_dates` function is
    # already determining the appropriate business days.
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in reversed(range(days))]

def fetch_crude_price(series_id, api_key, dates_for_report):
    # Fetch data for a wider range to ensure we capture the last few available data points,
    # as EIA API might not be updated daily or on weekends/holidays.
    today = datetime.today()
    # Go back a reasonable number of days to find at least 3 business days of data
    lookback_days = 30 # Fetch data for the last 30 calendar days
    start_date_fetch = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date_fetch = today.strftime("%Y-%m-%d") # Fetch up to today

    url = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={api_key}&frequency=daily&data[0]=value&facets[series][]={series_id}&start={start_date_fetch}&end={end_date_fetch}&sort[0][column]=period&sort[0][direction]=asc"
    response = requests.get(url)
    
    # Initialize prices for the report's dates with N/A
    prices_for_report = {date: "N/A" for date in dates_for_report}

    if response.status_code == 200:
        data = response.json().get("response", {}).get("data", [])
        if data:
            df = pd.DataFrame(data)
            if 'period' in df.columns and 'value' in df.columns:
                df['date'] = pd.to_datetime(df['period']).dt.strftime('%Y-%m-%d')
                df.set_index('date', inplace=True)
                
                # Convert 'value' column to numeric, coercing errors to NaN
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                
                # Filter for valid numeric values and get the latest 3
                available_prices_df = df[df['value'].notna()]
                latest_available_prices = available_prices_df['value'].tail(len(dates_for_report)).tolist() # Get values as list

                # Assign the latest available prices to the report's dates in order
                for i, date_str in enumerate(dates_for_report):
                    if i < len(latest_available_prices):
                        prices_for_report[date_str] = round(latest_available_prices[i], 2)
            else:
                print(f"Unexpected data structure for {series_id}: {df.columns}. Expected 'period' and 'value' columns.")
        else:
            print(f"No data returned from EIA API for {series_id} in the last {lookback_days} days.")
    else:
        print(f"Failed to fetch EIA data for {series_id}: {response.status_code} - {response.text}")
    
    return prices_for_report

def fetch_singapore_bunker_prices():
    bunker_data = {
        "VLSFO": {},
        "LSMGO": {},
        "HSFO": {},
    }
    dates = get_past_dates(days=3) # Get the last 3 calendar dates
    for fuel_type in bunker_data.keys():
        for date in dates:
            bunker_data[fuel_type][date] = "N/A" # Initialize with N/A

    url = "https://integr8fuels.com/bunkering-ports/bunkering-singapore/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    } # Added User-Agent and other headers

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Raise an exception for bad status codes (like 403)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the main content area or section where prices are listed
        content_area = soup.find('div', class_='page-content') # This class was an assumption; adjust if necessary
        if not content_area:
            # Fallback to body if main content div not found
            content_area = soup.body if soup.body else soup
            print("Warning: Could not find 'page-content' div. Searching entire body.")

        # Extract prices for each fuel type using regex
        patterns = {
            "VLSFO": r"VLSFO\.\s*\$US\/MT\.\s*\$(\d+\.?\d*)",
            "LSMGO": r"LSMGO\.\s*\$US\/MT\.\s*\$(\d+\.?\d*)",
            "HSFO": r"HSFO\.\s*\$US\/MT\.\s*\$(\d+\.?\d*)",
        }

        found_prices = {}
        for fuel, pattern in patterns.items():
            # Search within the text of the content_area
            match = re.search(pattern, content_area.get_text())
            if match:
                found_prices[fuel] = float(match.group(1))

        # Update bunker_data with the latest fetched prices for the most recent date
        # Integr8 Fuels typically shows only current prices, so apply to the latest date
        latest_date = dates[-1] 
        for fuel_type, price in found_prices.items():
            bunker_data[fuel_type][latest_date] = f"{price:.2f}"

        if not found_prices:
            print("No prices extracted from Integr8 Fuels. The page structure might have changed.")

        print("✅ Successfully fetched bunker prices from Integr8 Fuels.")
        return bunker_data

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch bunker prices from Integr8 Fuels: {e}")
        # Only print specific table error if it's not a 403 or other request error
        if not isinstance(e, requests.exceptions.HTTPError) or e.response.status_code != 403:
             print("Could not find the main prices table on Integr8 Fuels (due to parsing issue).")
        return bunker_data


def format_row(label, values):
    return f"{label:<30} | {' | '.join(str(v).ljust(8) for v in values)}"

def send_email(subject, body):
    message = MIMEMultipart()
    message['From'] = EMAIL_SENDER
    message['To'] = EMAIL_RECEIVER
    message['Subject'] = subject

    message.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, message.as_string())
        print("✅ Email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

def main():
    dates = get_past_dates() # This will return calendar dates, adjust if business days needed
    
    # Fetch crude prices
    wti_prices = fetch_crude_price("RWTCL", EIA_API_KEY, dates)
    brent_prices = fetch_crude_price("RBRTE", EIA_API_KEY, dates)

    # Fetch bunker prices from Integr8 Fuels
    singapore_bunker_prices = fetch_singapore_bunker_prices()

    # Prepare report body
    header = f"{'':<30} | {dates[0]} | {dates[1]} | {dates[2]}"
    lines = [
        header,
        "-" * len(header),
        format_row("WTI crude price", [wti_prices[d] for d in dates]),
        format_row("Brent crude price", [brent_prices[d] for d in dates]),
        format_row("Singapore VLSFO bunker price", [singapore_bunker_prices['VLSFO'].get(d, 'N/A') for d in dates]),
        format_row("Singapore LSMGO bunker price", [singapore_bunker_prices['LSMGO'].get(d, 'N/A') for d in dates]),
        format_row("Singapore HSFO bunker price", [singapore_bunker_prices['HSFO'].get(d, 'N/A') for d in dates]),
    ]
    report_body = "\n".join(lines)

    subject = "Daily Oil and Bunker Price Report"
    send_email(subject, report_body)

    print(report_body) # Print the report to console as well

if __name__ == "__main__":
    main()