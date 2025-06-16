import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from config import EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_SENDER, EMAIL_RECEIVER, EIA_API_KEY

# --- Configuration ---
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

# --- Helper Functions ---
def get_past_business_dates(days=3):
    today = datetime.today()
    business_dates = []
    current_date = today

    while len(business_dates) < days:
        # 0=Monday, 1=Tuesday, ..., 4=Friday, 5=Saturday, 6=Sunday
        if 0 <= current_date.weekday() <= 4: # Check if it's a weekday
            business_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date -= timedelta(days=1)
    
    return sorted(business_dates) # Return in ascending order for report

def fetch_crude_price(series_id, api_key, dates_for_report):
    # Fetch data for a wider range to ensure we capture the last few available data points,
    # as EIA API might not be updated daily or on weekends/holidays.
    today = datetime.today()
    # Go back a reasonable number of days to find at least 3 business days of data
    lookback_days = 30 # Fetch data for the last 30 calendar days to be safe
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
    url = "https://shipandbunker.com/prices"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    bunker_data = {
        "VLSFO": {d: "N/A" for d in get_past_business_dates(days=3)},
        "LSMGO": {d: "N/A" for d in get_past_business_dates(days=3)},
        "HSFO": {d: "N/A" for d in get_past_business_dates(days=3)},
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch Ship & Bunker data: {e}")
        return bunker_data # Return N/A for all if fetch fails

    price_table = None
    tables = soup.find_all('table')
    for table in tables:
        thead = table.find('thead')
        if thead:
            headers = [th.get_text(strip=True) for th in thead.find_all('th')]
            if "Port" in headers and "VLSFO $/mt" in headers:
                price_table = table
                break
        else:
            first_row_cells = table.find('tr')
            if first_row_cells:
                headers = [th.get_text(strip=True) for th in first_row_cells.find_all(['th', 'td'])]
                if "Port" in headers and "VLSFO $/mt" in headers:
                    price_table = table
                    break

    if not price_table:
        print("Could not find the main prices table on Ship & Bunker.")
        return bunker_data

    header_indices = {}
    header_row_cells = price_table.find('thead').find_all('th') if price_table.find('thead') else price_table.find('tr').find_all(['th', 'td'])
    for i, cell in enumerate(header_row_cells):
        header_text = cell.get_text(strip=True)
        if "VLSFO" in header_text:
            header_indices['VLSFO'] = i
        elif "MGO" in header_text:
            header_indices['LSMGO'] = i
        elif "IFO380" in header_text or "HSFO" in header_text:
            header_indices['HSFO'] = i
        elif "Port" in header_text:
            header_indices['Port'] = i

    if not all(key in header_indices for key in ['Port', 'VLSFO', 'LSMGO', 'HSFO']):
        print("Could not find all required price column headers.")
        return bunker_data

    singapore_row = None
    for row in price_table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        if cells and cells[header_indices['Port']].get_text(strip=True) == "Singapore":
            singapore_row = cells
            break

    if not singapore_row:
        print("Could not find Singapore row in the prices table.")
        return bunker_data

    latest_date_str = get_past_business_dates(days=1)[0]

    try:
        vlsfo_price_str = singapore_row[header_indices['VLSFO']].get_text(strip=True).replace(',', '')
        lsmgo_price_str = singapore_row[header_indices['LSMGO']].get_text(strip=True).replace(',', '')
        hsfo_price_str = singapore_row[header_indices['HSFO']].get_text(strip=True).replace(',', '')

        bunker_data["VLSFO"][latest_date_str] = float(vlsfo_price_str)
        bunker_data["LSMGO"][latest_date_str] = float(lsmgo_price_str)
        bunker_data["HSFO"][latest_date_str] = float(hsfo_price_str)

    except (ValueError, IndexError) as e:
        print(f"Error parsing bunker prices from Ship & Bunker for Singapore: {e}")
    
    return bunker_data


def format_row(label, values_dict, report_dates_ordered):
    formatted_values = []
    for d in report_dates_ordered:
        val = values_dict.get(d, 'N/A')
        formatted_values.append(f"{val:.2f}" if isinstance(val, (int, float)) else str(val))
    return f"{label:<35} | {' | '.join(f'{v:<6}' for v in formatted_values)}"


def format_email_content(dates, wti, brent, bunker):
    report_dates_ordered = sorted(dates)

    header = f"{'':<35} | {report_dates_ordered[0]:<6} | {report_dates_ordered[1]:<6} | {report_dates_ordered[2]:<6}"
    lines = [
        header,
        "-" * len(header),
        format_row("WTI crude price", wti, report_dates_ordered),
        format_row("Brent crude price", brent, report_dates_ordered),
        format_row("Singapore VLSFO bunker price", bunker['VLSFO'], report_dates_ordered),
        format_row("Singapore LSMGO bunker price", bunker['LSMGO'], report_dates_ordered),
        format_row("Singapore HSFO bunker price", bunker['HSFO'], report_dates_ordered),
    ]
    return "\n".join(lines)

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
    dates = get_past_business_dates(days=3)
    
    # Corrected EIA series ID for WTI crude from 'RWTC' to 'RWTCL'
    wti = fetch_crude_price('RWTCL', EIA_API_KEY, dates)
    brent = fetch_crude_price('RBRTE', EIA_API_KEY, dates)
    bunker = fetch_singapore_bunker_prices()

    email_body = format_email_content(dates, wti, brent, bunker)
    print(email_body)
    send_email("Daily Oil and Bunker Prices Report", email_body)

if __name__ == '__main__':
    main()