import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from config import EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_SENDER, EMAIL_RECEIVER, EIA_API_KEY

SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

def fetch_crude_price(series_id, api_key, dates):
    # EIA API documentation often specifies 'data' as a list of fields to retrieve, and 'facets' for filtering.
    # The 'series' facet is used to specify the series ID.
    # The date format for start and end dates should match 'YYYY-MM-DD'.
    url = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={api_key}&frequency=daily&data[0]=value&facets[series][]={series_id}&start={dates[0]}&end={dates[-1]}&sort[0][column]=period&sort[0][direction]=asc"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch EIA data for {series_id}: {response.status_code} - {response.text}")
        return {date: "N/A" for date in dates}

    data = response.json().get("response", {}).get("data", [])
    if not data:
        print(f"No data returned from EIA API for {series_id}.")
        return {date: "N/A" for date in dates}

    df = pd.DataFrame(data)

    # The EIA API typically returns a 'period' column for dates
    if 'period' in df.columns and 'value' in df.columns:
        df['date'] = pd.to_datetime(df['period'])
        df.set_index(df['date'].dt.strftime('%Y-%m-%d'), inplace=True)
        # Create a dictionary of prices, handling missing dates by setting them to 'N/A'
        prices = {date: "N/A" for date in dates}
        for date_str in dates:
            if date_str in df.index:
                prices[date_str] = round(df.loc[date_str, 'value'], 2)
        return prices
    else:
        print(f"Unexpected data structure for {series_id}: {df.columns}. Expected 'period' and 'value' columns.")
        return {date: "N/A" for date in dates}

def fetch_bunker_price_stub(dates):
    # Replace this with a real bunker API or scraper
    return {fuel: {date: "N/A" for date in dates} for fuel in ['VLSFO', 'LSMGO', 'HSFO']}

def build_email_content(wti, brent, bunker_prices, dates):
    header = "WTI crude price | Brent crude price | Singapore VLSFO | LSMGO | HSFO\n"
    lines = [header]
    for date in dates:
        line = f"{wti[date]} | {brent[date]} | {bunker_prices['VLSFO'][date]} | {bunker_prices['LSMGO'][date]} | {bunker_prices['HSFO'][date]}"
        lines.append(f"{date}: {line}")
    return "\n".join(lines)

def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.send_message(msg)
        print("✅ Email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

def main():
    today = datetime.today()
    dates = [
        (today - timedelta(days=2)).strftime('%Y-%m-%d'),
        (today - timedelta(days=1)).strftime('%Y-%m-%d'),
        today.strftime('%Y-%m-%d')
    ]

    print("📈 Fetching WTI prices...")
    wti = fetch_crude_price('PET.RWTC.D', EIA_API_KEY, dates)

    print("📈 Fetching Brent prices...")
    brent = fetch_crude_price('PET.RBRTE.D', EIA_API_KEY, dates)

    print("📈 Fetching bunker prices (stub)...")
    bunker_prices = fetch_bunker_price_stub(dates)

    content = build_email_content(wti, brent, bunker_prices, dates)
    print("📧 Email content:\n", content)

    send_email("Daily Oil and Bunker Prices Report", content)

if __name__ == "__main__":
    main()
