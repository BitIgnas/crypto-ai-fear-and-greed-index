import logging
import time
import json
from datetime import datetime

import requests
from google.cloud import logging_v2 as cloud_logging
from google.cloud import storage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

def setup_cloud_logging():
    client = cloud_logging.Client.from_service_account_json("spheric-crow-446318-f1-961938ba117e.json")
    logger = client.logger("dynamic_dca_logs")  # Custom log name
    client.setup_logging()
    logging.info("Cloud logging initialized with spheric-crow key.")

def get_timestamp():
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

ALPHASQUARED_API_URL = "https://alphasquared.io/wp-json/as/v1/asset-info?symbol={symbol}"
ALPHASQUARED_API_KEY = "sGLPu6nNeCxMjCa1m0yEaC5ADwKr4abyQo2801Ne"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "thefightingfalcon65@gmail.com"
EMAIL_PASSWORD = "vych aeol owjh jggz"
EMAIL_RECIPIENT = "ignotas99@gmail.com"

LOG_BUCKET_NAME = "gcp_trading_orders"
LOG_FILE_NAME = "trading_logs.json"

MONTHLY_BUDGET = 1400
ASSET_ALLOCATION = {
    "BTC": 0.5,
    "SOL": 0.25,
    "LINK": 0.25,
}

RISK_THRESHOLDS = {
    "BTC": {
        "buy_0_10": 1253,
        "buy_10_20": 939,
        "buy_20_30": 626,
        "buy_30_40": 313,
        "hold": (40, 60),
        "sell_60_70": 0.1,
        "sell_70_80": 0.2,
        "sell_80_90": 0.3,
        "sell_90_100": 0.4,
    },
    "SOL": {
        "buy_0_10": 626,
        "buy_10_20": 470,
        "buy_20_30": 313,
        "buy_30_40": 157,
        "hold": (40, 60),
        "sell_60_70": 0.1,
        "sell_70_80": 0.2,
        "sell_80_90": 0.3,
        "sell_90_100": 0.4,
    },
    "LINK": {
        "buy_0_10": 626,
        "buy_10_20": 470,
        "buy_20_30": 313,
        "buy_30_40": 157,
        "hold": (40, 60),
        "sell_60_70": 0.1,
        "sell_70_80": 0.2,
        "sell_80_90": 0.3,
        "sell_90_100": 0.4,
    },
}

SAVE_FILE = "trading_state.json"
CHECK_INTERVAL = 3600  # 1 hour
WEEKLY_EMAIL_INTERVAL = 7 * 24 * 3600  # 7 days

import csv
import io

RESET_CSV_FILE = "monthly_reset_status.csv"

def load_reset_csv():
    """Loads the CSV file from the bucket or initializes it if not found."""
    try:
        client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
        bucket = client.bucket(LOG_BUCKET_NAME)
        blob = bucket.blob(RESET_CSV_FILE)

        if not blob.exists():
            logging.info(f"|--- {RESET_CSV_FILE} not found in bucket. Initializing a new CSV. ---|")
            return []

        data = blob.download_as_text()
        csv_reader = csv.DictReader(io.StringIO(data))
        return list(csv_reader)
    except Exception as e:
        logging.info(f"|--- Error loading reset CSV: {e}. Returning empty data. ---|")
        return []


def load_portfolio_json():
    portfolio_file = "portfolio.json"

    try:
        client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
        bucket = client.bucket(LOG_BUCKET_NAME)
        portfolio_blob = bucket.blob(portfolio_file)

        # Check if the portfolio file exists in the bucket
        if portfolio_blob.exists():
            logging.info(f"|--- Loading portfolio from GCS bucket ---|")
            portfolio_data = portfolio_blob.download_as_text()
            portfolio = json.loads(portfolio_data)
        else:
            # Create a new portfolio file if it doesn't exist
            logging.info(f"|--- {portfolio_file} not found in bucket. Creating a new portfolio file. ---|")
            portfolio = {}
            portfolio_blob.upload_from_string(json.dumps(portfolio))

        logging.info(f"|--- Portfolio loaded successfully ---|")
        return portfolio

    except Exception as e:
        logging.info(f"|--- Error loading portfolio JSON: {e}. Initializing default portfolio. ---|")
        return {}

def save_portfolio_json(portfolio):
    portfolio_file = "portfolio.json"

    try:
        client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
        bucket = client.bucket(LOG_BUCKET_NAME)
        portfolio_blob = bucket.blob(portfolio_file)

        # Upload updated portfolio to GCS
        portfolio_blob.upload_from_string(json.dumps(portfolio, indent=4))
        logging.info(f"|--- Portfolio saved successfully to {portfolio_file} ---|")
    except Exception as e:
        logging.info(f"|--- Error saving portfolio JSON: {e} ---|")

def update_portfolio_json(portfolio, symbol, operation, quantity, price):
    try:
        if symbol not in portfolio:
            portfolio[symbol] = {
                "quantity": 0,
                "total_invested": 0,
                "average_price": 0
            }

        current_data = portfolio[symbol]

        if operation == "buy":
            new_total_invested = current_data["total_invested"] + (quantity * price)
            new_quantity = current_data["quantity"] + quantity
            new_average_price = new_total_invested / new_quantity

            portfolio[symbol].update({
                "quantity": new_quantity,
                "total_invested": new_total_invested,
                "average_price": new_average_price
            })

        elif operation == "sell":
            new_quantity = current_data["quantity"] - quantity
            if new_quantity < 0:
                raise ValueError(f"Cannot sell more than available holdings for {symbol}")

            new_total_invested = new_quantity * current_data["average_price"]

            portfolio[symbol].update({
                "quantity": new_quantity,
                "total_invested": new_total_invested,
                "average_price": current_data["average_price"]
            })

        logging.info(f"|--- Portfolio updated successfully for {symbol}: {operation} ---|")
        return portfolio

    except Exception as e:
        logging.info(f"|--- Error updating portfolio: {e} ---|")
        return portfolio

def save_reset_csv(data):
    """Saves the updated CSV data back to the bucket."""
    try:
        client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
        bucket = client.bucket(LOG_BUCKET_NAME)
        blob = bucket.blob(RESET_CSV_FILE)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["Month", "Reset", "AveragePortfolioSize"])
        writer.writeheader()
        writer.writerows(data)

        blob.upload_from_string(output.getvalue())
        logging.info(f"|--- Reset CSV updated successfully. ---|")
    except Exception as e:
        logging.info(f"|--- Error saving reset CSV: {e}. ---|")

def check_and_update_reset_csv(state):
    current_month = datetime.now().strftime("%Y-%m")
    csv_data = load_reset_csv()

    for row in csv_data:
        if row["Month"] == current_month and row["Reset"] == "True":
            logging.info(f"|--- Reset already performed for {current_month}. ---|")
            return True

    for row in csv_data:
        row["Reset"] = "False"

    # Calculate average portfolio size for this month
    average_portfolio_size = sum(state["savings"].values()) / len(state["savings"])
    csv_data.append({
        "Month": current_month,
        "Reset": "True",
        "AveragePortfolioSize": round(average_portfolio_size, 2)
    })

    save_reset_csv(csv_data)
    logging.info(f"|--- Monthly reset status updated for {current_month}. ---|")
    return False

# Load saved state dynamically from transaction logs
def load_state():
    try:
        state = {
            "savings": {"BTC": 0, "SOL": 0, "LINK": 0},
            "bought_zones": {"BTC": [], "SOL": [], "LINK": []},
            "sold_zones": {"BTC": [], "SOL": [], "LINK": []},
            "monthly_funds": MONTHLY_BUDGET,
            "last_reset": time.time(),
            "last_email": time.time(),
        }

        client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
        bucket = client.bucket(LOG_BUCKET_NAME)
        state_blob = bucket.blob(SAVE_FILE)

        if not state_blob.exists():
            logging.info(f"|--- {SAVE_FILE} not found in bucket. Initializing default state. ---|")
            state_blob.upload_from_string(json.dumps(state))

        state_data = json.loads(state_blob.download_as_text())
        logging.info(f"|--- Loaded state from GCS: {state_data} ---|")
        return state_data

    except Exception as e:
        logging.info(f"|--- Error loading state: {e}. Initializing default state locally. ---|")
        return {
            "savings": {"BTC": 0, "SOL": 0, "LINK": 0},
            "bought_zones": {"BTC": [], "SOL": [], "LINK": []},
            "sold_zones": {"BTC": [], "SOL": [], "LINK": []},
            "monthly_funds": MONTHLY_BUDGET,
            "last_reset": time.time(),
            "last_email": time.time(),
        }

def send_email(subject, body):
    logging.info(f"|--- Sending email with subject: {subject} ---|")
    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_RECIPIENT
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info(f"|--- Email sent successfully ---|")
    except Exception as e:
        logging.info(f"|--- Error sending email: {e} ---|")

# Save state
def save_state(state):
    try:
        with open(SAVE_FILE, "w") as f:
            json.dump(state, f)

        client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
        bucket = client.bucket(LOG_BUCKET_NAME)
        blob = bucket.blob(SAVE_FILE)
        blob.upload_from_filename(SAVE_FILE)
        logging.info(f"|--- State uploaded to GCS successfully ---|")
    except Exception as e:
        logging.info(f"|--- Error saving state: {e} ---|")

# Fetch risk index
def get_risk_index(symbol):
    try:
        logging.info(f"|--- Fetching risk index for {symbol} from AlphaSquared API ---|")
        response = requests.get(ALPHASQUARED_API_URL.format(symbol=symbol), headers={"Authorization": ALPHASQUARED_API_KEY})
        response.raise_for_status()
        data = response.json()
        risk_index = round(float(data["current_risk"]), 1)
        logging.info(f"|--- Current Risk Index for {symbol}: {risk_index} ---|")
        return risk_index
    except Exception as e:
        logging.info(f"|--- Error fetching risk index for {symbol}: {e} ---|")
        return None

#|------------------ Main flow ------------------|
def dynamic_dca():
    logging.info(f"|--- Starting dynamic DCA process ---|")
    state = load_state()
    logging.info(f"|--- Loaded state: {state} ---|")

    try:
        portfolio = load_portfolio_json()
    except Exception as e:
        logging.info(f"|--- Error loading portfolio JSON: {e}. Initializing default portfolio. ---|")
        portfolio = {}

    # Check and perform monthly reset for buys
    if not check_and_update_reset_csv(state):
        logging.info(f"|--- Performing monthly reset for the new month. ---|")

        # Backup current state
        try:
            client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
            bucket = client.bucket(LOG_BUCKET_NAME)
            backup_blob = bucket.blob(f"trading_state_{datetime.now().strftime('%Y_%m')}.json")
            backup_blob.upload_from_string(json.dumps(state))
            logging.info(f"|--- Monthly state backup created successfully. ---|")
        except Exception as e:
            logging.info(f"|--- Error creating monthly state backup: {e} ---|")

        # Reset buy-related state (leave sell-related state as-is)
        state["bought_zones"] = {symbol: [] for symbol in state["bought_zones"]}
        state["monthly_funds"] = MONTHLY_BUDGET
        state["last_reset"] = time.time()
        save_state(state)

        logging.info(f"|--- Monthly reset completed for {datetime.now().strftime('%Y-%m')}. ---|")
        return

    if time.time() - state["last_email"] >= WEEKLY_EMAIL_INTERVAL:
        try:
            generate_weekly_report(state)
            state["last_email"] = time.time()  # Update last email timestamp
            logging.info("|--- Weekly email report sent successfully ---|")
        except Exception as e:
            logging.info(f"|--- Error generating weekly report: {e} ---|")

    for symbol, allocation in ASSET_ALLOCATION.items():
        risk_index = get_risk_index(symbol)
        monthly_funds = state["monthly_funds"] * allocation
        savings = state["savings"][symbol]
        bought_zones = state["bought_zones"][symbol]
        sold_zones = state["sold_zones"][symbol]

        if risk_index is None:
            continue

        thresholds = RISK_THRESHOLDS[symbol]
        current_price = get_asset_price(symbol)

        # Handle buying logic
        if risk_index < thresholds["hold"][0]:
            eligible_zone = None
            for range_key, amount in thresholds.items():
                if "buy" in range_key:
                    range_min, range_max = map(int, range_key.split("_")[1:])
                    if range_min <= risk_index < range_max:
                        eligible_zone = range_key
                        break

            if eligible_zone and eligible_zone not in bought_zones:
                lowest_bought_zone = min(
                    [int(zone.split("_")[1]) for zone in bought_zones], default=101
                )
                if risk_index < lowest_bought_zone:
                    to_spend = min(thresholds[eligible_zone], savings + monthly_funds)
                    if to_spend > 0:
                        quantity_to_buy = to_spend / current_price

                        logging.info(f"|--- Buying {symbol}: Spending {to_spend} EUR in range {eligible_zone} ---|")
                        bought_zones.append(eligible_zone)
                        state["savings"][symbol] -= max(0, to_spend - monthly_funds)
                        state["monthly_funds"] -= to_spend

                        # Update portfolio
                        portfolio = update_portfolio_json(
                            portfolio, symbol, "buy", quantity_to_buy, current_price
                        )

                        # Reset selling zones when a buy occurs
                        state["sold_zones"][symbol] = []

                        log_action("buy", {"symbol": symbol, "risk_index": risk_index, "amount": to_spend})

                        send_email(
                            subject=f"Buy Alert for {symbol}",
                            body=f"Buying {symbol} in range {eligible_zone} for {to_spend} EUR.\n"
                                 f"Risk Index: {risk_index}, Quantity: {quantity_to_buy:.6f}, Total Holdings: {portfolio[symbol]['quantity']:.6f}."
                        )
                else:
                    logging.info(
                        f"|--- Cannot buy {symbol} '{eligible_zone}' zone, since {lowest_bought_zone} higher zone was already bought --|")
            else:
                logging.info(f"|--- Cannot buy {symbol} '{eligible_zone}', zone was already bought ---|")

        # Handle selling logic
        if risk_index > thresholds["hold"][1]:
            eligible_zone = None
            for range_key, percentage in thresholds.items():
                if "sell" in range_key:
                    range_min, range_max = map(int, range_key.split("_")[1:])
                    if range_min <= risk_index < range_max:
                        eligible_zone = range_key
                        break

            if eligible_zone and eligible_zone not in sold_zones:
                highest_sold_zone = max(
                    [int(zone.split("_")[1]) for zone in sold_zones], default=-1
                )
                if range_min > highest_sold_zone:
                    total_holdings = portfolio.get(symbol, {}).get("quantity", 0)
                    if total_holdings == 0:
                        logging.info(f"|--- No holdings to sell for {symbol}. ---|")
                        continue

                    quantity_to_sell = total_holdings * percentage

                    logging.info(f"|--- Selling {symbol}: {percentage * 100}% in range '{eligible_zone}' zone ---|")
                    sold_zones.append(eligible_zone)

                    # Update portfolio
                    portfolio = update_portfolio_json(
                        portfolio, symbol, "sell", quantity_to_sell, current_price
                    )

                    # Add sold amount to savings
                    sale_revenue = quantity_to_sell * current_price
                    state["savings"][symbol] += sale_revenue

                    log_action("sell",
                               {"symbol": symbol, "risk_index": risk_index, "percentage": percentage, "revenue": sale_revenue})
                    send_email(
                        subject=f"Sell Alert for {symbol}",
                        body=f"Selling {percentage * 100}% of holdings for {symbol} in range {eligible_zone}.\n"
                             f"Risk Index: {risk_index}, Quantity: {quantity_to_sell:.6f}, Remaining Holdings: {portfolio[symbol]['quantity']:.6f}.\n"
                             f"Revenue: {sale_revenue:.2f} EUR"
                    )
                else:
                    logging.info(f"|--- Cannot sell in {symbol} '{eligible_zone}' zone, as higher zone {highest_sold_zone} already sold. ---|")
            else:
                logging.info(f"|--- Cannot sell in {symbol} '{eligible_zone}', zone was already sold. ---|")

    # Save updated portfolio back to GCS
    save_portfolio_json(portfolio)

    save_state(state)

def get_asset_price(symbol):
    try:
        pair = f"{symbol.lower()}/eur"
        url = f"https://api.kraken.com/0/public/Ticker?pair={pair.replace('/', '%2F')}"
        response = requests.get(url, headers={"Accept": "application/json"})
        response.raise_for_status()
        data = response.json()

        # Extract price from API response
        if "result" in data:
            ticker_info = list(data["result"].values())[0]
            price = float(ticker_info["a"][0])
            logging.info(f"|--- Fetched price for {symbol}: {price} EUR ---|")
            return price
        else:
            logging.warning(f"|--- Price data not found for {symbol} ---|")
            return None
    except Exception as e:
        logging.error(f"|--- Error fetching price for {symbol}: {e} ---|")
        return None


# Log actions to GCS
def log_action(action, details):
    try:
        logging.info(f"|--- Logging action: {action}, details: {details} ---|")
        client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
        bucket = client.bucket(LOG_BUCKET_NAME)
        blob = bucket.blob(LOG_FILE_NAME)

        try:
            log_data = json.loads(blob.download_as_text())
        except Exception:
            log_data = []

        log_entry = {
            "action": action,
            "details": details,
            "timestamp": get_timestamp(),
        }
        log_data.append(log_entry)

        blob.upload_from_string(json.dumps(log_data, indent=4))
        logging.info(f"|--- Action logged successfully ---|")
    except Exception as e:
        logging.info(f"|--- Error logging action: {e} ---|")

def generate_weekly_report(state):
    logging.info(f"|--- Generating weekly report ---|")
    client = storage.Client.from_service_account_json("spheric-crow-446318-f1-1ae5e2dcc51b.json")
    bucket = client.bucket(LOG_BUCKET_NAME)
    blob = bucket.blob(LOG_FILE_NAME)

    try:
        log_data = json.loads(blob.download_as_text()) if blob.exists() else []
    except Exception as e:
        logging.info(f"|--- Error fetching log data for weekly report: {e} ---|")
        log_data = []

    report_sections = []
    total_spent = {}
    total_earned = {}

    for symbol in ASSET_ALLOCATION.keys():
        # Calculate total spent and earned for each symbol
        total_spent[symbol] = sum(
            entry["details"].get("amount", 0) for entry in log_data if entry["action"] == "buy" and entry["details"].get("symbol") == symbol
        )
        total_earned[symbol] = sum(
            entry["details"].get("percentage", 0) for entry in log_data if entry["action"] == "sell" and entry["details"].get("symbol") == symbol
        )
        latest_risk = get_risk_index(symbol)

        # Create section for each symbol
        report_sections.append(
            f"### {symbol} Weekly Summary ###\n"
            f"Total Spent: {total_spent[symbol]:.2f} EUR\n"
            f"Total Earned from Sales: {total_earned[symbol]:.2f} EUR\n"
            f"Latest Risk Index: {latest_risk}\n"
        )

    # Format email content
    email_body = (
        "Here is your weekly trading report:\n\n"
        + "\n".join(report_sections)
        + f"\nCurrent Savings: {json.dumps(state['savings'], indent=2)}\n"
        f"Last Reset: {datetime.fromtimestamp(state['last_reset']).strftime('%Y-%m-%d %H:%M:%S')}\n"
    )

    # Send the email
    try:
        send_email(
            subject="Weekly Trading Report",
            body=email_body
        )
        logging.info(f"|--- Weekly report email sent successfully ---|")
    except Exception as e:
        logging.info(f"|--- Error sending weekly report email: {e} ---|")

# Main loop
def main():
    setup_cloud_logging()
    while True:
        try:
            logging.info(f"|--- Starting main loop ---|")
            dynamic_dca()
        except Exception as e:
            logging.info(f"|--- Error: {e} ---|")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
