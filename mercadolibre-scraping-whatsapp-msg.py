import requests
import sqlite3
import time
import schedule
import logging
from twilio.rest import Client

# --- Configure Logging ---
logging.basicConfig(
    level=logging.DEBUG,  # Change to logging.INFO to reduce verbosity
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Twilio Credentials (update with your actual values) ---
account_sid = ''
auth_token = ''
twilio_client = Client(account_sid, auth_token)
twilio_from = ''      # Your Twilio WhatsApp sender number
twilio_to   = ''      # Your (or your group's) WhatsApp number

# --- List of search terms ---
lista_busquedas = ['RTX Usado', 'GTX Usado']

# --- Database file name ---
DB_FILE = "offers.db"

def init_db():
    """
    Initialize the SQLite database and create the offers table if it does not exist.
    """
    logging.debug("Initializing database...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS offers (
            id TEXT PRIMARY KEY,
            title TEXT,
            price REAL,
            permalink TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logging.debug("Database initialized.")

def fetch_offers(query):
    """
    Query the MercadoLibre API for the given search term using pagination
    and return a list of unique offers.
    """
    all_offers = {}
    # Iterate over offsets from 0 to 1000 (inclusive) in steps of 50.
    for offset in range(0, 1001, 50):
        url = f"https://api.mercadolibre.com/sites/MLU/search?q={query}&offset={offset}"
        logging.debug("Fetching offers for query '%s' with offset %d: %s", query, offset, url)
        try:
            response = requests.get(url)
            data = response.json()
            results = data.get("results", [])
            logging.debug("Query '%s', offset %d: Fetched %d offers.", query, offset, len(results))
            if not results:
                logging.debug("No offers returned for query '%s' at offset %d. Ending pagination.", query, offset)
                break
            for item in results:
                item_id = item.get("id")
                # Deduplicate: add if not already present.
                if item_id not in all_offers:
                    all_offers[item_id] = item
            # If fewer than 50 items were returned, likely there are no further pages.
            if len(results) < 50:
                logging.debug("Fewer than 50 results returned for query '%s'. Ending pagination.", query)
                break
        except Exception as e:
            logging.error("Error fetching offers for query '%s' at offset %d: %s", query, offset, e)
            break

    logging.info("Query '%s': Total unique offers found: %d", query, len(all_offers))
    return list(all_offers.values())

def get_existing_offer_ids():
    """
    Retrieve a set of offer IDs that are currently stored in the database.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM offers")
    rows = cursor.fetchall()
    conn.close()
    existing_ids = set(row[0] for row in rows)
    logging.debug("Existing offer IDs in DB: %s", existing_ids)
    return existing_ids

def add_offer_to_db(item):
    """
    Insert a new offer into the database.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO offers (id, title, price, permalink)
        VALUES (?, ?, ?, ?)
    ''', (item.get("id"), item.get("title"), item.get("price"), item.get("permalink")))
    conn.commit()
    conn.close()
    logging.debug("Offer %s added to database.", item.get("id"))

def remove_offer_from_db(offer_id):
    """
    Delete an offer from the database that is no longer available.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM offers WHERE id = ?", (offer_id,))
    conn.commit()
    conn.close()
    logging.debug("Offer %s removed from database.", offer_id)

def send_notification(new_offers):
    """
    Send a WhatsApp notification via Twilio listing all new offers.
    If the full message exceeds 1600 characters, split the message into parts
    (ensuring that an offer is not split across messages) and send them separately.
    """
    if not new_offers:
        logging.debug("No new offers to notify.")
        return

    # Build the complete message text
    message_text = "New 'Usado' offers found:\n"
    for item in new_offers:
        title = item.get("title")
        price = item.get("price")
        link = item.get("permalink")
        message_text += f"- {title} for ${price}\n{link}\n\n"

    try:
        # Try sending the message as a whole
        message = twilio_client.messages.create(
            from_=twilio_from,
            body=message_text,
            to=twilio_to
        )
        logging.info("Notification sent. SID: %s", message.sid)
    except Exception as e:
        error_message = str(e)
        # Check if the error is due to exceeding the 1600-character limit
        if "1600" in error_message:
            logging.warning("Message exceeds 1600 character limit. Splitting into multiple messages.")
            header = "New 'Usado' offers found:\n"
            messages = []
            current_message = header
            for item in new_offers:
                offer_text = f"- {item.get('title')} for ${item.get('price')}\n{item.get('permalink')}\n\n"
                # If adding this offer would exceed the limit, start a new message
                if len(current_message) + len(offer_text) > 1600:
                    messages.append(current_message)
                    current_message = offer_text
                else:
                    current_message += offer_text
            # Append any remaining text as the last message part
            if current_message:
                messages.append(current_message)

            # Send each message part separately
            for idx, msg in enumerate(messages, start=1):
                try:
                    part_message = twilio_client.messages.create(
                        from_=twilio_from,
                        body=msg,
                        to=twilio_to
                    )
                    logging.info("Notification part %d sent. SID: %s", idx, part_message.sid)
                except Exception as e2:
                    logging.error("Error sending notification part %d: %s", idx, e2)
        else:
            logging.error("Error sending notification: %s", e)

def job():
    """
    Main job function that iterates over all search terms, fetches offers,
    updates the database, and sends notifications for new offers.
    """
    logging.info("Job started...")
    all_current_offers = {}
    # Loop through each search term in the list and aggregate results.
    for query in lista_busquedas:
        offers = fetch_offers(query)
        logging.info("Query '%s' returned %d offers.", query, len(offers))
        for item in offers:
            all_current_offers[item.get("id")] = item  # Deduplicate offers by ID

    current_offers = list(all_current_offers.values())
    if not current_offers:
        logging.warning("No offers fetched for any query. Skipping this run.")
        return

    existing_offer_ids = get_existing_offer_ids()
    current_offer_ids = set(item.get("id") for item in current_offers)
    logging.debug("Current API offer IDs: %s", current_offer_ids)
    
    # Identify new offers (present in API results but not in the database)
    new_offers = [item for item in current_offers if item.get("id") not in existing_offer_ids]
    logging.info("%d new offers found.", len(new_offers))

    for item in new_offers:
        add_offer_to_db(item)
    
    # Identify offers that have disappeared (in DB but not in current API response)
    disappeared_offer_ids = existing_offer_ids - current_offer_ids
    logging.info("%d offers have disappeared.", len(disappeared_offer_ids))
    for offer_id in disappeared_offer_ids:
        remove_offer_from_db(offer_id)
    
    if new_offers:
        send_notification(new_offers)
    else:
        logging.info("No new offers to send notification for.")
    
    logging.info("Job finished.")

def main():
    """
    Initialize the database, run the job once, and schedule it to run every hour.
    """
    logging.info("Starting system...")
    init_db()  # Set up the SQLite database if it doesn't exist
    job()      # Run immediately at startup

    # Schedule the job to run every hour
    schedule.every(1).hours.do(job)
    logging.info("Scheduler started. The job will run every hour.")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Wait one minute between checks

if __name__ == "__main__":
    main()
