import logging
from telethon import TelegramClient
import csv
import os
import json
from dotenv import load_dotenv
import asyncio

# Set up logging
logging.basicConfig(
    filename='scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables once
load_dotenv('.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('phone')

# Load channels and comments from the JSON file
def load_channels_from_json(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get('channels', []), data.get('comments', [])
    except Exception as e:
        logging.error(f"Error reading channels from JSON: {e}")
        return [], []

# Load the last scraped message IDs from a JSON file
def load_last_scraped_ids(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading last scraped IDs: {e}")
            return {}
    return {}

# Save the last scraped message IDs to a JSON file
def save_last_scraped_ids(file_path, last_scraped_ids):
    try:
        with open(file_path, 'w') as f:
            json.dump(last_scraped_ids, f)
    except Exception as e:
        logging.error(f"Error saving last scraped IDs: {e}")

# Function to scrape data from a single channel, starting after the last scraped message
async def scrape_channel(client, channel_username, writer, media_dir, last_scraped_id):
    try:
        entity = await client.get_entity(channel_username)  # Get the channel entity
        channel_title = entity.title  # Get the channel's title

        # Start scraping messages from the most recent, stopping at last_scraped_id
        async for message in client.iter_messages(entity, offset_id=last_scraped_id, reverse=True):
            media_path = None
            if message.media:
                # If the message has media, download it and save it with a relevant filename
                filename = f"{channel_username}_{message.id}.{message.media.document.mime_type.split('/')[-1]}" if hasattr(message.media, 'document') else f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                await client.download_media(message.media, media_path)
                logging.info(f"Downloaded media for message ID {message.id}.")

            # Write the scraped data to CSV
            writer.writerow([channel_title, channel_username, message.id, message.message, message.date, media_path])
            logging.info(f"Processed message ID {message.id} from {channel_username}.")

            # Update last_scraped_id to the most recent message
            last_scraped_id = message.id

        return last_scraped_id  # Return the updated last scraped ID

    except Exception as e:
        logging.error(f"Error while scraping {channel_username}: {e}")
        return last_scraped_id  # Return the same ID in case of error

# Initialize the client with a session file
client = TelegramClient('scraping_session', api_id, api_hash)

async def main():
    try:
        await client.start(phone)  # Start the client
        logging.info("Client started successfully.")
        
        media_dir = 'photos'  # Directory to store downloaded media
        os.makedirs(media_dir, exist_ok=True)  # Create media directory if it doesn't exist

        # Load channels from JSON file
        channels, comments = load_channels_from_json('channels.json')

        # Load last scraped message IDs from a file
        last_scraped_ids = load_last_scraped_ids('last_scraped_ids.json')

        logging.info(f"Loaded channels: {channels}")

        for channel in channels:
            logging.info(f"Scraping data from channel: {channel}")
            
            # Create a CSV file for each channel
            csv_filename = f"{channel[1:]}_data.csv"  # Remove '@' from the channel name for the filename
            with open(csv_filename, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Channel Title', 'Channel Username', 'ID', 'Message', 'Date', 'Media Path'])

                # Get the last scraped message ID for this channel, or start from the newest message if none exists
                last_scraped_id = last_scraped_ids.get(channel, 0)

                # Scrape the channel starting after the last scraped message
                new_last_scraped_id = await scrape_channel(client, channel, writer, media_dir, last_scraped_id)

                # Update the last scraped ID for this channel
                last_scraped_ids[channel] = new_last_scraped_id

            # Add a delay between channel scraping to prevent hitting Telegram's rate limit
            await asyncio.sleep(5)  # 5 seconds delay

        # Save the updated last scraped message IDs to a file
        save_last_scraped_ids('last_scraped_ids.json', last_scraped_ids)

        # Log commented channels if any
        if comments:
            logging.info(f"Commented channels: {', '.join(comments)}")

    except Exception as e:
        logging.error(f"Error in main function: {e}")

if __name__ == "__main__":
    asyncio.run(main())
