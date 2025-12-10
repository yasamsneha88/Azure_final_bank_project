import json
import logging
import azure.functions as func

def main(event: func.EventGridEvent, outputQueueItem: func.Out[str]):
    logging.info("Event Grid Trigger Fired")

    try:
        data = event.get_json()
        logging.info(f"Event received: {json.dumps(data)}")

        blob_url = None

        # Event Grid body format
        if isinstance(data, dict):
            blob_url = data.get("url")
            if not blob_url and "data" in data and isinstance(data["data"], dict):
                blob_url = data["data"].get("url")

        if not blob_url:
            logging.error("❌ Blob URL not found in event trigger body")
            return

        logging.info(f"Blob URL extracted: {blob_url}")

        message = {
            "blob_url": blob_url
        }

        outputQueueItem.set(json.dumps(message))
        logging.info("Successfully pushed to Service Bus Queue.")

    except Exception as ex:
        logging.exception(f"Error processing event grid message: {ex}")