import os
import json
import logging
from datetime import datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
import pandas as pd
from io import StringIO


def classify_transaction(file_name):
    """Classify transaction based on filename"""
    file_name = file_name.lower()
    if "atm" in file_name:
        return "ATM"
    elif "upi" in file_name:
        return "UPI"
    elif "imps" in file_name:
        return "IMPS"
    elif "neft" in file_name:
        return "NEFT"
    return "UNKNOWN"


def detect_suspicious(txn):
    """Simple fraud detection rules"""
    flags = []

    try:
        amount = float(txn.get("Amount", 0))
    except:
        amount = 0

    txn_type = txn.get("txn_type", "")

    if amount > 50000:
        flags.append("High-value transaction")

    if txn_type == "ATM" and amount > 20000:
        flags.append("Large ATM withdrawal")

    if txn_type == "UPI" and amount > 50000:
        flags.append("Unusual UPI Transfer")

    return flags


def main(msg: func.ServiceBusMessage):
    logging.info("🔥 QueueProcessor started")

    payload = msg.get_body().decode()
    logging.info(f"➡ Received payload: {payload}")

    event = json.loads(payload)
    blob_url = event.get("blob_url")

    if not blob_url:
        logging.error("❌ No blob_url found in message")
        return

    # Read Environment configs
    storage_conn = os.getenv("AzureWebJobsStorage")
    cosmos_conn = os.getenv("COSMOS_CONN_STRING")

    db_name = os.getenv("COSMOS_DB_NAME")
    atm_container_name = os.getenv("COSMOS_ATM_CONTAINER")
    upi_container_name = os.getenv("COSMOS_UPI_CONTAINER")
    alert_container_name = os.getenv("COSMOS_ALERTS_CONTAINER")

    logging.info("🔐 Loaded environment settings successfully")

    # Extract container and blob name
    parts = blob_url.replace("https://", "").split("/")
    container_name = parts[1]
    blob_name = "/".join(parts[2:])

    logging.info(f"📌 Blob Stored — Container: {container_name}, File: {blob_name}")

    # Connect to Blob and download data
    blob_service = BlobServiceClient.from_connection_string(storage_conn)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob().readall()

    df = pd.read_csv(StringIO(blob_data.decode()))
    logging.info(f"📄 Total Records: {df.shape[0]}")

    # Connect to Cosmos
    cosmos = CosmosClient.from_connection_string(cosmos_conn)
    database = cosmos.get_database_client(db_name)

    atm_container = database.get_container_client(atm_container_name)
    upi_container = database.get_container_client(upi_container_name)
    alert_container = database.get_container_client(alert_container_name)

    # Detect file type
    txn_type = classify_transaction(blob_name)
    logging.info(f"📌 File Detected as Transaction Type: {txn_type}")

    inserted_count = 0
    fraud_count = 0

    for _, row in df.iterrows():
        doc = row.to_dict()

        # Fix ID — handles ATM + UPI uniquely
        txn_id = (
            doc.get("TransactionID") or
            doc.get("TxnID") or
            doc.get("EventID") or
            str(datetime.utcnow().timestamp())
        )

        doc["id"] = str(txn_id)
        doc["txn_type"] = txn_type

        # Normalize amount field
        amount = (
            doc.get("TransactionAmount") or
            doc.get("Amount") or
            doc.get("TxnAmount") or
            0
        )

        doc["Amount"] = float(amount)
        doc["processedAt"] = datetime.utcnow().isoformat()

        # Fraud Detection
        flags = detect_suspicious(doc)
        doc["fraud_flags"] = flags

        # Insert based on type
        if txn_type == "ATM":
            atm_container.upsert_item(doc)

        elif txn_type == "UPI":
            upi_container.upsert_item(doc)

        inserted_count += 1

        # Insert alert records
        if flags:
            for f in flags:
                alert_doc = {
                    "id": f"{doc['id']}_{f}",
                    "alertType": f,  # partition key
                    "txn_id": doc["id"],
                    "amount": doc["Amount"],
                    "txnType": txn_type,
                    "sourceFile": blob_name,
                    "alertTime": datetime.utcnow().isoformat()
                }
                alert_container.upsert_item(alert_doc)
                fraud_count += 1

    logging.info(f"🚀 Inserted Transactions: {inserted_count}")
    logging.info(f"🚨 Fraud Alerts Inserted: {fraud_count}")
    logging.info("🎯 Processing Completed Successfully!")
