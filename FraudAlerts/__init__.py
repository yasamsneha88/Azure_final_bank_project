import logging
import os
from datetime import datetime
import uuid
import json

import azure.functions as func
from azure.cosmos import CosmosClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# === ENV VARS (match your local.settings.json) ===
COSMOS_CONN_STRING = os.getenv("COSMOS_CONN_STRING")
COSMOS_DB_NAME = os.getenv("COSMOS_DB_NAME", "bank_ops_db")
COSMOS_FRAUD_CONTAINER = os.getenv("COSMOS_FRAUD_CONTAINER", "FraudAlerts2")

SERVICE_BUS_CONN = os.getenv("SERVICE_BUS_CONN")
SERVICE_BUS_QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME", "fraud-notifications")

# === GLOBAL CLIENTS (init once per cold start) ===
cosmos_client = CosmosClient.from_connection_string(COSMOS_CONN_STRING)
fraud_container = cosmos_client.get_database_client(COSMOS_DB_NAME) \
                               .get_container_client(COSMOS_FRAUD_CONTAINER)

sb_client = ServiceBusClient.from_connection_string(SERVICE_BUS_CONN, logging_enable=True)


def is_suspicious(txn: dict):
    """
    Very simple rule engine.
    Returns (is_suspicious: bool, reasons: list[str])
    """
    reasons = []

    try:
        amount = float(txn.get("Amount", 0))
    except Exception:
        amount = 0

    status = txn.get("Status", "")
    geo = txn.get("GeoLocation", "")
    device = txn.get("DeviceID", "")

    # Rule 1: high-value txn
    if amount >= 50000:
        reasons.append(f"High value transaction: {amount}")

    # Rule 2: non-success status
    if status and status != "SUCCESS":
        reasons.append(f"Non-success status: {status}")

    # Rule 3: missing or weird geo
    if not geo:
        reasons.append("Missing geolocation")

    # Rule 4: weird device pattern (example)
    if device and device.startswith("DEV9"):
        reasons.append(f"Unusual device pattern: {device}")

    return (len(reasons) > 0, reasons)


def main(event: func.EventGridEvent):
    """
    Event Grid trigger: receives a transaction event, runs fraud rules,
    writes alerts to Cosmos (FraudAlerts2) and sends a notification
    into Service Bus queue 'fraud-notifications'.
    """
    logging.info("FraudAlertsHandler triggered. Event ID: %s", event.id)

    # Event Grid data payload (should be your transaction object)
    txn = event.get_json()
    logging.info("Received transaction payload: %s", txn)

    suspicious, reasons = is_suspicious(txn)

    if not suspicious:
        logging.info("Transaction %s is not suspicious, skipping.", txn.get("TxnID"))
        return

    # --- Build alert document for Cosmos ---
    alert_id = str(uuid.uuid4())
    alert_doc = {
        "id": alert_id,
        "AlertID": alert_id,
        "CustomerID": txn.get("CustomerID"),
        "AccountNumber": txn.get("AccountNumber"),
        "TxnID": txn.get("TxnID"),
        "Amount": txn.get("Amount"),
        "Status": txn.get("Status"),
        "Channel": txn.get("TxnChannel") or txn.get("Channel") or "UPI",
        "GeoLocation": txn.get("GeoLocation"),
        "DeviceID": txn.get("DeviceID"),
        "Reasons": reasons,
        "AlertCreatedAt": datetime.utcnow().isoformat() + "Z",
        "SourceSystem": "FraudAlertsHandler"
    }

    # 1) Write to Cosmos DB (FraudAlerts2)
    fraud_container.upsert_item(alert_doc)
    logging.info("Inserted fraud alert into Cosmos container %s (AlertID=%s)",
                 COSMOS_FRAUD_CONTAINER, alert_id)

    # 2) Send notification via Service Bus
    notif_payload = {
        "AlertID": alert_id,
        "CustomerID": alert_doc["CustomerID"],
        "AccountNumber": alert_doc["AccountNumber"],
        "Amount": alert_doc["Amount"],
        "TxnID": alert_doc["TxnID"],
        "Reasons": reasons
    }

    notif_body = json.dumps(notif_payload)

    with sb_client:
        sender = sb_client.get_queue_sender(queue_name=SERVICE_BUS_QUEUE_NAME)
        with sender:
            sender.send_messages(ServiceBusMessage(notif_body))
            logging.info("Sent fraud notification to Service Bus queue '%s'",
                         SERVICE_BUS_QUEUE_NAME)