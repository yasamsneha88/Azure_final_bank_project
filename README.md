DAY 1 — Architecture & Ingestion (Hands-on)

Objectives

•	Create storage (ADLS Gen2 / Blob), enable Event Grid notifications on containers, create a Service Bus, and build an Azure Function skeleton that responds to Event Grid events.

2) Create ADLS Gen2 (Storage Account)

Azure Portal → Storage accounts - Create
•	Basics tab: Subscription, Resource group (the one created above).

•	Advanced: Enable Data Lake Storage Gen2 (hierarchical namespace = Enabled).

•	Networking: Start with Public access for development; later secure with private endpoints.

•	Create.

Create containers

Portal: Storage Account - Containers 

•	raw/customers

•	raw/atm

•	raw/upi

3) Enable Event Grid on container

Event Grid can subscribe to blob created events.
•	Portal: Storage Account - Events (or Event Grid section) - + Event Subscription.

•	Event Schema: Event Grid Schema - Event Types: BlobCreated → Endpoint Type: Web Hook 

Function URL later) or Storage Queue/Service Bus. For now create and leave the endpoint blank or choose Event Grid Topic if you plan to publish.

4) Create Service Bus namespace - queue/topic
Portal : Service Bus -  Create.

•	Pricing tier: Basic or Standard for dev.

•	After create → Queues → + Queue → ingestion-requests.

5) Create Azure Function App (Python) — Ingestion function skeleton
Portal → Function App → + Create.

•	Runtime stack: Python (3.9 or supported), OS: Linux, Plan: Consumption (dev) or Premium if you need VNET.

Once created, go to Functions -  Add - Create from template - choose Event Grid Trigger (if present) or HTTP trigger (temporary) — we'll wire Event Grid subscription to the function's event endpoint.

Skeleton (deployable from VSCode)
Create a directory in VSCode


6) Juniper vSRX / Firewall — development guidance

•	If you don't have Juniper vSRX subscribed, use Azure Firewall or NSGs for dev.

•	Portal: Create → Firewall → name fw-azbank-dev

•	For now restrict DB ports; later, configure Private Endpoints.

DAY 2 — Transformation Layer & NoSQL Operational Store (Hands-on)

Objectives

•	Create Cosmos DB (Core SQL) account, design collections, and implement a queue-triggered function that reads queue messages, fetches blob content, validates, dedups, and writes to Cosmos DB.

1) Create Cosmos DB
Portal → Azure Cosmos DB → Create → API: Core (SQL).

Design collections (use logical containers / partition key)

•	AccountProfile partition key: /CustomerID

•	ATMTransactions partition key: /TransactionID

•	UPIEvents partition key: /TransactionID

•	FraudAlerts partition key: /CustomerID

Create these containers in Data Explorer.

2) Queue-triggered Azure Function (processing)

Create another Azure Function (Service Bus trigger)

Deduplication strategy: Upsert documents in Cosmos using TransactionID as id. If insert fails because exists, skip.

3) PySpark Data Clean Pipeline (Databricks/Synapse)

•	Create a Databricks workspace or Synapse Spark pool.

•	Develop notebooks that read from ADLS (bronze) and from Cosmos (via Spark connector), normalize, and write to Silver and Gold.

Notebook pseudocode (PySpark)
Mount ADLS: Use service principal + storage credentials to mount into Databricks.

DAY 3 — Data Warehouse (Synapse / Azure SQL)

Objectives

•	Implement dimensional model, create SQL DB or Synapse Dedicated SQL Pool, and create ETL jobs to populate dimensions and facts.

1) Choose analytical store

•	For a small PoC use Azure SQL Managed Instance or Azure SQL DB.

•	For larger scale, use Synapse Dedicated SQL Pool.

2) Create Schema & Tables

3) PySpark --> SQL Load (MERGE/UPSERT)

Day 3 handles data transformation using Databricks.
Banks rely heavily on clean, accurate, and enriched data before using it for analytics or reporting. Databricks provides a scalable Spark engine where you can run PySpark transformations on large banking datasets. You will mount ADLS, create notebooks, build Delta Tables, and write transformation logic for customers, transactions, and loans.

DAY 4 — Real-time Alerts, Security & CI/CD

Objectives

•	Implement fraud alert function, secure endpoints with Private Endpoints/Firewall, and setup CI/CD for Functions & notebooks.

1) Real-time Fraud Azure Function (Event Grid Trigger)
Create func-azbank-fraud-detector with Event Grid trigger on high-value transactions.

Pseudo logic:

•	Receive transaction

•	Check rules (amount > ₹50,000, geolocation mismatch, freq checks using Cosmos history)

•	If suspicious -> write to FraudAlerts container and push to notifications Service Bus topic.

Notification worker: Another Function listens to notifications and integrates with Twilio/SendGrid (for SMS/email). Keep API keys in Key Vault.

2) Security — Private Endpoints & Firewall

•	For Cosmos DB and SQL, configure Private Endpoint so they are only accessible inside VNet.

•	Use Azure Firewall / NSGs to restrict traffic.

•	Use Managed Identity for Functions to access resources (avoid connection strings in code).

3) CI/CD (GitHub Actions) — basic pipeline for Function App
.github/workflows/azure-functions.yaml (skeleton)

Store secrets: In GitHub repo secrets, add publish profile or use Service Principal.

DAY 5 — Reporting, Customer 360 & Demo

Objectives

•	Build Customer 360 queries/views, create Power BI datasets and dashboards, and run demo scenario.
Implement CI/CD for Function App & Data Pipelines

•	Automate the deployment of Function App code using GitHub Actions or Azure DevOps.

•	Ensure each commit triggers build and test pipelines.

•	Package Python function code and deploy it to the Function App automatically.

Establish Proper Version Control for All Components

•	Store Function App code, Databricks notebooks, ARM/Bicep templates, and configuration files in a Git repository.

•	Use feature branches for safe development and controlled merges.
 
 Configure Application Insights for Logging & Monitoring

•	Enable detailed runtime logs for Function Apps.

•	Track exceptions, slow executions, and failed triggers.

•	Monitor storage events and Service Bus message processing.

Set Up Log Analytics for Platform Observability
•	Create centralized logging for:
o	Function execution history
o	Event Grid trigger logs
o	Databricks job runs
o	Pipeline failures


Validate Function App Behavior Under Real Data

•	Upload sample banking files.Verify that Event Grid triggers correctly.

•	Confirm files reach the raw zone and metadata is logged properly.

Test Databricks Batch Processing End-to-End
•	Run scheduled jobs manually as part of testing.

•	Check that clean data lands in processed and curated layers.

•	Validate schema, null handling, and transformations.

 Validate Synapse Analytics Layer

•Run SQL queries on curated data.

•	Confirm tables and views reflect the latest ingestion.

•	Check customer 360, transaction summaries, and compliance metrics.

•	Move all sensitive data (keys, secrets, connection strings) into Key Vault.•	Update Function App’s configuration to pull secrets using Key Vault references.

•	Trigger the entire flow from file upload to analytics query:
ADLS - Service Bus - Function App - Databricks - Synapse

•	Validate no step fails and each component interacts correctly.

Screenshots:

<img width="1918" height="962" alt="image" src="https://github.com/user-attachments/assets/963f0154-88bf-4b0a-8fe6-a84ebdb3d9dd" />
<img width="1902" height="895" alt="image" src="https://github.com/user-attachments/assets/3e705c65-dfb6-4d2d-be4f-7991f068c9b3" />
<img width="1368" height="730" alt="image" src="https://github.com/user-attachments/assets/23fd32bf-743f-4112-bc68-39a873fff2dc" />
<img width="1909" height="1024" alt="Screenshot 2025-12-06 124448" src="https://github.com/user-attachments/assets/caabc592-54f4-4396-94d7-411ef934880f" />
<img width="1327" height="632" alt="image" src="https://github.com/user-attachments/assets/5a7290c4-4669-4c73-a545-529da5a9204d" />
<img width="1883" height="850" alt="image" src="https://github.com/user-attachments/assets/fd7b03aa-b70d-4592-96f3-5a3c318153ec" />
<img width="1093" height="761" alt="image" src="https://github.com/user-attachments/assets/0e30c5be-7cb8-4999-be77-c159fb27944f" />
<img width="835" height="653" alt="image" src="https://github.com/user-attachments/assets/41bfa0a2-8f73-4809-8637-e136bc702368" />
<img width="1012" height="735" alt="image" src="https://github.com/user-attachments/assets/11d3d93a-0c31-4c30-b6e6-8c7013d526cf" />
<img width="1013" height="467" alt="image" src="https://github.com/user-attachments/assets/800f7f88-4560-4f23-8d34-efd8f993bdee" />
<img width="1461" height="750" alt="image" src="https://github.com/user-attachments/assets/b690f321-8462-42db-83b3-06c6861cdf14" />
<img width="1022" height="733" alt="image" src="https://github.com/user-attachments/assets/15eff209-8ef7-494f-8044-9233f4ab7939" />
<img width="1725" height="517" alt="image" src="https://github.com/user-attachments/assets/60743269-2d0c-4b37-966f-a358b95b6d8f" />
<img width="1002" height="558" alt="image" src="https://github.com/user-attachments/assets/c0ec0746-c8e9-46c9-a0ed-2559442e153c" />





