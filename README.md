Azure Banking Project — Day 1

Overview:
Day 1 covers the setup of an event-driven pipeline using Azure services.  
When a file is uploaded to Blob Storage, Event Grid triggers a Function which sends the blob URL to a Service Bus Queue.  
Another Function reads the queue message and logs it.

Components Used:
- Azure Blob Storage  
- Azure Event Grid  
- Azure Service Bus Queue  
- Azure Functions (EventGridTrigger + ServiceBusTrigger)  
- Python

How It Works:
1. File uploaded → Blob Storage  
2. Event Grid fires → triggers **handleEventGrid**  
3. handleEventGrid extracts blob URL → sends to Service Bus Queue  
4. **QueueProcessor** picks message → logs the content

Testing:
- Upload a file to Blob Storage  
- Check **handleEventGrid** logs  
- Check **Service Bus Queue** message count  
- Check **QueueProcessor** logs

Screenshots:
- Blob upload event trigger

![WhatsApp Image 2025-12-05 at 18 55 54_307d9616](https://github.com/user-attachments/assets/7c926d19-ac9a-4c47-9d29-ab01e7bcfc28)

![WhatsApp Image 2025-12-05 at 19 53 22_270a5a3a](https://github.com/user-attachments/assets/d849cfd4-8706-4439-8a2f-71445386eadf)

- Showing both functions
  
![WhatsApp Image 2025-12-05 at 18 03 23_b2bf8145](https://github.com/user-attachments/assets/9c5e32f5-d1f7-4a72-8a46-98525a8c78ef)

- handleEventGrid
  
![WhatsApp Image 2025-12-05 at 19 08 11_c60c1de0](https://github.com/user-attachments/assets/a414cefa-813d-4d07-9a6a-a946964bc871)

- QueueProcessor 
  
![WhatsApp Image 2025-12-05 at 19 01 12_2443bc8c](https://github.com/user-attachments/assets/40d0e27c-530b-4d9d-9fd3-b9170195021d)

- Service Bus Queue metrics / messages
  
![WhatsApp Image 2025-12-05 at 18 35 30_cb9d9cda](https://github.com/user-attachments/assets/73c5087f-62ee-4c18-88a3-4dc2f45b9ac3)

