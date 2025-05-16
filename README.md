# Azure Event Hub to MySQL Ingestion Pipeline

## 📌 Project Overview

This project implements a real-time data ingestion pipeline using **Azure Functions** and **Azure Event Hub**. It is designed to process incoming event messages, validate them against a predefined **JSON schema**, normalize nested data structures using **pandas**, and store the results in a **MySQL database**.

The solution is ideal for applications that require real-time or near-real-time processing of structured event data—such as order records, telemetry, or audit logs.

---

## ⚙️ Key Functionality

- **Event Trigger**: Listens to an Azure Event Hub for new messages.
- **Schema Validation**: Validates each message against a standard JSON schema (retrieved via `get_schema()`).
- **Data Transformation**: Uses `pandas.json_normalize` to flatten nested data (e.g., shipping address).
- **Database Integration**: Inserts validated and transformed records into a MySQL table.
- **Logging**: Uses `logging` module for structured logs and error tracking.

---

## 🧱 Architecture

```text
Azure Event Hub
      ↓
Azure Function App (Event Trigger)
      ↓
JSON Schema Validation
      ↓
Data Normalization (pandas)
      ↓
MySQL Database (Azure)
