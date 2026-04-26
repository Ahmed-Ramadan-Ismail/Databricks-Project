# 🏛️ End-to-End Medallion Lakehouse on Azure Databricks

## 📌 Project Overview
This project showcases an enterprise-grade **End-to-End Data Pipeline** built on **Azure Databricks**. The core objective is to ingest raw data from multiple sources, process it at scale using **Apache Spark**, and structure it into a **Medallion Architecture (Lakehouse)** to serve business-ready insights via **Power BI**.

---

## 🏗️ Architecture Design
The pipeline follows a robust, scalable architecture transitioning data through three distinct layers (Bronze, Silver, and Gold) stored as **Delta Tables**.

![Pipeline Architecture](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/Design%20Pipeline.png)

### 🛠️ Tech Stack & Tools
* **Compute & Processing:** Azure Databricks, Apache Spark
* **Storage Format:** Delta Lake (Parquet-based)
* **Orchestration:** Databricks Workflows / Jobs
* **BI & Visualization:** Power BI (DirectQuery / Import Mode)
* **Data Sources:** REST APIs, Flat Files (CSV, JSON, Parquet)

---

## 🔄 The Medallion Data Journey

### 🥉 1. Bronze Layer (Raw Data)
* **Goal:** Landing zone for raw, uncleaned data exactly as it arrives from the source.
* **Process:** Spark jobs ingest data from APIs and file systems (CSV, JSON) and write it into Bronze Delta Tables without any transformations. This ensures historical traceability.

### 🥈 2. Silver Layer (Cleaned & Structured Data)
* **Goal:** A trusted source of truth. Data is filtered, cleansed, and validated.
* **Process:** Spark jobs read from the Bronze layer, apply strict schema validations, handle nulls/duplicates, and structure the data.

### 🥇 3. Gold Layer (Business-Ready Data)
* **Goal:** Highly optimized data ready for reporting and analytical queries.
* **Process:** Complex business logic and aggregations are applied. The data is modeled into **Fact and Dimension tables (Star Schema)** to optimize performance for Power BI dashboards.

---

```mermaid
graph LR
    subgraph Bronze ["Bronze Layer"]
        direction TB
        B1[hospital_data.bronze_patients]
        B2[hospital_data.bronze_doctors]
        B3[hospital_data.bronze_appointments]
        B4[hospital_data.bronze_treatments]
        B5[hospital_data.bronze_billing]
    end

    subgraph Silver ["Silver Layer"]
        direction TB
        S1[hospital_data.silver_patients]
        S2[hospital_data.silver_doctors]
        S3[hospital_data.silver_appointments]
        S4[hospital_data.silver_treatments]
        S5[hospital_data.silver_billing]
    end

    subgraph Gold ["Gold Layer"]
        direction TB
        G1[dim_patients]
        G2[dim_doctors]
        G3[dim_treatments]
        G4{{"fact_hospital_operations"}}
    end

    %% Connections
    B1 --> S1 --> G1
    B2 --> S2 --> G2
    B4 --> S4 --> G3
    
    S3 --> G4
    S4 --> G4
    S5 --> G4

    %% Styling for alignment
    style G4 fill:#f9f,stroke:#333,stroke-width:4px
