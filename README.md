<div align="center">

<img src="https://raw.githubusercontent.com/Ahmed-Ramadan-Ismail/Databricks-Project/main/image/hospital_icon.png" width="100"/>

# 🏥 Hospital Data Pipeline
### End-to-End Medallion Lakehouse on Azure Databricks

[![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://azure.microsoft.com/en-us/products/databricks)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)

</div>

---

## 📌 Project Overview

This project showcases an **enterprise-grade, End-to-End Data Pipeline** built on **Azure Databricks** for a healthcare domain. Raw hospital data is ingested from multiple sources (REST APIs + flat files), processed at scale using **Apache Spark**, and structured into a **Medallion Architecture (Lakehouse)** with three distinct layers (Bronze → Silver → Gold) stored as **Delta Tables** — ultimately serving business-ready insights through **Power BI** dashboards.

---

## 🏗️ Architecture

![Pipeline Architecture](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/Design%20Pipeline.png)

The pipeline transitions data through three medallion layers, each stored as Delta Tables inside the **Unity Catalog** on Azure Databricks.

| Layer | Purpose | Storage Format |
|-------|---------|----------------|
| 🥉 Bronze | Raw landing zone — data as-is | Delta Table |
| 🥈 Silver | Cleaned, validated, structured | Delta Table |
| 🥇 Gold | Business-ready, Star Schema | Delta Views |

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| **Cloud Platform** | Microsoft Azure |
| **Compute & Processing** | Azure Databricks + Apache Spark |
| **Storage Format** | Delta Lake (Parquet-based) |
| **Catalog & Governance** | Unity Catalog (UC) |
| **Orchestration** | Databricks Workflows / Jobs |
| **BI & Visualization** | Power BI (DirectQuery / Import Mode) |
| **Data Sources** | REST APIs (GitHub), Flat Files (CSV) |
| **Language** | Python (PySpark), SQL |

---

## 📂 Project Structure

```
hospital-data-pipeline/
│
├── notebooks/
│   ├── Bronze_layer.ipynb        # Data ingestion → Raw Delta Tables
│   ├── Silver_layer.ipynb        # Transformation & Cleansing
│   └── Gold_layer.ipynb          # Business Logic → Star Schema Views
│
├── image/
│   ├── Design_Pipeline.png
│   ├── schema_Design.png
│   ├── run_pipeline.png
│   ├── add_triggers.png
│   ├── load_data_silver.png
│   ├── photo_bronze_load_data_in_catalog.png
│   ├── upload_data_files_to_volume.png
│   ├── conncation_to_Power_bi_.png
│   └── dashboard_1.png
│
└── README.md
```

---

## 🔄 Medallion Data Journey

### 🥉 Step 1 — Bronze Layer (Raw Data Ingestion)

The Bronze layer is the **landing zone**: data arrives exactly as-is from sources — no transformations applied. This guarantees full historical traceability and replayability.

**Data Sources:**

| Table | Source Type | Method |
|-------|-------------|--------|
| `bronze_patients` | REST API (GitHub) | `pd.read_csv(url)` → Spark DF |
| `bronze_treatments` | REST API (GitHub) | `pd.read_csv(url)` → Spark DF |
| `bronze_appointments` | Volume (CSV) | `spark.read.csv(volume_path)` |
| `bronze_doctors` | Volume (CSV) | `spark.read.csv(volume_path)` |
| `bronze_billing` | Volume (CSV) | `spark.read.csv(volume_path)` |

All tables are tagged with a `Data_Source` column to track their origin, then written as **Delta Tables** to `hospital.healthlycare.*`.

```python
# Example — Ingest from API + Volume, tag source, write to Bronze
df_patients = spark.createDataFrame(pd.read_csv(url_patients))
df_patients = df_patients.withColumn("Data_Source", lit("API"))
df_patients.write.mode("overwrite").format("delta").saveAsTable("hospital.healthlycare.bronze_patients")
```

**Catalog after Bronze load:**

![Bronze Catalog](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/photo%20bronze%20load%20data%20in%20catalog.png)

---

### 🥈 Step 2 — Silver Layer (Transformation & Cleansing)

The Silver layer is the **trusted source of truth**. Spark jobs read from Bronze, apply strict data quality rules, and produce clean, structured Delta Tables.

**Transformations Applied:**

| Entity | Transformation |
|--------|---------------|
| **Patients** | `F`/`M` → `Female`/`Male` · `first_name` + `last_name` → `Full_Name` · `date_of_birth` cast to `DateType` · Null checks on `patient_id` |
| **Doctors** | `first_name` + `last_name` → `Full_Name` |
| **Treatments** | `cost` rounded to 1 decimal place |
| **Appointments** | `appointment_time` reformatted to `hh:mm a` · `transaction_id` generated using `monotonically_increasing_id()` |
| **Billing** | `amount` rounded to 1 decimal place |

```python
# Example — Patients cleansing
patients = patients.replace("F", "Female", subset="gender").replace("M", "Male", subset="gender")
patients = patients.withColumn("Full_Name", concat_ws(" ", col("first_name"), col("last_name")))
patients = patients.withColumn("date_of_birth", to_date(col("date_of_birth"), "yyyy-MM-dd"))
```

**Silver tables loaded to catalog:**

![Silver Load](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/load%20data%20silver.png)

---

### 🥇 Step 3 — Gold Layer (Business-Ready Star Schema)

The Gold layer applies **business logic and aggregations**, modelling data into a **Star Schema** optimised for Power BI reporting.

**Schema Design:**

![Schema Design](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/schema%20Design.png)

**Gold Objects Created (as Delta Views):**

```
hospital.healthlycare
├── fact_hospital          ← Central fact table (appointments + billing + treatments)
├── dim_patients           ← Patient dimension
├── dim_doctors            ← Doctor dimension
└── dim_treatments         ← Treatment dimension
```

**Data Flow (Bronze → Silver → Gold):**

```mermaid
graph LR
    subgraph Bronze ["🥉 Bronze Layer"]
        B1[bronze_patients]
        B2[bronze_doctors]
        B3[bronze_appointments]
        B4[bronze_treatments]
        B5[bronze_billing]
    end
    subgraph Silver ["🥈 Silver Layer"]
        S1[silver_patients]
        S2[silver_doctors]
        S3[silver_appointments]
        S4[silver_treatments]
        S5[silver_billing]
    end
    subgraph Gold ["🥇 Gold Layer"]
        G1[dim_patients]
        G2[dim_doctors]
        G3[dim_treatments]
        G4{{"fact_hospital"}}
    end

    B1 --> S1 --> G1
    B2 --> S2 --> G2
    B4 --> S4 --> G3
    S3 --> G4
    S4 --> G4
    S5 --> G4

    style G4 fill:#f9a,stroke:#c33,stroke-width:3px
```

---

## ⚙️ Pipeline Orchestration

The three notebooks are chained as a **Databricks Workflow** (Jobs & Pipelines) with a scheduled trigger.

**Pipeline Tasks:**

```
Bronze_layer  ──►  Silver_layer  ──►  Gold_layer
```

**Trigger:** Every 5 minutes, starting at 4 minutes past the hour (configurable).

**Tasks view:**

![Pipeline Tasks](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/run%20pipeline.png)

**Schedules & Triggers:**

![Triggers](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/add%20trigges.png)

**Successful Runs:**

All three layers ran successfully — both manually triggered and by the scheduler (57s avg runtime).

---

## 📤 Data Loading to Unity Catalog Volume

Source CSV files (appointments, billing, doctors) are uploaded directly to a **Databricks Unity Catalog Volume** before ingestion:

```
/Volumes/hospital/healthlycare/healthly_volume/
├── appointments.csv
├── billing.csv
└── doctors.csv
```

![Volume Upload](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/upload%20data%20files%20to%20volume.png)

---

## 📊 Power BI Integration

The Gold layer is connected to **Power BI Desktop** via the native Azure Databricks connector. All 14 tables (Bronze + Silver + Gold) are visible and selectable in the Power BI Navigator.

**Connection:** `dbc-fec0963f-c546.cloud.databricks.com` → `hospital.healthlycare`

![Power BI Connection](https://github.com/Ahmed-Ramadan-Ismail/Databricks-Project/blob/main/image/conncation%20to%20Power%20bi%20.png)

**Dashboard Preview:**

![Dashboard](image/dashboard_1.png)

---

## 🗄️ Unity Catalog Structure

```
hospital (Catalog)
└── healthlycare (Schema)
    ├── Tables (10)
    │   ├── bronze_appointments
    │   ├── bronze_billing
    │   ├── bronze_doctors
    │   ├── bronze_patients
    │   ├── bronze_treatments
    │   ├── silver_appointments
    │   ├── silver_billing
    │   ├── silver_doctors
    │   ├── silver_patients
    │   └── silver_treatments
    ├── Views (4)
    │   ├── dim_doctors
    │   ├── dim_patients
    │   ├── dim_treatments
    │   └── fact_hospital
    └── Volumes (1)
        └── healthly_volume
```

---

## 🚀 How to Run

1. **Upload source files** to the Unity Catalog Volume:
   - `appointments.csv`, `billing.csv`, `doctors.csv` → `/Volumes/hospital/healthlycare/healthly_volume/`

2. **Run notebooks in order** (or trigger the Workflow):
   ```
   Bronze_layer.ipynb  →  Silver_layer.ipynb  →  Gold_layer.ipynb
   ```

3. **Connect Power BI** using the Azure Databricks connector:
   - Server: your Databricks workspace URL
   - HTTP Path: your SQL Warehouse HTTP path
   - Select tables from `hospital.healthlycare`

4. **(Optional) Schedule the pipeline** via Databricks Jobs with a cron trigger.

---

## 👤 Author

**Ahmed Ramadan Ismail**  
Data Engineer | Azure Databricks | Apache Spark | Delta Lake

[![GitHub](https://img.shields.io/badge/GitHub-Ahmed--Ramadan--Ismail-181717?style=flat&logo=github)](https://github.com/Ahmed-Ramadan-Ismail)

---

<div align="center">
<sub>Built with ❤️ using Azure Databricks · Apache Spark · Delta Lake · Power BI</sub>
</div>
