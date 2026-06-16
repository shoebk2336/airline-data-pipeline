
# ✈️ Airline Flight Data Ingestion Pipeline

An end-to-end, event-driven data pipeline built on AWS that automates daily airline flight data ingestion, transformation, and loading into a Redshift data warehouse.

---

## 📋 Project Overview

This pipeline demonstrates core data engineering skills by building a fully automated, serverless ETL workflow using AWS-native services. When a daily flight CSV file is uploaded to S3, the pipeline automatically triggers, transforms the data by enriching it with airport dimension information, and loads the results into Amazon Redshift for analytics.

---

## 🏗️ Architecture

```
┌──────────┐     ┌─────────────┐     ┌────────────────┐     ┌──────────────┐
│  S3      │────▶│ EventBridge │────▶│ Step Functions │────▶│ Glue Crawler │
│ (CSV     │     │ (Trigger)   │     │ (Orchestrator) │     │ (Schema      │
│  Upload) │     └─────────────┘     └────────────────┘     │  Discovery)  │
└──────────┘                                │               └──────────────┘
                                            │                       │
                                            ▼                       ▼
┌──────────┐     ┌─────────────┐     ┌────────────────┐     ┌──────────────┐
│  SNS     │◀────│ Choice      │◀────│  Glue Job      │◀────│ Data Catalog │
│ (Alert)  │     │ (Status     │     │ (ETL Transform │     │ (Metadata)   │
│          │     │  Check)     │     │  + Load)       │     │              │
└──────────┘     └─────────────┘     └────────────────┘     └──────────────┘
                                            │
                                            ▼
                                     ┌──────────────┐
                                     │  Redshift    │
                                     │  Serverless  │
                                     │ (Warehouse)  │
                                     └──────────────┘
```

---

## 🔧 Services Used

| Service | Purpose |
|---------|---------|
| **Amazon S3** | Data lake — stores raw CSV files in Hive-partitioned format |
| **AWS Glue Crawlers** | Automatically discover schema from new files |
| **AWS Glue Data Catalog** | Centralized metadata store (schema, location, format) |
| **AWS Glue Studio** | Visual ETL — joins flight data with airport dimensions |
| **Amazon Redshift Serverless** | Columnar data warehouse for analytics |
| **Amazon EventBridge** | Event-driven trigger on CSV file upload |
| **AWS Step Functions** | Serverless orchestration with error handling |
| **Amazon SNS** | Success/failure email notifications |
| **VPC + Endpoints** | Secure private connectivity between services |

---

## 📂 S3 Data Organization

```
s3://airlines-landing-data-proj/
├── Airport_dim/                          ← Static dimension data (loaded once)
│   └── airports.csv
├── Daily_flight/                         ← Daily partitioned flight data (Hive format)
│   └── date=2026-02-02/
│       └── flights.csv
└── Temp_files/                           ← Staging area for Redshift COPY
```

### Why Hive Format (`date=YYYY-MM-DD`)?
- Enables automatic partition discovery by Glue Crawler
- Allows partition pruning — queries skip irrelevant data, reducing scan cost by up to 90%
- Industry standard for data lake organization

---

## 🔄 ETL Transformation Logic

The Glue Job performs **denormalization** — enriching flight facts with airport details via two LEFT JOINs:

```
                    ┌─────────────────────┐
                    │   Airport Dim       │
                    │   (airport_id,      │
                    │    city, state,     │
                    │    name)            │
                    └──────┬──────────────┘
                           │
          ┌────────────────┼─────────────────┐
          │ LEFT JOIN 1    │                 │ LEFT JOIN 2
          │ (origin)       │                 │ (destination)
          ▼                │                 ▼
┌──────────────────┐       │       ┌──────────────────┐
│ Flight Facts     │       │       │ Result Table     │
│ originairportid ─┼───────┘       │ + dep_city       │
│ destairportid   ─┼──────────────▶│ + dep_state      │
│ carrier          │               │ + dep_airport    │
│ depdelay         │               │ + arr_city       │
│ arrdelay         │               │ + arr_state      │
└──────────────────┘               │ + arr_airport    │
                                   └──────────────────┘
```

- **Join 1:** `daily_flight.originairportid = airport_dim.airport_id` → Gets departure airport details
- **Join 2:** `result.destairportid = airport_dim.airport_id` → Gets arrival airport details
- **Why LEFT JOIN?** Preserves all flight records even if airport info is missing — prevents data loss

---

## 🗄️ Redshift Schema Design

### Dimension Table (loaded once via COPY command)

```sql
CREATE SCHEMA airlines;

CREATE TABLE airlines.airport_dim (
    airport_id    BIGINT,
    city          VARCHAR,
    state         VARCHAR,
    name          VARCHAR
);

COPY airlines.airport_dim
FROM 's3://airlines-landing-data-proj/Airport_dim/airports.csv'
IAM_ROLE 'arn:aws:iam::784230180132:role/ReadshiftS3readrole'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';
```

### Fact Table (populated daily by Glue Job)

```sql
CREATE TABLE airlines.daily_flight_facts (
    carrier       VARCHAR,
    dep_airport   VARCHAR,
    arr_airport   VARCHAR,
    dep_city      VARCHAR,
    arr_city      VARCHAR,
    dep_state     VARCHAR,
    arr_state     VARCHAR,
    dep_delay     BIGINT,
    arr_delay     BIGINT
);
```

---

## ⚡ Step Functions — Orchestration

The state machine coordinates the entire workflow with automatic retry logic and status polling:

```
StartCrawler → Wait (5s) → GetCrawler → Choice: Is Running?
                                              │
                                    YES ──→ Wait again (loop)
                                    NO  ──→ StartGlueJob
                                              │
                                              ▼
                              Wait (60s) → GetJobRun → Choice: Status?
                                                          │
                                                SUCCEEDED → SNS Success ✅
                                                FAILED    → SNS Failure ❌
                                                RUNNING   → Wait again (loop)
```

### JSONata Conditions Used:

```
Crawler Check:  {% $states.input.Crawler.State = "RUNNING" %}
Job Check:      {% $states.input.JobRunState = "SUCCEEDED" %}
```

### How to Find the Correct Condition Path:
1. Run the Step Function (even if it fails)
2. Click the state BEFORE your Choice state
3. Look at the **Output** tab — that JSON = `$states.input` for the next state
4. Map your condition to that exact structure

---

## 🔒 VPC & Network Security

Glue connects to Redshift through a VPC with private endpoints — no data traverses the public internet:

```
┌─────────────────────── VPC ───────────────────────────────────┐
│                                                                │
│   ┌─── Subnet ──────────────────────────────────────────────┐ │
│   │                                                          │ │
│   │   AWS Glue (ENI — Elastic Network Interface)            │ │
│   │     │                                                    │ │
│   │     ├── Port 5439 ───→ Redshift Serverless              │ │
│   │     ├── Port 443 ────→ VPC Endpoint → STS               │ │
│   │     ├── Port 443 ────→ VPC Endpoint → Secrets Manager   │ │
│   │     └────────────────→ VPC Endpoint → S3 (Gateway)      │ │
│   │                                                          │ │
│   │   🛡️ Security Group: All Traffic (self-referencing)     │ │
│   └──────────────────────────────────────────────────────────┘ │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### VPC Endpoints Required:

| Endpoint | Type | Purpose | Cost |
|----------|------|---------|------|
| STS | Interface | IAM role verification | ~$7.20/month |
| Secrets Manager | Interface | Database credential retrieval | ~$7.20/month |
| S3 | Gateway | Temp data staging for Redshift | Free |

### Security Group Inbound Rules:

| Type | Port | Source | Purpose |
|------|------|--------|---------|
| All Traffic | All | Same SG (self-referencing) | **Mandatory** — Glue workers communicate internally |
| HTTPS | 443 | VPC CIDR | Glue → VPC Endpoints |
| Custom TCP | 5439 | Same SG | Glue → Redshift |

---

## 📡 EventBridge Trigger

The pipeline triggers automatically when a `.csv` file is uploaded to the Daily_flight folder:

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["airlines-landing-data-proj"]
    },
    "object": {
      "key": [{
        "suffix": ".csv"
      }]
    }
  }
}
```

**Prerequisite:** EventBridge notifications must be enabled on the S3 bucket (Properties → Amazon EventBridge → On)

---

## 📊 Sample Output (Redshift Query Results)

```sql
SELECT * FROM airlines.daily_flight_facts LIMIT 5;
```

| carrier | dep_city | arr_city | dep_state | arr_state | dep_delay | arr_delay |
|---------|----------|----------|-----------|-----------|-----------|-----------|
| AA | Dallas | Chicago | TX | IL | 15 | 10 |
| UA | Denver | New York | CO | NY | -3 | 5 |
| DL | Atlanta | Los Angeles | GA | CA | 0 | -8 |
| WN | Phoenix | Las Vegas | AZ | NV | 22 | 18 |
| B6 | Boston | Miami | MA | FL | 45 | 52 |

---

## 🚀 Complete Data Flow

### One-Time Setup:
```
airports.csv (S3) → COPY command → Redshift (airlines.airport_dim)
```

### Automated Daily Pipeline:
```
1. New CSV uploaded to S3 (Daily_flight/date=YYYY-MM-DD/flights.csv)
2. EventBridge detects .csv upload → triggers Step Function
3. Step Function starts Crawler (daily_flights)
4. Crawler discovers schema → updates Data Catalog
5. Step Function starts Glue Job (flight_data_ingestion)
6. Glue Job:
   a. Reads flight data from Data Catalog (source: S3)
   b. Reads airport_dim from Redshift (via JDBC connection)
   c. Performs 2 LEFT JOINs (origin + destination airports)
   d. Writes enriched data to Redshift (airlines.daily_flight_facts)
7. Step Function checks job status
8. SNS sends email notification (SUCCESS ✅ or FAILURE ❌)
```

---

## 📁 Repository Structure

```
airline-data-pipeline/
├── README.md
├── architecture/
│   └── pipeline-architecture.png
├── screenshots/
│   ├── 01-s3-bucket-structure.png
│   ├── 02-s3-hive-format.png
│   ├── 03-glue-crawlers.png
│   ├── 04-glue-visual-etl.png
│   ├── 05-redshift-query-results.png
│   ├── 06-step-functions-success.png
│   ├── 07-vpc-endpoints.png
│   ├── 08-security-group-rules.png
│   ├── 09-eventbridge-rule.png
│   └── 10-sns-notification.png
├── sql/
│   ├── create-schema.sql
│   ├── create-tables.sql
│   └── copy-commands.sql
├── glue-job/
│   └── flight_data_ingestion.py
├── step-functions/
│   └── state-machine.json
└── eventbridge/
    └── event-pattern.json
```

---

## 🛠️ Setup & Deployment

### Prerequisites
- AWS Account with free tier / credits
- IAM user with appropriate permissions
- Basic knowledge of SQL and Python

### Deployment Steps

| Step | Action | Service |
|------|--------|---------|
| 1 | Create S3 bucket with 3 folders, enable EventBridge | S3 |
| 2 | Upload airport dimension CSV | S3 |
| 3 | Create Redshift Serverless namespace + workgroup (8 RPU) | Redshift |
| 4 | Create schema, tables, and load dimension data via COPY | Redshift |
| 5 | Create 3 VPC Endpoints (STS, Secrets Manager, S3) | VPC |
| 6 | Configure Security Group (All Traffic self-ref, 443, 5439) | VPC |
| 7 | Create Redshift user for Glue | Redshift |
| 8 | Create JDBC Connection in Glue | Glue |
| 9 | Create 2 Crawlers (airport_dim + daily_flights) | Glue |
| 10 | Build Visual ETL Job with 2 LEFT JOINs, target = Redshift | Glue Studio |
| 11 | Create Step Functions state machine with polling loops | Step Functions |
| 12 | Create EventBridge rule with .csv suffix filter | EventBridge |
| 13 | Create SNS topic + email subscription | SNS |
| 14 | Test: Upload CSV → verify data in Redshift | End-to-End |

---

## 💰 Cost Optimization

| Service | Cost (Idle) | Cost (Running) | Action |
|---------|-------------|----------------|--------|
| S3 | Pennies/month | Pennies/month | Keep |
| Glue Crawler | $0 | ~$0.01/run | Keep |
| Glue Job | $0 | ~$0.44/10-min run | Keep |
| Redshift Serverless | ~$0 (auto-pauses) | $3/hour (8 RPU) | Keep (free credits) |
| EventBridge | $0 | $0 (free tier) | Keep |
| Step Functions | $0 | $0 (free tier) | Keep |
| SNS | $0 | $0 (free tier) | Keep |
| VPC Endpoints (Interface) | **~$14.40/month** | Same | ⚠️ Delete when not testing |
| VPC Endpoint S3 (Gateway) | $0 | $0 | Keep |

**💡 Tip:** Delete Interface VPC endpoints when not actively developing. Recreate in 2 minutes when needed. Total idle cost without endpoints: ~$0-2/month.

---

## 🐛 Errors Encountered & Solutions

| # | Error | Root Cause | Fix |
|---|-------|-----------|-----|
| 1 | Access Key needs subscription | Wrong region | Set correct region (us-east-1) |
| 2 | IAM role creation error | Insufficient IAM privileges | Create role manually in IAM console |
| 3 | Failed to assume role (STS) | No STS VPC endpoint | Create Interface endpoint for STS |
| 4 | Unable to connect to Secrets Manager | No Secrets Manager endpoint | Create Interface endpoint |
| 5 | Password authentication failed | Special chars in password | Use alphanumeric password only |
| 6 | Cannot drop user | User has privileges | Revoke all privileges first, or use ALTER USER |
| 7 | All ingress ports must be open | Missing self-referencing rule | Add "All Traffic" inbound from same SG |
| 8 | S3 endpoint validation failed | Endpoint not linked to subnet's route table | Associate endpoint with correct route table |
| 9 | Entity Not Found (getCatalogSource) | Table missing from Data Catalog | Re-run crawler / verify DB and table names |
| 10 | SNS Specify TopicArn | No topic ARN in SNS publish state | Create SNS topic and add ARN |
| 11 | Step Functions not authorized | Missing Glue/SNS policies on SF role | Attach Glue + SNS policies to role |
| 12 | Data not reaching Redshift | Glue Job target was S3/Data Catalog | Change target node to Amazon Redshift |
| 13 | Run-part files in daily_flights folder | Glue output writing to same S3 path | Fix target to Redshift; delete junk files |
| 14 | Reference to '$' not supported | JSONata syntax needed (not JSONPath) | Use `{% $states.input.X %}` format |

---

## 🎓 Key Learnings

- **Namespace ≠ Database:** Namespace is a container for databases, roles, and snapshots. Database is auto-created inside it.
- **Data Catalog is metadata only:** It's a map pointing to S3 data — not actual storage. Writing to it creates S3 files, not Redshift records.
- **COPY > INSERT:** For bulk loading into Redshift, always use COPY command (parallel loading via MPP architecture).
- **Security Group self-referencing:** Mandatory for Glue — Spark workers communicate on random ports with each other.
- **VPC Endpoints:** Required because Glue runs inside VPC but needs to reach AWS services (STS, Secrets Manager, S3) that are outside the VPC.
- **JSONata conditions:** Use `$states.input` to access the previous state's output in Step Functions Choice states.
- **Target matters:** Glue → Data Catalog = stays in S3. Glue → Redshift = reaches the warehouse.
- **IAM Role ARN (not Policy ARN):** COPY command requires the Role ARN, values must be in single quotes.

---

## 👤 Author

**Shoeb Khan** — Data Engineer

---

## 📄 License

This project is for educational and portfolio demonstration purposes.
