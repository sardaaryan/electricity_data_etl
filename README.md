# Automated EIA Electricity Data Pipeline
### Tracking the Energy Bottleneck in the AI/Tech Infrastructure Race

## 📌 Project Overview
This project is a production-ready ETL (Extract, Transform, Load) pipeline that automates the collection of retail electricity price data from the U.S. Energy Information Administration (EIA) API. It is designed to track the primary cost-driver of the next decade of tech infrastructure: **Energy.**

## 💡 The Strategic Context: The "IREN" Narrative
The tech landscape has undergone a tectonic shift. As seen with the market performance of companies like **IREN (Iris Energy)**, the "moat" for modern tech giants is no longer just software—it is the ability to secure massive amounts of cheap, reliable power. IREN’s pivot from pure Bitcoin mining to AI Data Centers demonstrated that valuation is increasingly driven by **secured power capacity and energy cost management.**

In a world where AI data centers, semiconductor fabs, and crypto-operations compete for the same grid resources, the winners are those who can:
1. **Identify low-cost industrial hubs** using historical price data.
2. **Anticipate price volatility** that could compromise operational margins.
3. **Analyze the gap** between government-subsidized rates and market reality.

This project provides the foundational data engineering to analyze these trends, proving that the future of tech is inextricably linked to energy economics.



---

## 🛠 Technical Architecture
The system is built with a focus on **Data Engineering best practices** to ensure scalability and reliability:

* **Incremental Loading (High-Water Mark Strategy):** To optimize API usage, the pipeline queries the destination database for the `MAX(month)` and only requests "delta" data from the source.
* **Configuration-Driven Design:** Managed via `config.yaml`, the pipeline can scale across different states (e.g., comparing Texas’s deregulated grid to New York’s regulated market) without any code changes.
* **Data Quality & Validation:** A robust transformation layer handles null values in specialized sectors (Transportation/Other), enforces numeric schemas, and normalizes date formats.
* **Idempotency:** The logic is designed to be run repeatedly without creating duplicate records, ensuring a consistent state in the destination sink.



---

## 💻 Tech Stack
| Category | Technology |
| :--- | :--- |
| **Language** | Python 3.11+ |
| **Data Manipulation** | Pandas |
| **Database/ORM** | SQLite / SQLAlchemy |
| **Config/Security** | YAML / python-dotenv |
| **Data Source** | EIA Open Data API v2 |

---

## 🚀 Getting Started

### 1. Prerequisites
* Register for a free API Key at [EIA.gov](https://www.eia.gov/opendata/register.php).
* Python installed on your local machine.

### 2. Installation
```bash
# Clone the repository
git clone [https://github.com/sarda/electricity-etl.git](https://github.com/sarda/electricity-etl.git)
cd electricity-etl

# Install dependencies
pip install -r requirements.txt
```

### 3. Running the Pipeline
```bash
python pipeline.py
```