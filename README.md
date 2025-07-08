# 🚀 Cryptocurrency Data Warehouse with Advanced Analytics

A professional end-to-end data pipeline that extracts real-time cryptocurrency data, performs advanced mathematical transformations, and provides business-ready analytics for investment decision-making.

![Data Pipeline](https://img.shields.io/badge/Pipeline-Real--Time-brightgreen) ![dbt](https://img.shields.io/badge/dbt-Transformations-orange) ![SQLite](https://img.shields.io/badge/Database-SQLite-blue) ![API](https://img.shields.io/badge/Data-CoinGecko%20API-yellow)

## 📊 Project Overview

This project demonstrates a complete **ELT (Extract, Load, Transform)** data pipeline that processes cryptocurrency data with mathematical rigor, delivering actionable insights for financial analysis.

### Key Achievements
- **Real-time data extraction** from CoinGecko API
- **Advanced mathematical modeling** with log returns and volatility analysis  
- **Professional data architecture** with layered transformations
- **Business-ready analytics** with growth classification and risk assessment

## 🏗️ Architecture

```
📡 CoinGecko API
    ↓
🔄 Python Extraction
    ↓
📊 SQLite Data Warehouse
    ↓
🔧 dbt Transformations
    ↓
📈 Analytics Dashboard
```

### Data Flow Layers

| Layer | Purpose | Records | Technology |
|-------|---------|---------|------------|
| **Raw** | Original API data | 93 | CoinGecko API + Python |
| **Staging** | Cleaned & validated | 93 | SQLite + dbt |
| **Intermediate** | Mathematical transformations | 90 | Advanced SQL |
| **Marts** | Business analytics | 3 | Executive dashboard |

## 💰 Key Results

Our analysis of **30 days** of cryptocurrency data reveals:

### Performance Summary
| Cryptocurrency | Total Return | Growth Category | Risk Level | Volatility Days |
|----------------|--------------|-----------------|------------|-----------------|
| **ADA (Cardano)** | +60.87% | High Growth | Moderate | 6 high-risk days |
| **ETH (Ethereum)** | +65.04% | High Growth | Moderate | 5 high-risk days |  
| **BTC (Bitcoin)** | +17.72% | Moderate Growth | Low | 0 high-risk days |

### Mathematical Insights
- **Log Returns Analysis**: Advanced financial mathematics for superior analytical properties
- **Volatility Modeling**: Risk quantification using statistical standard deviation
- **Growth Classification**: Automated categorization based on return patterns
- **Risk Assessment**: Daily volatility analysis with threshold-based classification

## 🔧 Technical Implementation

### Technologies Used
- **Python** - Data extraction and API integration
- **SQLite** - Data warehouse and storage
- **dbt** - Data transformation and modeling
- **SQL** - Advanced analytics and mathematical calculations
- **CoinGecko API** - Real-time cryptocurrency data source
- **Git** - Version control and project management

### Mathematical Features
- **Log Returns**: `ln(price_t / price_{t-1})` for mathematical superiority
- **Volatility Analysis**: Rolling standard deviation with multiple time windows
- **Risk Categorization**: Statistical classification based on return magnitude
- **Performance Metrics**: Cumulative returns using exponential of log returns
- **Growth Classification**: Automated business logic for investment decisions

## 📁 Project Structure

```
crypto-data-warehouse-dbt-airflow/
├── 📊 extract/
│   ├── extract_crypto.py          # CoinGecko API integration
│   └── crypto_data_*.csv          # Raw extracted data
├── 🔄 load/
│   └── load_to_sqlite.py          # Database loading logic
├── 🎯 dbt_project/
│   ├── models/
│   │   ├── staging/               # Data cleaning layer
│   │   ├── intermediate/          # Mathematical transformations  
│   │   └── marts/                 # Business analytics
│   ├── dbt_project.yml           # dbt configuration
│   └── profiles.yml              # Database connections
├── 🗄️ crypto_warehouse.db         # SQLite data warehouse
└── 📋 README.md                   # Project documentation
```

## 🚀 Quick Start

### Prerequisites
- Python 3.13+
- SQLite3
- Git

### Installation
```bash
# Clone the repository
git clone https://github.com/Bra-AM/crypto-data-warehouse-dbt-airflow.git
cd crypto-data-warehouse-dbt-airflow

# Create virtual environment
python -m venv cryptodata-env
source cryptodata-env/bin/activate  # On Windows: cryptodata-env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install dbt dependencies
cd dbt_project
dbt deps
```

### Running the Pipeline
```bash
# 1. Extract cryptocurrency data
cd extract
python extract_crypto.py

# 2. Load data to warehouse
cd ../load  
python load_to_sqlite.py

# 3. Run dbt transformations
cd ../dbt_project
dbt run

# 4. View results
sqlite3 ../crypto_warehouse.db "SELECT * FROM marts_crypto_analytics;"
```

## 📈 Data Analysis Examples

### View Mathematical Transformations
```sql
SELECT 
    symbol, 
    price_timestamp,
    price_usd,
    ROUND(log_return, 6) as daily_return,
    risk_category
FROM int_crypto_returns 
WHERE symbol = 'BTC'
LIMIT 10;
```

### Performance Analytics
```sql
SELECT 
    symbol,
    total_return_pct,
    growth_category,
    high_risk_days,
    avg_daily_return
FROM marts_crypto_analytics
ORDER BY total_return_pct DESC;
```

## 🎯 Business Value

### For Investment Decision-Making
- **Risk Assessment**: Quantified volatility analysis for portfolio management
- **Performance Tracking**: Mathematical return calculations with proper compounding
- **Growth Classification**: Automated categorization for investment strategies
- **Real-time Insights**: Current market data for timely decision-making

### Technical Differentiation
- **Mathematical Rigor**: Log returns and advanced statistical measures
- **Professional Architecture**: Industry-standard ELT pipeline design
- **Scalable Foundation**: Easily extensible to additional cryptocurrencies
- **Production-Ready**: Proper error handling, logging, and data validation

## 🔮 Future Enhancements

- **Apache Airflow**: Automated scheduling and monitoring
- **Advanced Visualizations**: Interactive dashboards with Plotly/Streamlit
- **Machine Learning**: Predictive models for price forecasting  
- **Portfolio Optimization**: Multi-asset correlation analysis
- **Real-time Streaming**: Continuous data ingestion and processing

## 📊 Sample Output

**Final Analytics Dashboard:**
```
Symbol | Total Return | Growth Category | Risk Days | Daily Return
-------|--------------|-----------------|-----------|-------------
ADA    | +60.87%      | High Growth     | 6         | 0.015848
ETH    | +65.04%      | High Growth     | 5         | 0.016701  
BTC    | +17.72%      | Moderate Growth | 0         | 0.005438
```

## 🤝 Contributing

This project demonstrates professional data engineering practices and mathematical modeling techniques. Feel free to explore the code and adapt it for your own cryptocurrency analysis needs.

## 📧 Contact

**Project Showcase**: This repository demonstrates advanced data engineering, mathematical modeling, and business analytics capabilities for professional portfolio purposes.

---

*Built with mathematical precision and professional data engineering practices* 🚀
