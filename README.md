# BDT Final Assignment

This project is part of the **Big Data Tools & Techniques** final assignment. It involves the processing, transformation, and visualization of trip data from the [Divvy Bicycle Sharing System](https://divvy-tripdata.s3.amazonaws.com/index.html or https://www.kaggle.com/datasets/saifali77/divvytripdata-2022-dataset?select=202202-divvy-tripdata) for February 2022.

## Dataset

The dataset used for this assignment is publicly available on Kaggle:

- **Source:** [Divvy Tripdata 2022 - February](https://divvy-tripdata.s3.amazonaws.com/index.html or https://www.kaggle.com/datasets/saifali77/divvytripdata-2022-dataset)
- **File:** `202202-divvy-tripdata.csv`

## 📂 Project Structure

```
BDT_FINAL_ASSIGNMENT/
├── data/
│   └── 202202-divvy-tripdata.csv
├── output/
│   ├── daily_trip_count.png
│   ├── top_end_stations.png
│   └── user_type_distribution.png
├── src/
│   ├── bronze.py
│   ├── cleandb.py
│   ├── gold.py
│   ├── silver.py
│   └── utils.py
├── .env
├── BDT_FINALE-token.json
├── noshare.toml
├── poetry.lock
├── pyproject.toml
├── README.md
└── secure-connect-bdt-finale.zip
```

## Setup Instructions

1. **Install dependencies**  
   Use [Poetry](https://python-poetry.org/docs/#installation):
   ```bash
   poetry install
   ```

2. Run pipeline
   ```bash
   poetry run python src/bronze.py
   poetry run python src/silver.py
   poetry run python src/gold.py
   ```

3. **Clean database**
   ```bash
   poetry run python src/cleandb.py
   ```

## Utility Module

The `utils.py` script in the `src/` directory contains functions to:
- Connect to AstraDB/Cassandra securely
- Create necessary Cassandra tables

Ensure connection credentials and `.env` file are properly configured.

## Sensitive Files

These are excluded using `.gitignore`:
- `.env`
- `BDT_FINALE-token.json`
- `secure-connect-bdt-finale.zip`
- `noshare.toml`

## Outputs

- `daily_trip_count.png`
- `top_end_stations.png`
- `user_type_distribution.png`

## Author

Giridhar Reddy Challa — Big Data Tools & Techniques, 2025.