# Football Data Engineering Project

This Python-based project crawls data from Wikipedia using Requests and BeautifulSoup libraries, then using Apache Airflow to orchestrate, clean it and push it to Azure Data Lake for furthur processing.

## Table of Contents

1. [System Architecture]()

## System Architecture)
![system_architecture.png](assets%2Fsystem_architecture.png)

## Requirements
- Python 3.9 (minimum)
- Docker
- PostgresSQL
- Apache Airflow 2.6 (minimum)

## Getting Started

1. Clone the repository.
   ```bash
   git clone https://github.com/MinhTran1506/WikipediaDataProject.git
   ```

2. Install Python dependencies.
   ```bash
   pip install -r requirements.txt
   ```

## Running the Code With Docker

1. Start your services on Docker with
   ```bash
   docker compose up -d
   ```
2. Trigger the DAG on the Airflow UI.

## How It Works
1. Fetches data from Wikipedia.
2. Cleans the data.
3. Transforms the data.
4. Pushes the data to Azure Data Lake

## Dashboard For The Data
![Dashboard 1](assets%2FDashboard%201.png)
