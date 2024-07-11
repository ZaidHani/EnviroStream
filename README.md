# EnviroStream
EnviroStream is a data engineering project designed to process and analyze environmental sensor telemetry data in real-time. The project utilizes Apache Kafka for data ingestion, Spark Streaming for data processing, Postgres for data storage, and Streamlit for data visualization.

![Dashboard Showcase](https://github.com/ZaidHani/EnviroStream/blob/main/media/Screenshot%20(763).png)

## Architecture

1. **Data Ingestion:** Environmental sensor telemetry data is ingested using Apache Kafka, with 3 topics for different IoT machines and 3 brokers managed by Apache Zookeeper.
2. **Data Processing:** Spark Streaming acts as a consumer to read data from Kafka topics and processes the data.
3. **Data Storage:** The processed data is stored in a Postgres database.
4. **Data Visualization:** Streamlit reads the live data from the Postgres database and visualizes it through interactive dashboards.
![Dashboard Showcase](https://github.com/ZaidHani/EnviroStream/blob/main/media/EnviroStream.drawio.png)

## Setup

To set up the EnviroStream project, follow these steps:
### Prerequisites
- Python 3.8+
- Java 8+

run the setup.py file in the src directory.

Feel free to adjust any of the sections based on your specific setup and preferences.
