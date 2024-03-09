# DataEnginner-Leagues_Snowflake
![Banner Image](https://raw.githubusercontent.com/CD-AC/DataEnginner-Leagues_Snowflake/main/Snowflake_Football_League.jpg)

# Description
This project is designed to streamline the process of data acquisition from European soccer leagues' standings, available on ESPN's website. Leveraging the power of Apache Airflow, containerized with Docker, we've automated the ETL process to cleanse, transform, and prepare the data for analysis. The cleaned data is temporarily stored in a staging area before being loaded into a final table in Snowflake. Finally, we utilize Power BI to connect to the Snowflake table and generate comprehensive statistical reports on the leagues.

# Architecture
![Banner Image](https://raw.githubusercontent.com/CD-AC/DataEnginner-Leagues_Snowflake/main/Architecture.png)

# Data Visualization with Power BI
![Banner Image](https://raw.githubusercontent.com/CD-AC/DataEnginner-Leagues_Snowflake/main/Football_Leagues_PBI.png)

# Features
- Data Extraction: Automated scraping of European soccer leagues data from ESPN's website.
- Data Transformation and Cleaning: Utilization of Apache Airflow to automate the data cleansing and transformation processes.
- Containerization: Docker containerization of Apache Airflow for enhanced scalability and ease of deployment.
- Data Storage: Efficient data loading into a Snowflake data warehouse, with a structured approach for both staging and final data storage.
- Data Analysis: Advanced statistical reporting and analysis using Power BI, connected directly to Snowflake.

