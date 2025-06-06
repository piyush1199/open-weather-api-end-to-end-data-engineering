# Open Weather End-to-End Data Engineering Project

## Project Overview

This project demonstrates building a data pipeline for extracting, processing, and analyzing live weather data using AWS services. The pipeline collects data from OpenWeather's API and processes it through AWS services for efficient querying and analysis.

## Architecture Components
## Architecture Diagram

![Open Weather Data Pipeline Architecture](https://github.com/piyush1199/open-weather-api-end-to-end-data-engineering/blob/main/Open%20Weather%20End-to-End%20Data%20Engineering%20Project%20-%20visual%20selection.png?raw=true)

*Figure 1: data pipeline architecture*

![Open Weather Data Pipeline Architecture](https://github.com/piyush1199/open-weather-api-end-to-end-data-engineering/blob/main/Architecture.jpg?raw=true)

*Figure 2: End-to-end data pipeline architecture for weather data processing*
## Components

### 1. Data Extraction
- **AWS Lambda**: Daily triggered function that fetches weather data from OpenWeather API
- **AWS CloudWatch**: Scheduler that triggers the Lambda function at specified intervals
- **Amazon S3**: Storage for raw API response data

### 2. Data Processing
- **Current Weather Lambda**: Extracts current weather for 5 major Australian cities
- **Forecast Lambda**: Extracts 3-day weather forecasts for the same cities
- **Processed Data Storage**: Results stored back in S3 in organized structure

### 3. Data Catalog & Querying
- **AWS Glue Crawler**: Creates data catalog from S3 storage
- **AWS Athena**: Enables SQL querying of processed weather data

## Implementation Steps

1. **API Extraction Setup**
   - Configure OpenWeather API credentials
   - Create CloudWatch trigger for daily execution
   - Implement Lambda function for API calls

2. **Data Processing**
   - Develop Lambda functions for current weather and forecast extraction
   - Configure S3 bucket structure for raw and processed data

3. **Data Catalog & Analysis**
   - Set up Glue Crawler to scan processed data
   - Configure Athena tables for querying
   - Implement sample queries for analysis

## Pipeline Workflow

1. **Daily Trigger**  
   CloudWatch → Lambda → OpenWeather API call
2. **Raw Storage**  
   API response → S3 bucket (raw zone)
3. **Data Processing**  
   S3 event → Current Weather Lambda + Forecast Lambda
4. **Processed Storage**  
   Cleaned data → S3 bucket (processed zone)
5. **Data Catalog**  
   Glue Crawler → Creates schema/metadata
6. **Analysis**  
   Athena SQL queries → Business insights
