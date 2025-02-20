# MTGO Vintage Metagame Data

## Overview

This project is an ETL (Extract, Transform, Load) pipeline designed to process match results for Vintage tournaments on Magic Online (MTGO). The pipeline pulls data from a public Google Sheet, cleans and transforms it, then loads it into a PostgreSQL database. The processed data is then used to support a public REST API for community access and data analysis.

## Process

- **Extract** data from a publicly maintained Google Sheet.
- **Clean & Transform** tournament results, matchups, and deck information.
- **Load** structured data into a PostgreSQL database.
- **Support** a public REST API for querying tournament results and statistics.

## Data Sources

- [**Google Sheet Link**](): Community-collated tournament results, matchups, and deck archetypes.

## Database Schema

The data is loaded into a PostgreSQL database with the following tables:

- **EVENTS**: Captures individual tournament events.
- **EVENT_REJECTIONS**: Tracks rejected events and reason text.
- **MATCHES**: Stores match results, player deck IDs, and outcomes.
- **MATCH_REJECTIONS**: Tracks rejected matches and reason text.
- **VALID_DECKS**: Classification table storing valid deck archetypes.
- **VALID_EVENT_TYPES**: Classification table containing valid event type names.
- **LOAD_REPORTS**: Logs ETL process execution details.

The [**Data Dictionary**](https://github.com/cderickson/Vintage-Metagame-API/wiki/Data-Dictionary) contains feature definitions.

## Deployment & Architecture

This project is deployed in AWS and operates using the following services:

### **ETL Pipeline Execution**
- **AWS Lambda**: The ETL script runs as a scheduled Lambda function.
- **Amazon EventBridge**: A weekly scheduled rule triggers the Lambda function to refresh the data.

### **API Deployment**
- **AWS Lambda**: A separate Lambda function serves as the backend for the REST API.
- **Amazon API Gateway**: Provides an HTTP endpoint for querying processed tournament data.

### **Infrastructure Diagram**

Google Sheets → AWS Lambda (ETL) → PostgreSQL (RDS) → AWS Lambda (API) → API Gateway → Users
