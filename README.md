## Dock Financial Data Pipelines

Automated pipeline for generating and processing Dock balance reports using Apache Airflow, SFTP, AWS S3, and Lambda.

### Overview

This repository contains data pipelines developed using Apache Airflow to automate data extraction, transfer, and processing. These pipelines manage balance reports and statements, ensuring that data is organized and accessible for further analysis.

- Extracts financial balance reports, transfers them via SFTP to AWS S3, and unzips files using AWS Lambda.

### Prerequisites

#### Required Configurations
1. Airflow Variables:
Ensure the following variables are configured in the `Admin > Variables` menu:

- For both pipelines:
  - `DOCK_CLIENT_ID`: Client ID for Dock API authentication.
  - `DOCK_SECRET`: Secret key for authentication.
  - `DOCK_AUTH_URL`: URL for Dock API authentication.
  - `AWS_S3_BUCKET`: Name of the S3 bucket to store files.
- Balance Reports:
  - `DOCK_TRANSACTIONS_URL`: Base URL for balance report requests.
- Digital Account Statements:
  - `DOCK_DIGITAL_ACCOUNT_ID`: Digital account ID.
  - `DOCK_DIGITAL_ACCOUNTS_URL`: Base URL for digital account statement requests.

2. Airflow Connections:
- SFTP Connection:
  - ID: `dock_ssh`
  - Configured for accessing Dock's SFTP server.
- AWS Connection:
  - Valid configuration for accessing AWS S3 and Lambda.

3. AWS Infrastructure:
- A configured S3 bucket to store files.
- An AWS Lambda function named `dock_unzip_files` to unzip ZIP files in S3.

4. Python Dependencies: Install the required packages:

`pip install apache-airflow apache-airflow-providers-amazon apache-airflow-providers-sftp requests`

### Pipelines

- **Pipeline 1: Balance Reports**

- **1. Workflow:**
  - Authenticate with the Dock API.
  - Request a balance report and retrieve the ticket.
  - Check if the file is available on the SFTP server.
  - Transfer the file to S3.
  - Unzip ZIP files in S3 using AWS Lambda.

- **Pipeline 2: Account Statements**

- **2. Workflow:**
  - Authenticate with the Dock API.
  - Request account statements for the previous day.
  - Check if the file is available on the SFTP server.
  - Transfer the file to S3.
  - Unzip ZIP files in S3 using AWS Lambda.

### Testing

1. Test each function individually to ensure API, SFTP, and S3 connectivity:
2. Verify DAG execution in Airflow and monitor the logs for each task.

### Contribution to Data Engineering

- End-to-End Automation: Automates the lifecycle of financial data, from API requests to unzipping in S3.
- Scalability: Flexible architecture allows for expansion to other types of reports.
- Monitoring: Detailed logs and Airflow task tracking ensure transparency across all steps.

### Best Practices

**1. Credential Management:**
- Use Airflow variables or AWS Secrets Manager to secure sensitive information.

**2. Log Monitoring:**
- Always review Airflow logs to identify failures or inconsistencies.

**3. Regular Testing:**
- Test each pipeline component individually before making significant changes.

**4. Parameterized Configurations:**
- Ensure URLs, credentials, and other parameters are dynamically configured using variables.

