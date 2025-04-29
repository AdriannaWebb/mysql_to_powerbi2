# MySQL to Power BI ETL Pipeline

## Project Overview

This project is an ETL (Extract, Transform, Load) pipeline designed for the Better Business Bureau of Nebraska to extract cash receipts data from a MySQL database, transform it for analysis, and load it into Excel files that can be utilized by Power BI. The pipeline creates several outputs including a main Excel file with multiple sheets and a specialized invoice summary file.

## Features

- Extracts data from the CashReceipts MySQL table
- Performs multiple data transformations:
    - Deduplication of cash receipts by Invoice_ID
    - Filtering and deduplication of dues and enrollment fees by business ID (BID)
    - Aggregation of receipts by Invoice_ID with count, sum, and latest payment date
- Generates multiple output files:
    - `transformed_data.xlsx` with multiple sheets
    - `invoice_summary.xlsx` with aggregate invoice data
- Comprehensive logging system
- Configurable through environment variables

## Project Structure

```

mysql_to_powerbi/
├── .env (not tracked in git)
├── .gitignore
├── config.py
├── main.py
├── requirements.txt
├── run_etl.bat
├── test_extract.py
├── logs/
│   └── process.log
├── output/ (not tracked in git)
│   ├── transformed_data.xlsx
│   └── invoice_summary.xlsx
└── scripts/
    ├── __pycache__/
    ├── extract.py
    ├── load.py
    ├── logging_setup.py
    └── transform.py

```

## Prerequisites

- Python 3.7+
- MySQL database with CashReceipts table
- Required Python packages (see requirements.txt)

## Installation

1. Clone this repository to your local machine
2. Install required packages: `pip install -r requirements.txt`
3. Create a `.env` file in the root directory with the following variables:
    
    ```
    
    DB_HOST=your_database_host
    DB_USER=your_database_user
    DB_PASSWORD=your_database_password
    DB_NAME=your_database_name
    DB_PORT=your_database_port
    
    ```
    
4. Create the output folder: `mkdir output`

## Usage

### Running the ETL process

To run the ETL process, execute:

```

python main.py

```

Alternatively, you can use the batch file on Windows:

```

run_etl.bat

```

### Testing the database connection

To test the database connection and basic extraction:

```

python test_extract.py

```

## Output Files

### transformed_data.xlsx

Contains two sheets:

- **CashReceipts**: All cash receipts deduplicated by Invoice_ID, keeping the most recent payment
- **DuesAndEnrollment**: Only rows with dues or enrollment fees, deduplicated by BID

### invoice_summary.xlsx

Contains a single sheet with aggregate invoice data:

- BID: Business ID
- Invoice_ID: Unique invoice identifier
- Receipt_Count: Number of payments made towards this invoice
- Total_Amount_Paid: Sum of all payments made towards this invoice
- Latest_Payment_Date: Most recent payment date for this invoice
- Plan_Type: Type of payment plan associated with the invoice

## Logging

Logs are stored in the `logs/process.log` file and also displayed in the console. The log includes information about each step of the ETL process, including counts of rows processed and any errors encountered.

## Contributing

1. Create a feature branch
2. Make your changes
3. Test thoroughly
4. Commit and push your changes
5. Create a pull request

## Security Notes

- Database credentials are stored in the `.env` file which is not tracked in git
- Output files containing sensitive business data are not tracked in git
- Always ensure proper access controls for the output directory

## Troubleshooting

- Check the logs at `logs/process.log` for detailed error information
- Ensure your database connection details in the `.env` file are correct
- Verify that you have write permissions to the output directory