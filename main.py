# main.py
from scripts.extract import extract_cash_receipts
from scripts.transform import transform_cash_receipts, transform_cash_receipts_dues
from scripts.load import load_to_excel
from scripts.logging_setup import logger
import time

def main():
    """Main ETL process"""
    try:
        start_time = time.time()
        logger.info("Starting ETL process")
        
        # Extract
        logger.info("Extract phase started")
        raw_data = extract_cash_receipts()
        logger.info("Extract phase completed")
        
        # Transform - Original transformation (deduplicate by Invoice_ID)
        logger.info("Transform phase started for CashReceipts")
        transformed_data = transform_cash_receipts(raw_data)
        logger.info("Transform phase completed for CashReceipts")
        
        # Transform - New dues transformation (filter by Line_Item and deduplicate by BID)
        logger.info("Transform phase started for Dues/Enrollment")
        transformed_dues_data = transform_cash_receipts_dues(raw_data)
        logger.info("Transform phase completed for Dues/Enrollment")
        
        # Load - Both transformations to separate sheets
        logger.info("Load phase started")
        dataframes_dict = {
            'CashReceipts': transformed_data,
            'DuesAndEnrollment': transformed_dues_data
        }
        load_to_excel(dataframes_dict)
        logger.info("Load phase completed")
        
        execution_time = time.time() - start_time
        logger.info(f"ETL process completed successfully in {execution_time:.2f} seconds")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    main()