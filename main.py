# main.py
from scripts.extract import extract_cash_receipts
from scripts.transform import transform_cash_receipts, transform_cash_receipts_dues, transform_cash_receipts_by_invoice
from scripts.load import load_to_excel, load_to_invoice_excel
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
        
        # Transform - Invoice-based transformation (group by Invoice_ID with counts and sums)
        logger.info("Transform phase started for Invoice Summary")
        invoice_summary_data = transform_cash_receipts_by_invoice(raw_data)
        logger.info("Transform phase completed for Invoice Summary")
        
        # Load - Both original transformations to separate sheets in one Excel file
        logger.info("Load phase started for main Excel file")
        dataframes_dict = {
            'CashReceipts': transformed_data,
            'DuesAndEnrollment': transformed_dues_data
        }
        load_to_excel(dataframes_dict)
        logger.info("Load phase completed for main Excel file")
        
        # Load - Invoice summary to a separate Excel file
        logger.info("Load phase started for Invoice Summary Excel file")
        load_to_invoice_excel(invoice_summary_data)
        logger.info("Load phase completed for Invoice Summary Excel file")
        
        execution_time = time.time() - start_time
        logger.info(f"ETL process completed successfully in {execution_time:.2f} seconds")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    main()