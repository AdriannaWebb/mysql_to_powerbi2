# scripts/transform.py
import pandas as pd
from scripts.logging_setup import logger

def transform_cash_receipts(df):
    """Transform the CashReceipts data by deduplicating based on Invoice_ID"""
    try:
        logger.info("Starting data transformation")
        
        # Create a copy to avoid modifying the original dataframe
        transformed_df = df.copy()
        
        # Convert Payment_Date to datetime if it's not already
        transformed_df['Payment_Date'] = pd.to_datetime(transformed_df['Payment_Date'])
        
        # Get the original row count before deduplication
        original_row_count = len(transformed_df)
        logger.info(f"Original row count: {original_row_count}")
        
        # Sort by Invoice_ID and Payment_Date (descending)
        transformed_df = transformed_df.sort_values(['Invoice_ID', 'Payment_Date'], ascending=[True, False])
        
        # Keep the first occurrence of each Invoice_ID (which will be the most recent due to sorting)
        transformed_df = transformed_df.drop_duplicates(subset=['Invoice_ID'], keep='first')
        
        # Get the new row count after deduplication
        new_row_count = len(transformed_df)
        removed_rows = original_row_count - new_row_count
        
        logger.info(f"Transformation complete. Removed {removed_rows} duplicate rows.")
        logger.info(f"New shape after deduplication: {transformed_df.shape}")
        
        return transformed_df
        
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise

def transform_cash_receipts_dues(df):
    """
    Transform the CashReceipts data by:
    1. Filtering for rows with 'Dues' or 'Enrollment Fee' in Line_Item
    2. Deduplicating based on BID, keeping the most recent Payment_Date
    """
    try:
        logger.info("Starting dues/enrollment fees data transformation")
        
        # Create a copy to avoid modifying the original dataframe
        transformed_df = df.copy()
        
        # Convert Payment_Date to datetime if it's not already
        transformed_df['Payment_Date'] = pd.to_datetime(transformed_df['Payment_Date'])
        
        # Get the original row count before filtering
        original_row_count = len(transformed_df)
        logger.info(f"Original row count: {original_row_count}")
        
        # Filter for rows containing 'Dues' or 'Enrollment Fee' in Line_Item
        dues_mask = transformed_df['Line_Item'].str.contains('Dues', case=False, na=False)
        enrollment_mask = transformed_df['Line_Item'].str.contains('Enrollment Fee', case=False, na=False)
        transformed_df = transformed_df[dues_mask | enrollment_mask]
        
        # Get the row count after filtering
        filtered_row_count = len(transformed_df)
        filtered_rows = original_row_count - filtered_row_count
        logger.info(f"Filtered {filtered_rows} rows, keeping only dues and enrollment fees")
        
        # Sort by BID and Payment_Date (descending)
        transformed_df = transformed_df.sort_values(['bid', 'Payment_Date'], ascending=[True, False])
        
        # Keep the first occurrence of each BID (which will be the most recent due to sorting)
        deduped_df = transformed_df.drop_duplicates(subset=['bid'], keep='first')
        
        # Get the new row count after deduplication
        new_row_count = len(deduped_df)
        removed_rows = filtered_row_count - new_row_count
        logger.info(f"Removed {removed_rows} duplicate BIDs, keeping most recent payment date")
        logger.info(f"New shape after filtering and deduplication: {deduped_df.shape}")
        
        return deduped_df
        
    except Exception as e:
        logger.error(f"Error during dues transformation: {e}")
        raise
