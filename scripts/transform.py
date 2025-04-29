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
def transform_cash_receipts_by_invoice(df):
    """
    Transform the CashReceipts data by:
    1. Grouping by Invoice_ID
    2. Calculating receipt count, total amount paid, and latest payment date for each invoice
    """
    try:
        logger.info("Starting invoice-based transformation")
        
        # Create a copy to avoid modifying the original dataframe
        df_copy = df.copy()
        
        # Convert Payment_Date to datetime if it's not already
        df_copy['Payment_Date'] = pd.to_datetime(df_copy['Payment_Date'])
        
        # Get the original row count
        original_row_count = len(df_copy)
        logger.info(f"Original row count: {original_row_count}")
        
        # Count total distinct Invoice_IDs including nulls
        total_invoices = df_copy['Invoice_ID'].nunique(dropna=False)
        logger.info(f"Total distinct Invoice_IDs (including nulls): {total_invoices}")
        
        # Count non-null Invoice_IDs
        non_null_invoices = df_copy.dropna(subset=['Invoice_ID'])['Invoice_ID'].nunique()
        logger.info(f"Non-null distinct Invoice_IDs: {non_null_invoices}")
        
        # Check for Invoice_IDs that map to multiple BIDs
        invoice_bid_pairs = df_copy.groupby('Invoice_ID')['bid'].nunique()
        multi_bid_invoices = invoice_bid_pairs[invoice_bid_pairs > 1].count()
        logger.info(f"Number of Invoice_IDs that map to multiple BIDs: {multi_bid_invoices}")
        
        if multi_bid_invoices > 0:
            # Log some examples
            problematic_invoices = invoice_bid_pairs[invoice_bid_pairs > 1].index.tolist()[:5]  # First 5 examples
            logger.info(f"Examples of Invoice_IDs with multiple BIDs: {problematic_invoices}")
            
            # For the first example, show the different BIDs
            if problematic_invoices:
                first_problem = problematic_invoices[0]
                bids_for_invoice = df_copy[df_copy['Invoice_ID'] == first_problem]['bid'].unique()
                logger.info(f"For Invoice_ID {first_problem}, the BIDs are: {bids_for_invoice}")
        
        # Create a groupby object on Invoice_ID
        grouped = df_copy.groupby('Invoice_ID', dropna=False)
        
        # Prepare the results dataframe
        results = []
        skipped_count = 0
        
        for invoice_id, group in grouped:
            # Skip if Invoice_ID is null or empty
            if pd.isna(invoice_id) or invoice_id == '':
                skipped_count += 1
                continue
                
            # Get the first BID (should be the same for all rows with the same Invoice_ID)
            bid = group['bid'].iloc[0]
            
            # Count the number of receipts for this invoice
            receipt_count = len(group)
            
            # Calculate the total amount paid
            total_amount_paid = group['Payment_Amount'].sum()
            
            # Find the most recent payment date
            latest_payment_date = group['Payment_Date'].max()
            
            # Get the Plan_Type (should be the same for all rows with the same Invoice_ID)
            plan_type = group['Plan_Type'].iloc[0]
            
            results.append({
                'BID': bid,
                'Invoice_ID': invoice_id,
                'Receipt_Count': receipt_count,
                'Total_Amount_Paid': total_amount_paid,
                'Latest_Payment_Date': latest_payment_date,
                'Plan_Type': plan_type
            })
        
        # Create a new dataframe from the results
        invoice_df = pd.DataFrame(results)
        
        # Log the results
        new_row_count = len(invoice_df)
        logger.info(f"Skipped {skipped_count} null/empty Invoice_IDs")
        logger.info(f"Created invoice summary with {new_row_count} unique invoices")
        logger.info(f"New shape after invoice grouping: {invoice_df.shape}")
        
        return invoice_df
        
    except Exception as e:
        logger.error(f"Error during invoice-based transformation: {e}")
        raise