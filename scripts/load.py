# scripts/load.py
import pandas as pd
from config import OUTPUT_FILE
from scripts.logging_setup import logger

def load_to_excel(df):
    """Export the transformed dataframe to Excel"""
    try:
        logger.info(f"Saving data to Excel file: {OUTPUT_FILE}")
        
        # Create Excel writer
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            # Convert to Excel without index
            df.to_excel(writer, index=False, sheet_name='CashReceipts')
            
        logger.info(f"Data successfully saved to {OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Error saving data to Excel: {e}")
        raise

def load_to_csv(df, output_file=None):
    """Export the transformed dataframe to CSV (alternative option)"""
    try:
        # Use the default output file if none specified
        if output_file is None:
            output_file = str(OUTPUT_FILE).replace('.xlsx', '.csv')
            
        logger.info(f"Saving data to CSV file: {output_file}")
        
        # Save to CSV without index
        df.to_csv(output_file, index=False)
            
        logger.info(f"Data successfully saved to {output_file}")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")
        raise

def load_to_excel(dataframes_dict):
    """
    Export multiple transformed dataframes to separate sheets in a single Excel file
    
    Args:
        dataframes_dict: Dictionary of {sheet_name: dataframe} pairs
    """
    try:
        logger.info(f"Saving data to Excel file: {OUTPUT_FILE}")
        
        # Create Excel writer
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            # Write each dataframe to a separate sheet
            for sheet_name, df in dataframes_dict.items():
                logger.info(f"Writing sheet: {sheet_name} with {len(df)} rows")
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                
        logger.info(f"Data successfully saved to {OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Error saving data to Excel: {e}")
        raise    