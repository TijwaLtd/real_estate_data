import pandas as pd
import os
import glob
from typing import List, Dict, Set
import hashlib
import logging
from concurrent.futures import ProcessPoolExecutor
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealEstateDataProcessor:
    """Process multiple real estate CSV files into a single cleaned output format."""
    
    def __init__(self):
        """
        Initialize the processor with the default output columns.
        """
        self.output_columns = [
            'Street Address', 'Unit #', 'City', 'State', 'Postal Code', 
            'First Name', 'Last Name', 'Mailing Address', 'Mailing Unit #',
            'Mailing City', 'Mailing State', 'Mailing Zip', 'Property Type',
            'Bedrooms', 'Total Bathrooms', 'Building Sqft', 'Lot Size Sqft',
            'Est. Value', 'Phone 1', 'Phone 2', 'Phone 3', 'Phone 4', 'Phone 5',
            'Email', 'Email 2', 'Email 3', 'Email 4', 'Email 5'
        ]
        self.unique_records = set()  
        self.all_data = []  
            
    def _generate_unique_key(self, row: Dict) -> str:
        """
        Generate a unique hash key for a record to identify duplicates.
        Uses property address + owner name as the unique identifier.
        """
        # Combine address fields and owner name for uniqueness
        key_parts = []
        
        # Address components
        address_parts = [
            str(row.get('Street Address', '')),
            str(row.get('City', '')),
            str(row.get('State', '')),
            str(row.get('Postal Code', ''))
        ]
        key_parts.extend([part.lower().strip() for part in address_parts if part])
        
        # Owner components
        owner_parts = [
            str(row.get('First Name', '')),
            str(row.get('Last Name', ''))
        ]
        key_parts.extend([part.lower().strip() for part in owner_parts if part])
        
        # Create a hash of the combined key
        key_string = '|'.join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
            
    def _map_input_to_output(self, input_row: pd.Series) -> Dict:
        """Map fields from input CSV format to output format."""
        output_row = {}
        
        # Map address fields
        output_row['Street Address'] = input_row.get('Address', '')
        output_row['City'] = input_row.get('City', '')
        output_row['State'] = input_row.get('State', '')
        output_row['Postal Code'] = input_row.get('Zip', '')
        
        # Map owner/contact information
        output_row['First Name'] = input_row.get('Owner 1 First Name', '')
        output_row['Last Name'] = input_row.get('Owner 1 Last Name', '')
        
        # Map mailing address
        output_row['Mailing Address'] = input_row.get('Owner Mailing Address', '')
        output_row['Mailing City'] = input_row.get('Owner Mailing City', '')
        output_row['Mailing State'] = input_row.get('Owner Mailing State', '')
        output_row['Mailing Zip'] = input_row.get('Owner Mailing Zip', '')
        
        # Map property details
        output_row['Property Type'] = input_row.get('Property Type', '')
        output_row['Bedrooms'] = input_row.get('Bedrooms', '')
        output_row['Total Bathrooms'] = input_row.get('Bathrooms', '')
        output_row['Building Sqft'] = input_row.get('Living Square Feet', '')
        output_row['Lot Size Sqft'] = input_row.get('Lot (Square Feet)', '')
        
        # Add estimated value (doesn't exist in input, so leave blank)
        output_row['Est. Value'] = ''
        
        # Map contact information
        phone_fields = ['Phone 1', 'Phone 2', 'Phone 3', 'Phone 4', 'Phone 5']
        email_fields = ['Email', 'Email 2', 'Email 3', 'Email 4', 'Email 5']
        
        # Extract phone numbers
        phone_values = [
            input_row.get('Phone 1', ''),
            input_row.get('Phone 2', ''),
            input_row.get('Phone 3', '')
        ]
        
        # Filter out empty phone values
        phone_values = [p for p in phone_values if p]
        
        # Map available phone numbers to output fields
        for i, phone in enumerate(phone_values[:5]):  # Limit to 5 phone numbers
            output_row[phone_fields[i]] = phone
            
        # Fill remaining phone fields with empty strings
        for i in range(len(phone_values), 5):
            output_row[phone_fields[i]] = ''
            
        # Map email
        email_value = input_row.get('Email', '')
        if email_value:
            output_row['Email'] = email_value
        else:
            output_row['Email'] = ''
            
        # Fill remaining email fields with empty strings
        for i in range(1, 5):
            output_row[email_fields[i]] = ''
            
        # Add Unit # field (not in input data)
        output_row['Unit #'] = ''
        output_row['Mailing Unit #'] = ''
        
        return output_row
    
    def _has_contact_info(self, row: Dict) -> bool:
        """Check if a record has enough contact information to be useful."""
        # Check if any phone number exists
        has_phone = any(row.get(f'Phone {i}', '') for i in range(1, 6))
        
        # Check if any email exists
        has_email = any(row.get(f'Email {i}', '') for i in range(1, 6))
        
        # Check if mailing address exists
        has_mailing = bool(row.get('Mailing Address', ''))
        
        return has_phone or has_email or has_mailing
    
    def process_file(self, file_path: str) -> pd.DataFrame:
        """
        Process a single CSV file and return transformed data.
        
        Args:
            file_path: Path to the input CSV file
            
        Returns:
            DataFrame with processed data
        """
        try:
            logger.info(f"Processing file: {file_path}")
            start_time = time.time()
            
            # Read the CSV file
            input_df = pd.read_csv(file_path, low_memory=False)
            file_records = []
            
            # Process each row
            for _, row in input_df.iterrows():
                processed_row = self._map_input_to_output(row)
                
                # Only keep records with contact information
                if self._has_contact_info(processed_row):
                    file_records.append(processed_row)
            
            logger.info(f"Processed {len(file_records)} records from {file_path} in {time.time() - start_time:.2f} seconds")
            return pd.DataFrame(file_records)
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return pd.DataFrame(columns=self.output_columns)
    
    def process_files(self, input_files: List[str], max_workers: int = None) -> pd.DataFrame:
        """
        Process multiple CSV files in parallel and return combined results.
        
        Args:
            input_files: List of paths to input CSV files
            max_workers: Maximum number of parallel workers
            
        Returns:
            DataFrame with combined processed data
        """
        all_processed_data = []
        
        # Process files in parallel using ProcessPoolExecutor
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.process_file, input_files))
            
        # Combine results
        for df in results:
            if not df.empty:
                all_processed_data.append(df)
        
        if not all_processed_data:
            logger.warning("No valid data found in any input files")
            return pd.DataFrame(columns=self.output_columns)
            
        # Combine all dataframes
        combined_df = pd.concat(all_processed_data, ignore_index=True)
        
        # Remove duplicates
        logger.info(f"Before deduplication: {len(combined_df)} records")
        
        # Create a unique ID for each record based on address and owner
        combined_df['_unique_id'] = combined_df.apply(
            lambda row: self._generate_unique_key({k: v for k, v in row.items()}), 
            axis=1
        )
        
        # Keep the first occurrence of each unique record
        deduplicated_df = combined_df.drop_duplicates(subset=['_unique_id'], keep='first')
        
        # Drop the temporary ID column
        deduplicated_df = deduplicated_df.drop(columns=['_unique_id'])
        
        logger.info(f"After deduplication: {len(deduplicated_df)} records")
        
        # Ensure output has all required columns in the correct order
        for col in self.output_columns:
            if col not in deduplicated_df.columns:
                deduplicated_df[col] = ''
                
        # Reorder columns to match output template
        deduplicated_df = deduplicated_df[self.output_columns]
        
        return deduplicated_df

def main():
    """
    Main function to process all CSV files in a directory.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Process real estate CSV files into a unified format")
    parser.add_argument("--input-dir", required=True, help="Directory containing input CSV files")
    parser.add_argument("--output-path", required=True, help="Path to save the output CSV")
    parser.add_argument("--max-workers", type=int, default=None, 
                        help="Maximum number of parallel workers (default: number of CPU cores)")
    
    args = parser.parse_args()
    
    start_time = time.time()
    input_dir = args.input_dir
    output_path = args.output_path
    max_workers = args.max_workers
    
    logger.info(f"Starting processing for files in {input_dir}")
    
    # Get all CSV files in the input directory
    input_files = glob.glob(os.path.join(input_dir, "*.csv"))
    
    if not input_files:
        logger.error(f"No CSV files found in {input_dir}")
        return
        
    logger.info(f"Found {len(input_files)} CSV files to process")
    
    # Initialize processor
    processor = RealEstateDataProcessor()
    
    # Process all files
    result_df = processor.process_files(input_files, max_workers=max_workers)
    
    # Save to output file
    result_df.to_csv(output_path, index=False)
    
    logger.info(f"Processing complete. Output saved to {output_path}")
    logger.info(f"Total execution time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Processed {len(result_df)} unique records from {len(input_files)} files")

if __name__ == "__main__":
    main()