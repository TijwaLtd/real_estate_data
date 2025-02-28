import pandas as pd
import os
import glob
import io
import tempfile
import hashlib
import logging
from concurrent.futures import ProcessPoolExecutor
import time
import re
from typing import List, Dict, Set, Union, Tuple, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealEstateDataProcessor:
    """Process multiple real estate files (CSV/XLSX) into a single cleaned output format."""
    
    def __init__(self):
        """
        Initialize the processor with the default output columns and column mappings.
        """
        self.output_columns = [
            'Street Address', 'Unit #', 'City', 'State', 'Postal Code', 
            'First Name', 'Last Name', 'Mailing Address', 'Mailing Unit #',
            'Mailing City', 'Mailing State', 'Mailing Zip', 'Property Type',
            'Bedrooms', 'Total Bathrooms', 'Building Sqft', 'Lot Size Sqft',
            'Est. Value', 'Phone 1', 'Phone 2', 'Phone 3', 'Phone 4', 'Phone 5',
            'Email', 'Email 2', 'Email 3', 'Email 4', 'Email 5'
        ]
        
        # Define possible column name variations for different data sources
        self.column_mappings = {
            # Address fields
            'Street Address': [
                'Street Address', 'Address', 'Property Address', 'PROPERTY ADDRESS'
            ],
            'Unit #': [
                'Unit #', 'Unit', 'APT', 'APARTMENT', 'UNIT NUMBER'
            ],
            'City': [
                'City', 'Property City', 'PROPERTY CITY', 'CITY'
            ],
            'State': [
                'State', 'Property State', 'PROPERTY STATE', 'ST', 'STATE'
            ],
            'Postal Code': [
                'Postal Code', 'Zip', 'Property Zip', 'PROPERTY ZIP', 'ZIP', 'ZIP CODE'
            ],
            
            # Owner fields
            'First Name': [
                'First Name', 'Owner 1 First Name', 'OWNER FIRST NAME', 'FIRST NAME'
            ],
            'Last Name': [
                'Last Name', 'Owner 1 Last Name', 'OWNER LAST NAME', 'LAST NAME'
            ],
            
            # Owner fields patterns (for finding alternate owner fields)
            'Owner First Name Pattern': [
                r'Owner (\d+) First Name',
                r'Owner(\d+) First Name',
                r'OWNER(\d+)_FIRST_NAME'
            ],
            'Owner Last Name Pattern': [
                r'Owner (\d+) Last Name',
                r'Owner(\d+) Last Name',
                r'OWNER(\d+)_LAST_NAME'
            ],
            
            # Mailing address fields
            'Mailing Address': [
                'Mailing Address', 'Owner Mailing Address', 'MAILING ADDRESS', 'MAIL ADDRESS'
            ],
            'Mailing Unit #': [
                'Mailing Unit #', 'Mailing Unit', 'MAILING APT', 'MAIL UNIT'
            ],
            'Mailing City': [
                'Mailing City', 'Owner Mailing City', 'MAILING CITY', 'MAIL CITY'
            ],
            'Mailing State': [
                'Mailing State', 'Owner Mailing State', 'MAILING STATE', 'MAIL STATE'
            ],
            'Mailing Zip': [
                'Mailing Zip', 'Owner Mailing Zip', 'MAILING ZIP', 'MAIL ZIP'
            ],
            
            # Property details
            'Property Type': [
                'Property Type', 'Land Use', 'PROPERTY TYPE', 'TYPE'
            ],
            'Bedrooms': [
                'Bedrooms', 'Bedroom Count', 'BEDROOMS', 'BED', 'BR'
            ],
            'Total Bathrooms': [
                'Total Bathrooms', 'Bathroom Count', 'Bathrooms', 'BATHROOMS', 'BATH', 'BA'
            ],
            'Building Sqft': [
                'Building Sqft', 'Living Square Feet', 'Total Building Area Square Feet', 
                'BUILDING SQFT', 'SQ FT', 'SQFT'
            ],
            'Lot Size Sqft': [
                'Lot Size Sqft', 'Lot (Square Feet)', 'Lot Size Square Feet', 
                'LOT SIZE', 'LOT SQFT'
            ],
            
            # Value
            'Est. Value': [
                'Est. Value', 'Total Assessed Value', 'ASSESSED VALUE', 'VALUE'
            ],
            
            # Contact information - Phones
            'Phone Patterns': [
                r'Phone (\d+)',
                r'Phone(\d+)',
                r'Cell (\d+)',
                r'Cell(\d+)',
                r'Landline (\d+)',
                r'Landline(\d+)',
                r'PHONE(\d+)',
                r'CELL(\d+)'
            ],
            
            # Contact information - Emails
            'Email Patterns': [
                r'Email (\d+)',
                r'Email(\d+)',
                r'Email(\d+)_x',
                r'EMAIL(\d+)'
            ]
        }
        
        self.unique_records = set()
    
    def _find_matching_column(self, df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """
        Find a matching column in the DataFrame from a list of possible column names.
        
        Args:
            df: Input DataFrame
            possible_names: List of possible column names
            
        Returns:
            Matching column name or None if no match found
        """
        for name in possible_names:
            if name in df.columns:
                return name
        return None
    
    def _find_pattern_columns(self, df: pd.DataFrame, patterns: List[str]) -> Dict[str, str]:
        """
        Find columns that match patterns (like Phone 1, Phone 2, etc.).
        
        Args:
            df: Input DataFrame
            patterns: List of regex patterns
            
        Returns:
            Dictionary mapping normalized names to actual column names
        """
        matches = {}
        
        for col in df.columns:
            for pattern in patterns:
                match = re.match(pattern, col)
                if match:
                    # Get the number part (1, 2, 3, etc.)
                    num = match.group(1) if len(match.groups()) > 0 else "1"
                    # Determine the column type (Phone or Email)
                    if 'phone' in pattern.lower() or 'cell' in pattern.lower() or 'landline' in pattern.lower():
                        key = f"Phone {num}"
                    elif 'email' in pattern.lower():
                        key = f"Email{' ' if num == '1' else ' '}{num}"
                    matches[key] = col
                    break
                    
        return matches
    
    def _split_name(self, full_name: str) -> Tuple[str, str]:
        """
        Split a full name into first and last name.
        
        Args:
            full_name: Full name as a string
            
        Returns:
            Tuple of (first_name, last_name)
        """
        if not full_name or pd.isna(full_name):
            return ('', '')
            
        parts = full_name.strip().split()
        
        if len(parts) == 1:
            return (parts[0], '')
        elif len(parts) == 2:
            return (parts[0], parts[1])
        else:
            # Handle multiple names by taking first name and the rest as last name
            return (parts[0], ' '.join(parts[1:]))
    
    def _extract_unit(self, address: str) -> Tuple[str, str]:
        """
        Extract unit information from an address.
        
        Args:
            address: Address string that might contain unit info
            
        Returns:
            Tuple of (address_without_unit, unit)
        """
        if not address or pd.isna(address):
            return ('', '')
            
        # Common unit designators
        unit_patterns = [
            r'(?:#|UNIT|APT|APARTMENT|STE|SUITE)\s*([A-Za-z0-9-]+)',  # #123, UNIT 123, APT 123
            r'\s+([A-Za-z0-9-]+)$'  # Address ending with unit e.g., "123 Main St 456"
        ]
        
        for pattern in unit_patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                unit = match.group(1)
                # Remove the unit from the address
                cleaned_address = re.sub(pattern, '', address, flags=re.IGNORECASE).strip()
                return (cleaned_address, unit)
                
        return (address, '')
    
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
    
    def _map_input_to_output(self, input_row: pd.Series, column_map: Dict[str, str]) -> Dict:
        """
        Map fields from input file format to output format.
        
        Args:
            input_row: Input row data
            column_map: Mapping of output columns to input columns
            
        Returns:
            Dictionary with mapped data
        """
        output_row = {col: '' for col in self.output_columns}
        
        # Process each output field using the column mapping
        for output_field in self.output_columns:
            if output_field in column_map and column_map[output_field] in input_row:
                output_row[output_field] = input_row[column_map[output_field]]
        
        # Handle special cases
        
        # Process combined names if needed
        if 'First Name' not in column_map and 'Last Name' not in column_map:
            # Look for a full name field
            full_name_fields = ['Owner Name', 'Owner', 'Full Name', 'Name']
            for field in full_name_fields:
                if field in input_row and not pd.isna(input_row[field]):
                    first, last = self._split_name(input_row[field])
                    output_row['First Name'] = first
                    output_row['Last Name'] = last
                    break
        
        # Extract unit from address if not separately provided
        if not output_row['Unit #'] and output_row['Street Address']:
            address, unit = self._extract_unit(output_row['Street Address'])
            if unit:
                output_row['Street Address'] = address
                output_row['Unit #'] = unit
        
        # Do the same for mailing address
        if not output_row['Mailing Unit #'] and output_row['Mailing Address']:
            address, unit = self._extract_unit(output_row['Mailing Address'])
            if unit:
                output_row['Mailing Address'] = address
                output_row['Mailing Unit #'] = unit
        
        # Ensure phones are mapped in order (1-5)
        phone_indexes = []
        for i in range(1, 6):
            key = f'Phone {i}'
            if key in column_map:
                phone_val = input_row.get(column_map[key], '')
                if phone_val and not pd.isna(phone_val):
                    phone_indexes.append((i, str(phone_val).strip()))
        
        # Reset all phone fields first
        for i in range(1, 6):
            output_row[f'Phone {i}'] = ''
            
        # Then fill them in order
        for i, (_, phone) in enumerate(sorted(phone_indexes), 1):
            if i <= 5:  # Limit to 5 phone numbers
                output_row[f'Phone {i}'] = phone
        
        # Similar approach for emails
        email_indexes = []
        for key in column_map:
            if key.startswith('Email'):
                email_val = input_row.get(column_map[key], '')
                if email_val and not pd.isna(email_val):
                    if key == 'Email':
                        idx = 1
                    else:
                        # Extract number from key like "Email 2"
                        idx_match = re.search(r'Email (\d+)', key)
                        idx = int(idx_match.group(1)) if idx_match else 1
                    email_indexes.append((idx, str(email_val).strip()))
        
        # Reset all email fields first
        email_fields = ['Email', 'Email 2', 'Email 3', 'Email 4', 'Email 5']
        for field in email_fields:
            output_row[field] = ''
            
        # Then fill them in order
        for i, (_, email) in enumerate(sorted(email_indexes), 1):
            if i <= 5:  # Limit to 5 emails
                if i == 1:
                    output_row['Email'] = email
                else:
                    output_row[f'Email {i}'] = email
        
        return output_row
    
    def _has_contact_info(self, row: Dict) -> bool:
        """Check if a record has enough contact information to be useful."""
        # Check if any phone number exists
        has_phone = any(row.get(f'Phone {i}', '') for i in range(1, 6))
        
        # Check if any email exists
        email_fields = ['Email', 'Email 2', 'Email 3', 'Email 4', 'Email 5']
        has_email = any(row.get(field, '') for field in email_fields)
        
        # Check if mailing address exists
        has_mailing = bool(row.get('Mailing Address', ''))
        
        return has_phone or has_email or has_mailing
    
    def _create_column_mapping(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Create a mapping between standardized column names and actual column names in the file.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary mapping output column names to input column names
        """
        mapping = {}
        
        # Find simple direct column matches
        for output_col, possible_names in self.column_mappings.items():
            if not output_col.endswith('Pattern'):  # Skip pattern fields
                matched_col = self._find_matching_column(df, possible_names)
                if matched_col:
                    mapping[output_col] = matched_col
        
        # Find pattern-based columns (Phone 1, Phone 2, Email 1, etc.)
        phone_patterns = self.column_mappings.get('Phone Patterns', [])
        email_patterns = self.column_mappings.get('Email Patterns', [])
        
        # Map phone columns
        phone_mapping = self._find_pattern_columns(df, phone_patterns)
        mapping.update(phone_mapping)
        
        # Map email columns
        email_mapping = self._find_pattern_columns(df, email_patterns)
        mapping.update(email_mapping)
        
        # Map owner fields for multiple owners
        owner_first_patterns = self.column_mappings.get('Owner First Name Pattern', [])
        owner_last_patterns = self.column_mappings.get('Owner Last Name Pattern', [])
        
        # Find all owner fields
        owner_columns = {}
        for col in df.columns:
            for pattern in owner_first_patterns:
                match = re.match(pattern, col)
                if match:
                    num = int(match.group(1))
                    if num not in owner_columns:
                        owner_columns[num] = {}
                    owner_columns[num]['first'] = col
                    break
                    
            for pattern in owner_last_patterns:
                match = re.match(pattern, col)
                if match:
                    num = int(match.group(1))
                    if num not in owner_columns:
                        owner_columns[num] = {}
                    owner_columns[num]['last'] = col
                    break
        
        # Use primary owner (owner 1) if available
        if 1 in owner_columns and 'first' in owner_columns[1] and 'last' in owner_columns[1]:
            mapping['First Name'] = owner_columns[1]['first']
            mapping['Last Name'] = owner_columns[1]['last']
        
        return mapping
    
    def process_file_content(self, file_content: bytes, file_name: str) -> pd.DataFrame:
        """
        Process file content in memory and return transformed data.
        
        Args:
            file_content: Binary content of the file
            file_name: Original file name (for logging and to determine file type)
            
        Returns:
            DataFrame with processed data
        """
        try:
            logger.info(f"Processing file: {file_name}")
            start_time = time.time()
            
            # Determine file type and read accordingly
            file_extension = os.path.splitext(file_name)[1].lower()
            
            # Read the file content into a DataFrame
            if file_extension == '.csv':
                # For CSV files, use pandas read_csv with binary input
                input_df = pd.read_csv(io.BytesIO(file_content), low_memory=False)
            elif file_extension in ['.xlsx', '.xls']:
                # For Excel files, use pandas read_excel with binary input
                input_df = pd.read_excel(io.BytesIO(file_content))
            else:
                logger.error(f"Unsupported file type: {file_extension}")
                return pd.DataFrame(columns=self.output_columns)
            
            # Create a column mapping for this specific file
            column_map = self._create_column_mapping(input_df)
            logger.info(f"Column mapping created for {file_name}: {column_map}")
            
            file_records = []
            
            # Process each row
            for _, row in input_df.iterrows():
                processed_row = self._map_input_to_output(row, column_map)
                
                # Only keep records with contact information
                if self._has_contact_info(processed_row):
                    file_records.append(processed_row)
            
            logger.info(f"Processed {len(file_records)} records from {file_name} in {time.time() - start_time:.2f} seconds")
            return pd.DataFrame(file_records)
            
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            return pd.DataFrame(columns=self.output_columns)
    
    def process_file(self, file_path: str) -> pd.DataFrame:
        """
        Process a single file from disk and return transformed data.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            DataFrame with processed data
        """
        try:
            # Read file content
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            # Process the file content
            return self.process_file_content(file_content, os.path.basename(file_path))
            
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return pd.DataFrame(columns=self.output_columns)
    
    def process_files(self, input_files: List[str], max_workers: int = None) -> pd.DataFrame:
        """
        Process multiple files in parallel and return combined results.
        
        Args:
            input_files: List of paths to input files
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
        
        return self._combine_and_deduplicate(all_processed_data)
    
    def process_file_objects(self, files: List[Tuple[bytes, str]], max_workers: int = None) -> pd.DataFrame:
        """
        Process multiple file objects in memory and return combined results.
        
        Args:
            files: List of tuples containing (file_content, file_name)
            max_workers: Maximum number of parallel workers
            
        Returns:
            DataFrame with combined processed data
        """
        all_processed_data = []
        
        # Process files using a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_paths = []
            
            # Save files to temporary location
            for i, (file_content, file_name) in enumerate(files):
                file_extension = os.path.splitext(file_name)[1]
                temp_file_path = os.path.join(temp_dir, f"temp_file_{i}{file_extension}")
                
                with open(temp_file_path, 'wb') as f:
                    f.write(file_content)
                
                temp_file_paths.append(temp_file_path)
            
            # Process the files
            return self.process_files(temp_file_paths, max_workers)
    
    def process_memory_files(self, files: List[Tuple[bytes, str]]) -> pd.DataFrame:
        """
        Process multiple file objects directly in memory (without saving to disk).
        
        Args:
            files: List of tuples containing (file_content, file_name)
            
        Returns:
            DataFrame with combined processed data
        """
        all_processed_data = []
        
        # Process each file in memory
        for file_content, file_name in files:
            df = self.process_file_content(file_content, file_name)
            if not df.empty:
                all_processed_data.append(df)
        
        return self._combine_and_deduplicate(all_processed_data)
    
    def _combine_and_deduplicate(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine multiple DataFrames and remove duplicates.
        
        Args:
            dataframes: List of DataFrames to combine
            
        Returns:
            Deduplicated combined DataFrame
        """
        if not dataframes:
            logger.warning("No valid data found in any input files")
            return pd.DataFrame(columns=self.output_columns)
            
        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True)
        
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

# Command-line functionality 
def main():
    """
    Main function to process all files in a directory.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Process real estate data files into a unified format")
    parser.add_argument("--input-dir", required=True, help="Directory containing input CSV/XLSX files")
    parser.add_argument("--output-path", required=True, help="Path to save the output CSV")
    parser.add_argument("--max-workers", type=int, default=None, 
                        help="Maximum number of parallel workers (default: number of CPU cores)")
    
    args = parser.parse_args()
    
    start_time = time.time()
    input_dir = args.input_dir
    output_path = args.output_path
    max_workers = args.max_workers
    
    logger.info(f"Starting processing for files in {input_dir}")
    
    # Get all CSV and Excel files in the input directory
    input_files = []
    input_files.extend(glob.glob(os.path.join(input_dir, "*.csv")))
    input_files.extend(glob.glob(os.path.join(input_dir, "*.xlsx")))
    input_files.extend(glob.glob(os.path.join(input_dir, "*.xls")))
    
    if not input_files:
        logger.error(f"No CSV or Excel files found in {input_dir}")
        return
        
    logger.info(f"Found {len(input_files)} files to process")
    
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