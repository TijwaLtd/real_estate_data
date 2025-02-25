# Real Estate Data Processor

A Python utility for processing, cleaning, and standardizing real estate property data from multiple CSV files.

## Overview

This tool allows you to process multiple CSV files containing real estate property data and combine them into a single standardized output file. It handles data mapping, deduplication, and ensures that only records with useful contact information are included in the final output.

## Features

- **Unified Data Format**: Transforms disparate input data into a standardized output format
- **Deduplication**: Removes duplicate property records based on address and owner information
- **Contact Information Filtering**: Only keeps records with useful contact details (phone, email, or mailing address)
- **Parallel Processing**: Efficiently processes multiple files simultaneously using multiprocessing
- **Comprehensive Logging**: Tracks processing progress and provides statistics on results

## Requirements

- Python 3.6+
- pandas
- concurrent.futures (standard library)
- hashlib (standard library)
- logging (standard library)

## Installation

Clone this repository or download the script files to your local machine.

```bash
# Install required dependencies
pip install pandas
```

## Usage

Run the script from the command line, providing the input directory containing CSV files and the desired output path:

```bash
python real_estate_processor.py --input-dir /path/to/csv/files --output-path /path/to/output.csv
```

### Command Line Arguments

- `--input-dir`: Directory containing input CSV files (required)
- `--output-path`: Path to save the output CSV file (required)
- `--max-workers`: Maximum number of parallel workers (optional, defaults to the number of CPU cores)

## Code Structure and Functionality

The code is organized into a class-based structure with the following components:

### RealEstateDataProcessor Class

This is the main class responsible for processing the real estate data:

- ****init****: Initializes the processor with the default output columns and data storage structures.

- **\_generate_unique_key**: Creates a unique hash for each property record based on the address and owner information to identify duplicates.

- **\_map_input_to_output**: Maps fields from various input CSV formats to a standardized output format, handling differences in field names and data organization.

- **\_has_contact_info**: Checks if a record has enough contact information (phone, email, or mailing address) to be useful in the output.

- **process_file**: Processes a single CSV file, transforming each row and filtering out records without contact information.

- **process_files**: Handles multiple CSV files in parallel using Python's ProcessPoolExecutor, combines the results, and performs deduplication.

### Main Function

The `main()` function serves as the entry point for the script:

1. Parses command-line arguments using argparse
2. Locates all CSV files in the specified input directory
3. Initializes the RealEstateDataProcessor
4. Processes all files using the processor
5. Saves the combined and deduplicated results to the specified output file
6. Provides detailed logging of the processing results

### Data Processing Flow

1. **Input Reading**: Each CSV file is read using pandas
2. **Field Mapping**: Input fields are mapped to standardized output fields
3. **Contact Filtering**: Records without sufficient contact info are filtered out
4. **Parallel Processing**: Multiple files are processed simultaneously
5. **Deduplication**: Duplicate records are identified and removed
6. **Output Organization**: Columns are reordered to match the desired output format
7. **Result Writing**: Final data is written to a CSV file

## Input Data Format

The script expects CSV files with real estate data. The following fields are mapped from the input to the standardized output:

| Input Field           | Output Field    |
| --------------------- | --------------- |
| Address               | Street Address  |
| City                  | City            |
| State                 | State           |
| Zip                   | Postal Code     |
| Owner 1 First Name    | First Name      |
| Owner 1 Last Name     | Last Name       |
| Owner Mailing Address | Mailing Address |
| Owner Mailing City    | Mailing City    |
| Owner Mailing State   | Mailing State   |
| Owner Mailing Zip     | Mailing Zip     |
| Property Type         | Property Type   |
| Bedrooms              | Bedrooms        |
| Bathrooms             | Total Bathrooms |
| Living Square Feet    | Building Sqft   |
| Lot (Square Feet)     | Lot Size Sqft   |
| Phone 1, 2, 3         | Phone 1-5       |
| Email                 | Email           |

## Output Format

The script produces a CSV file with the following columns:

```
Street Address, Unit #, City, State, Postal Code, First Name, Last Name,
Mailing Address, Mailing Unit #, Mailing City, Mailing State, Mailing Zip,
Property Type, Bedrooms, Total Bathrooms, Building Sqft, Lot Size Sqft,
Est. Value, Phone 1, Phone 2, Phone 3, Phone 4, Phone 5,
Email, Email 2, Email 3, Email 4, Email 5
```

## How It Works

1. The script scans the input directory for CSV files
2. Each file is processed in parallel, mapping input fields to the standardized output format
3. Records without contact information (phone, email, or mailing address) are filtered out
4. Duplicate records are removed based on a unique hash of address and owner information
5. The combined, deduplicated data is saved to the specified output file

## Example

```bash
python real_estate_processor.py --input-dir ./raw_data --output-path ./processed_data/combined_properties.csv
```

## Logging

The script provides detailed logging information during execution, including:

- Number of files found and processed
- Records processed from each file
- Before and after deduplication counts
- Total execution time
- Final record count

## License

[Specify your license here]

## Contributing

[Include contribution guidelines if applicable]
