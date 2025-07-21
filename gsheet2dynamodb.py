import pandas as pd
import boto3
from botocore.exceptions import ClientError
import csv
import json # Used for handling potential JSON strings in data
from decimal import Decimal, InvalidOperation # Add Decimal import for DynamoDB
import decimal
import time # For delays between chunks
from datetime import datetime # For timestamped logs
import os # For error log file existence check

# --- Configuration ---
# IMPORTANT: Replace with your desired AWS region
AWS_REGION = 'us-east-2' 
# IMPORTANT: Replace with your desired DynamoDB table name
DYNAMODB_TABLE_NAME = '20240222transactionsall'
# IMPORTANT: Specify the CSV file you want to use from your uploads
# For example, '2024-02-22 Everything Latest export - everything.csv'
CSV_FILE_PATH = 'migratecsv/2024-02-22_everything-latest-export-2.csv'
# IMPORTANT: Define your DynamoDB table's primary key(s).
# This is crucial. Choose one or two columns from your CSV that uniquely identify each row.
# For a simple primary key (Partition Key only):
PARTITION_KEY_NAME = 'ID' # Example: Use 'Date' column as partition key
# For a composite primary key (Partition Key + Sort Key):
# PARTITION_KEY_NAME = 'User' # Example: Use 'User' as partition key
SORT_KEY_NAME = 'Date'     # Example: Use 'Date' as sort key

# Set the limit for the number of rows to process for testing
ROW_LIMIT = None  # Process all 21,982 rows
CHUNK_SIZE = 500  # Process 500 records at a time - good balance of speed and reliability  
START_FROM_ROW = 0  # Resume from specific row if needed (0-indexed)

# Initialize DynamoDB client
# boto3 will automatically pick up credentials from your AWS CLI configuration
# or environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

def create_dynamodb_table(table_name, partition_key, sort_key=None):
    """
    Creates a DynamoDB table if it does not already exist.
    """
    print(f"Attempting to create table: {table_name}...")
    try:
        attribute_definitions = [
            {'AttributeName': partition_key, 'AttributeType': 'S'} # Assuming string type for partition key
        ]
        key_schema = [
            {'AttributeName': partition_key, 'KeyType': 'HASH'} # HASH for Partition Key
        ]

        if sort_key:
            attribute_definitions.append({'AttributeName': sort_key, 'AttributeType': 'S'}) # Assuming string type for sort key
            key_schema.append({'AttributeName': sort_key, 'KeyType': 'RANGE'}) # RANGE for Sort Key

        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            # Provisioned Throughput is for On-Demand capacity mode,
            # which is generally recommended for variable workloads.
            # For provisioned mode, you'd specify ReadCapacityUnits and WriteCapacityUnits.
            BillingMode='PAY_PER_REQUEST'
        )
        print("Table creation initiated. Waiting for table to become active...")
        table.wait_until_exists()
        print(f"Table '{table_name}' created successfully and is active.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table '{table_name}' already exists.")
            table = dynamodb.Table(table_name)
        else:
            print(f"Error creating table: {e}")
            raise
    return table

def prepare_item_for_dynamodb(row_dict, partition_key_name, sort_key_name=None):
    """
    Prepares a dictionary (row) for DynamoDB insertion.
    DynamoDB does not allow empty strings or certain complex types directly.
    Converts all values to appropriate DynamoDB types.
    Ensures primary keys are always present, even if their values are initially NaN or empty.
    """
    item = {}
    
    # Explicitly handle primary keys first to ensure they are always present
    pk_value = row_dict.get(partition_key_name)
    cleaned_pk_name = partition_key_name.strip().replace(' ', '_').replace('.', '_').replace('#', 'Num').replace('/', '_')
    
    if pd.isna(pk_value) or (isinstance(pk_value, str) and pk_value.strip() == ''):
        # Primary key value is missing or empty. DynamoDB will reject this.
        # Set a placeholder to make the error explicit from DynamoDB.
        item[cleaned_pk_name] = "NULL_OR_EMPTY_PK_VALUE" 
        print(f"WARNING: Primary key '{partition_key_name}' (cleaned: '{cleaned_pk_name}') has NaN or empty value: '{pk_value}'. Setting to 'NULL_OR_EMPTY_PK_VALUE'. This item will likely fail to upload.")
    else:
        # For primary keys, ensure they are strings. DynamoDB will handle type for other attributes.
        item[cleaned_pk_name] = str(pk_value)

    if sort_key_name:
        sk_value = row_dict.get(sort_key_name)
        cleaned_sk_name = sort_key_name.strip().replace(' ', '_').replace('.', '_').replace('#', 'Num').replace('/', '_')
        if pd.isna(sk_value) or (isinstance(sk_value, str) and sk_value.strip() == ''):
            item[cleaned_sk_name] = "NULL_OR_EMPTY_SK_VALUE"
            print(f"WARNING: Sort key '{sort_key_name}' (cleaned: '{cleaned_sk_name}') has NaN or empty value: '{sk_value}'. Setting to 'NULL_OR_EMPTY_SK_VALUE'. This item will likely fail to upload.")
        else:
            item[cleaned_sk_name] = str(sk_value)

    # Process other attributes
    for key, value in row_dict.items():
        cleaned_key = key.strip().replace(' ', '_').replace('.', '_').replace('#', 'Num').replace('/', '_')
        
        # Skip if it's a primary key, as they are already handled
        if cleaned_key == cleaned_pk_name or (sort_key_name and cleaned_key == cleaned_sk_name):
            continue

        # Handle empty strings and NaN values for non-primary attributes
        if pd.isna(value) or (isinstance(value, str) and value.strip() == ''):
            continue # Skip empty/NaN non-primary attributes
        
        # Attempt to convert to appropriate types for non-primary attributes
        try:
            if isinstance(value, (int, float)):
                # Handle potential NaN or inf values
                if pd.isna(value) or not pd.isfinite(value):
                    continue  # Skip invalid numeric values
                item[cleaned_key] = Decimal(str(value)) # Convert float to Decimal
            elif isinstance(value, str):
                if value.startswith(('$', '-$')):
                    numeric_value = value.replace('$', '').replace(',', '').strip()
                    try:
                        item[cleaned_key] = Decimal(numeric_value)
                    except (ValueError, InvalidOperation):
                        item[cleaned_key] = str(value)
                elif value.lower() in ['true', 'false']:
                    item[cleaned_key] = value.lower() == 'true'
                elif value.strip().isdigit():
                    item[cleaned_key] = int(value.strip())
                elif value.replace('.', '', 1).replace('-', '', 1).isdigit():
                    try:
                        item[cleaned_key] = Decimal(value.strip()) # Convert float to Decimal
                    except (ValueError, InvalidOperation):
                        item[cleaned_key] = str(value)
                else:
                    item[cleaned_key] = str(value)
            else:
                item[cleaned_key] = str(value)
        except Exception as e:
            print(f"Warning: Could not convert value '{value}' for key '{key}'. Storing as string. Error: {e}")
            item[cleaned_key] = str(value)
            
    return item

def validate_table_count(table_name, expected_min_count):
    """Validate the number of items in the table"""
    table = dynamodb.Table(table_name)
    try:
        response = table.scan(Select='COUNT')
        actual_count = response['Count']
        print(f"✅ Table validation: {actual_count} items in table (expected >= {expected_min_count})")
        return actual_count >= expected_min_count
    except Exception as e:
        print(f"❌ Error validating table: {e}")
        return False

def create_error_log_file(csv_file):
    """Create a timestamped error log file for tracking failed records"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = csv_file.replace('.csv', '').replace('/', '_')
    error_log_file = f"failed_records_{base_name}_{timestamp}.csv"
    return error_log_file

def log_failed_record(error_log_file, row_number, error_message, row_data, error_type="processing"):
    """Log a failed record to the error log file"""
    try:
        # Create error log entry
        error_entry = {
            'row_number': row_number,
            'error_type': error_type,
            'error_message': str(error_message),
            'timestamp': datetime.now().isoformat(),
            'raw_data': json.dumps(row_data) if isinstance(row_data, dict) else str(row_data)
        }
        
        # Write to CSV (append mode)
        file_exists = os.path.exists(error_log_file)
        
        with open(error_log_file, 'a', newline='', encoding='utf-8') as f:
            fieldnames = ['row_number', 'error_type', 'error_message', 'timestamp', 'raw_data']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            writer.writerow(error_entry)
            
    except Exception as e:
        print(f"  ⚠️  Warning: Could not log error to file: {e}")

def create_summary_report(total_processed, total_failed, error_log_file, migration_duration):
    """Create a summary report of the migration"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_file = f"migration_summary_{timestamp}.txt"
        
        with open(summary_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("DYNAMODB MIGRATION SUMMARY REPORT\n")
            f.write("=" * 60 + "\n")
            f.write(f"Migration Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Duration: {migration_duration:.2f} seconds\n\n")
            
            f.write("RESULTS:\n")
            f.write(f"✅ Successfully processed: {total_processed:,} records\n")
            f.write(f"❌ Failed to process: {total_failed:,} records\n")
            
            if total_processed + total_failed > 0:
                success_rate = (total_processed / (total_processed + total_failed)) * 100
                f.write(f"📊 Success rate: {success_rate:.2f}%\n\n")
            
            if total_failed > 0:
                f.write("ERROR DETAILS:\n")
                f.write(f"📄 Detailed error log: {error_log_file}\n")
                f.write("🔄 To retry failed records, use the error log file\n\n")
            
            f.write("CONFIGURATION:\n")
            f.write(f"CSV File: {CSV_FILE_PATH}\n")
            f.write(f"DynamoDB Table: {DYNAMODB_TABLE_NAME}\n")
            f.write(f"AWS Region: {AWS_REGION}\n")
            f.write(f"Chunk Size: {CHUNK_SIZE}\n")
        
        print(f"📋 Summary report saved: {summary_file}")
        return summary_file
        
    except Exception as e:
        print(f"⚠️  Warning: Could not create summary report: {e}")
        return None

def upload_csv_to_dynamodb_chunked(csv_file, table_name, partition_key, sort_key=None, row_limit=None, chunk_size=500, start_from_row=0):
    """
    Reads data from a CSV file and uploads it to DynamoDB in chunks for better reliability.
    Creates detailed error logs for any failed records.
    """
    table = dynamodb.Table(table_name)
    start_time = time.time()
    
    # Create error log file
    error_log_file = create_error_log_file(csv_file)
    
    print(f"🚀 Starting CHUNKED data migration from '{csv_file}' to DynamoDB table '{table_name}'...")
    print(f"📊 Chunk size: {chunk_size} records | Starting from row: {start_from_row}")
    print(f"📝 Error log file: {error_log_file}")

    try:
        # First, get total row count without loading all data
        total_rows = sum(1 for line in open(csv_file)) - 1  # -1 for header
        if row_limit:
            total_rows = min(total_rows, row_limit)
        
        print(f"📈 Total rows to process: {total_rows:,}")
        
        # Process in chunks
        total_processed = 0
        total_failed = 0
        chunk_num = 0
        
        # Calculate starting chunk
        start_chunk = start_from_row // chunk_size
        skip_rows = start_from_row
        
        while skip_rows < total_rows:
            chunk_num += 1
            end_row = min(skip_rows + chunk_size, total_rows)
            actual_chunk_size = end_row - skip_rows
            
            print(f"\n🔄 Processing Chunk {chunk_num}: rows {skip_rows+1:,} to {end_row:,}")
            
            try:
                # Read only this chunk
                df_chunk = pd.read_csv(csv_file, 
                                     skiprows=range(1, skip_rows + 1) if skip_rows > 0 else None,
                                     nrows=actual_chunk_size)
                
                # Ensure column names are stripped of whitespace
                df_chunk.columns = df_chunk.columns.str.strip()
                records = df_chunk.to_dict(orient='records')
                
                chunk_processed = 0
                chunk_failed = 0
                
                # Process this chunk
                with table.batch_writer() as batch:
                    for i, row in enumerate(records):
                        current_row_number = skip_rows + i + 1
                        try:
                            # Construct the item for DynamoDB
                            item = prepare_item_for_dynamodb(row, partition_key, sort_key)
                            
                            batch.put_item(Item=item)
                            chunk_processed += 1
                            
                            if chunk_processed % 50 == 0:
                                print(f"  ⏳ Chunk progress: {chunk_processed}/{actual_chunk_size}")

                        except ClientError as e:
                            error_code = e.response['Error']['Code']
                            error_message = e.response['Error']['Message']
                            full_error = f"DynamoDB {error_code}: {error_message}"
                            
                            print(f"  ❌ DynamoDB error at row {current_row_number:,}: {error_code} - {error_message}")
                            log_failed_record(error_log_file, current_row_number, full_error, row, "dynamodb_error")
                            chunk_failed += 1
                            
                        except Exception as e:
                            print(f"  ❌ Processing error at row {current_row_number:,}: {e}")
                            log_failed_record(error_log_file, current_row_number, str(e), row, "processing_error")
                            chunk_failed += 1
                
                # Update totals
                total_processed += chunk_processed
                total_failed += chunk_failed
                
                # Report chunk results
                success_rate = (chunk_processed / actual_chunk_size) * 100 if actual_chunk_size > 0 else 0
                print(f"  ✅ Chunk {chunk_num} complete: {chunk_processed:,}/{actual_chunk_size:,} successful ({success_rate:.1f}%)")
                
                if chunk_failed > 0:
                    print(f"  ❌ Failed in this chunk: {chunk_failed:,} (logged to {error_log_file})")
                
                # Validate after each chunk
                if chunk_processed > 0:
                    validate_table_count(table_name, total_processed)
                
                # Brief pause between chunks to avoid throttling
                if skip_rows + chunk_size < total_rows:  # Don't sleep after last chunk
                    time.sleep(1)  # 1 second pause
                
            except Exception as e:
                print(f"  ❌ Error processing chunk {chunk_num}: {e}")
                print(f"  📍 You can resume from row {skip_rows:,} by setting START_FROM_ROW = {skip_rows}")
                
                # Log chunk-level error
                log_failed_record(error_log_file, f"chunk_{chunk_num}_rows_{skip_rows+1}_to_{end_row}", 
                                str(e), f"Chunk processing failed", "chunk_error")
                break
            
            skip_rows += chunk_size
        
        # Calculate migration duration
        migration_duration = time.time() - start_time
        
        # Final summary
        print(f"\n🎯 MIGRATION SUMMARY:")
        print(f"   📊 Total rows processed: {total_processed:,}")
        print(f"   ❌ Total failures: {total_failed:,}")
        print(f"   ✅ Success rate: {(total_processed/(total_processed+total_failed)*100) if (total_processed+total_failed) > 0 else 0:.1f}%")
        print(f"   ⏱️  Duration: {migration_duration:.2f} seconds")
        print(f"   🗃️  Final table validation...")
        
        # Final validation
        validate_table_count(table_name, total_processed)
        
        # Create summary report
        if total_failed > 0:
            print(f"\n📝 ERROR TRACKING:")
            print(f"   📄 Detailed error log: {error_log_file}")
            print(f"   🔍 Review failed records for patterns")
            print(f"   🔄 Use error log to retry specific failed records")
        
        create_summary_report(total_processed, total_failed, error_log_file, migration_duration)

    except FileNotFoundError:
        print(f"❌ Error: CSV file not found at '{csv_file}'. Please check the path.")
    except Exception as e:
        print(f"❌ An unexpected error occurred during migration: {e}")
        # Log the major error
        log_failed_record(error_log_file, "migration_level", str(e), "Full migration failed", "migration_error")

# Keep the original function for small datasets
def upload_csv_to_dynamodb(csv_file, table_name, partition_key, sort_key=None, row_limit=None):
    """
    Original function - use for small datasets only.
    For large datasets, use upload_csv_to_dynamodb_chunked instead.
    """
    if row_limit and row_limit > 1000:
        print("⚠️  Large dataset detected. Consider using chunked processing for better reliability.")
    
    upload_csv_to_dynamodb_chunked(csv_file, table_name, partition_key, sort_key, row_limit, 
                                 chunk_size=min(500, row_limit) if row_limit else 500, start_from_row=0)

def retry_failed_records(error_log_file, table_name, partition_key, sort_key=None):
    """
    Retry processing records that previously failed, based on the error log file.
    """
    table = dynamodb.Table(table_name)
    
    if not os.path.exists(error_log_file):
        print(f"❌ Error log file not found: {error_log_file}")
        return False
    
    print(f"🔄 Starting retry of failed records from: {error_log_file}")
    
    try:
        # Read the error log
        failed_records = []
        with open(error_log_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['error_type'] in ['processing_error', 'dynamodb_error']:
                    try:
                        # Parse the raw data back to dict
                        raw_data = json.loads(row['raw_data'])
                        failed_records.append({
                            'row_number': row['row_number'],
                            'original_error': row['error_message'],
                            'data': raw_data
                        })
                    except Exception as e:
                        print(f"  ⚠️  Could not parse failed record from row {row['row_number']}: {e}")
        
        if not failed_records:
            print("✅ No failed records found to retry!")
            return True
        
        print(f"📊 Found {len(failed_records)} failed records to retry")
        
        # Create new error log for retry attempt
        retry_error_log = error_log_file.replace('.csv', '_retry.csv')
        
        retry_processed = 0
        retry_failed = 0
        
        with table.batch_writer() as batch:
            for record in failed_records:
                try:
                    # Retry processing this record
                    item = prepare_item_for_dynamodb(record['data'], partition_key, sort_key)
                    batch.put_item(Item=item)
                    retry_processed += 1
                    
                    if retry_processed % 25 == 0:
                        print(f"  ⏳ Retry progress: {retry_processed}/{len(failed_records)}")
                
                except Exception as e:
                    print(f"  ❌ Retry failed for row {record['row_number']}: {e}")
                    log_failed_record(retry_error_log, record['row_number'], str(e), record['data'], "retry_failed")
                    retry_failed += 1
        
        print(f"\n🎯 RETRY SUMMARY:")
        print(f"   ✅ Successfully retried: {retry_processed}/{len(failed_records)}")
        print(f"   ❌ Still failing: {retry_failed}/{len(failed_records)}")
        
        if retry_failed > 0:
            print(f"   📄 Persistent failures logged to: {retry_error_log}")
        
        return retry_failed == 0
        
    except Exception as e:
        print(f"❌ Error during retry operation: {e}")
        return False

if __name__ == "__main__":
    # 1. Create the DynamoDB table (or ensure it exists)
    # Note: DynamoDB requires primary keys to be of type String, Number, or Binary.
    # For CSV data, String is often the most flexible choice for keys.
    # If your primary key column contains numbers, DynamoDB will store them as Numbers.
    # If it contains dates, they will be stored as Strings.
    
    # You MUST define your primary key(s) above (PARTITION_KEY_NAME and optionally SORT_KEY_NAME)
    # based on your CSV file's columns.
    
    # Example: If 'Date' is your partition key and it's a string, use:
    # PARTITION_KEY_NAME = 'Date'
    # SORT_KEY_NAME = None # If no sort key
    
    # If 'Year' is partition key (number) and 'Month' is sort key (string):
    # PARTITION_KEY_NAME = 'Year' (ensure it's treated as a number in prepare_item_for_dynamodb)
    # SORT_KEY_NAME = 'Month'

    # Ensure your chosen primary key column exists in your CSV and has unique values
    # (or unique combinations for composite keys)
    
    # Check if a sort key is defined
    if 'SORT_KEY_NAME' in locals() and SORT_KEY_NAME:
        dynamodb_table = create_dynamodb_table(DYNAMODB_TABLE_NAME, PARTITION_KEY_NAME, SORT_KEY_NAME)
    else:
        dynamodb_table = create_dynamodb_table(DYNAMODB_TABLE_NAME, PARTITION_KEY_NAME)

    # 2. Upload data from CSV to DynamoDB using chunked processing
    if dynamodb_table:
        upload_csv_to_dynamodb_chunked(
            csv_file=CSV_FILE_PATH, 
            table_name=DYNAMODB_TABLE_NAME, 
            partition_key=PARTITION_KEY_NAME, 
            sort_key=locals().get('SORT_KEY_NAME'), 
            row_limit=ROW_LIMIT,
            chunk_size=CHUNK_SIZE,
            start_from_row=START_FROM_ROW
        )
