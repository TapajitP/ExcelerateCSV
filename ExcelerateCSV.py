#ExcelerateCSV
# Author: Paul, Tapajit
# Date: 24-11-2024

import os
import pandas as pd
from datetime import datetime
import traceback
from openpyxl import Workbook
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil  # For system memory stats
from tqdm import tqdm  # For progress bars
import logging
from logging.handlers import RotatingFileHandler

# Global stats dictionary to track processing outcomes
processing_stats = {
    "total_files": 0,  # Total number of files to process
    "success_count": 0,  # Count of successfully processed files
    "failure_count": 0,  # Count of failed files
    "time_per_file": {},  # Time taken to process each file
    "errors": {}  # Specific errors encountered for each file
}

# Configurable constants
# NOTE: Replace "MASKED_BASE_DIRECTORY" with the full path to the directory containing your CSV files.
# Example: r"C:\Users\YourUsername\Documents\YourFolder"
BASE_DIRECTORY = r"MASKED_BASE_DIRECTORY"  # Base directory where CSV files are located

# NOTE: Replace "MASKED_PREFIX" with the prefix of the files you want to process.
# Example: "XYZ_" will process files like "XYZ_Invoice1.csv", "XYZ_2024.csv", etc.
FILE_PREFIX = "MASKED_PREFIX"  # Prefix to filter files

LOG_FILE_NAME = "script_log.txt"  # Log file name

def get_dynamic_chunk_size():
    """
    Dynamically calculates an optimal chunk size based on available system memory.
    Ensures the program can handle large files efficiently without consuming too much memory.

    Returns:
        int: The calculated chunk size.
    """
    available_memory = psutil.virtual_memory().available / (1024 ** 2)  # MB
    return max(1000, int(available_memory // 15))  # Use ~7% of available memory

def setup_logging(output_dir):
    """
    Sets up logging with rotating file handlers to ensure logs are maintained efficiently.

    Args:
        output_dir (str): The directory where the log file will be saved.
    """
    log_file = os.path.join(output_dir, LOG_FILE_NAME)
    handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)  # 10 MB max, 5 backups
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler, logging.StreamHandler()]  # Log to both file and console
    )
    logging.info("Logging initialized.")

def log_message(message):
    """
    Logs a message to both the console and the log file.

    Args:
        message (str): The message to log.
    """
    logging.info(message)

def create_output_directory(base_dir):
    """
    Creates the 'ExcelerateCSV' directory within the specified base directory.

    Args:
        base_dir (str): The base directory where the output folder will be created.

    Returns:
        str: The path to the created output directory.
    """
    output_dir = os.path.join(base_dir, "ExcelerateCSV")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

def validate_csv_file(file_path, delimiter):
    """
    Validates the CSV file to ensure it is non-empty and readable.

    Args:
        file_path (str): The path to the CSV file to validate.
        delimiter (str): The delimiter used in the file.

    Returns:
        bool: True if the file is valid, False otherwise.
    """
    try:
        df = pd.read_csv(file_path, delimiter=delimiter, nrows=10, dtype=str)
        if df.empty or df.shape[1] == 0:
            log_message(f"Validation failed: File {file_path} is empty or has no columns.")
            return False
        return True
    except Exception as e:
        log_message(f"Validation failed for file {file_path}: {e}")
        return False

def determine_delimiter(file_path):
    """
    Determines the delimiter used in the CSV file.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        str: The detected delimiter (either ',' or ';').
    """
    try:
        with open(file_path, 'r') as file:
            first_line = file.readline()
            if ',' in first_line and ';' not in first_line:
                return ','
            elif ';' in first_line and ',' not in first_line:
                return ';'
            return ','  # Default to comma
    except Exception as e:
        log_message(f"Error determining delimiter for file {file_path}: {e}")
        raise

def retry_on_memory_error(func, retries, *args, **kwargs):
    """
    Retries the specified function in case of a MemoryError, reducing the chunk size dynamically.

    Args:
        func (callable): The function to retry.
        retries (int): The maximum number of retry attempts.

    Raises:
        MemoryError: If the retry limit is exceeded.
    """
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except MemoryError:
            log_message(f"MemoryError encountered. Reducing chunk size for retry ({kwargs.get('chunk_size') // 2}).")
            kwargs['chunk_size'] = max(kwargs.get('chunk_size') // 2, 500)
    raise MemoryError("Exceeded retry limit for memory errors.")

def convert_csv_to_excel(file_path, output_dir, initial_chunk_size, retry_attempts=3):
    """
    Converts a CSV file to an Excel file with retries and dynamic chunk size.

    Args:
        file_path (str): The path to the CSV file.
        output_dir (str): The directory to save the Excel file.
        initial_chunk_size (int): Initial chunk size for processing.
        retry_attempts (int): Number of retry attempts allowed for MemoryError.
    """
    start_time = datetime.now()
    base_name = os.path.basename(file_path)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_file = os.path.join(output_dir, f"{os.path.splitext(base_name)[0]}_{timestamp}.xlsx")

    def process_file(chunk_size):
        """
        Inner function to process the CSV file with the given chunk size.

        Args:
            chunk_size (int): Number of rows to process at a time.

        Returns:
            bool: True if the file was successfully processed, False otherwise.
        """
        try:
            delimiter = determine_delimiter(file_path)
            if not validate_csv_file(file_path, delimiter):
                processing_stats["failure_count"] += 1
                processing_stats["errors"][file_path] = "Validation failed"
                return False

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                for i, chunk in enumerate(pd.read_csv(file_path, delimiter=delimiter, chunksize=chunk_size, dtype=str)):
                    # Replace 'nan' and 'NAN' values with an empty string
                    chunk.replace(['nan', 'NAN'], '', inplace=True, regex=False)

                    # Fix the downcasting warning explicitly
                    chunk = chunk.infer_objects()

                    # Convert all values to strings using apply
                    chunk = chunk.applymap(str)

                    # Write to Excel
                    chunk.to_excel(writer, index=False, header=(i == 0))
                    
                    # Cleanup memory
                    del chunk
                    gc.collect()
            return True
        except Exception as e:
            raise e  # Let the retry logic handle MemoryError or other exceptions.

    try:
        log_message(f"Processing file: {file_path}")
        current_chunk_size = initial_chunk_size
        for attempt in range(retry_attempts):
            try:
                if process_file(current_chunk_size):
                    log_message(f"Successfully converted and saved: {output_file}")
                    processing_stats["success_count"] += 1
                    break
            except MemoryError:
                log_message(f"MemoryError: Reducing chunk size for retry (Attempt {attempt + 1}/{retry_attempts}).")
                current_chunk_size = max(current_chunk_size // 2, 500)
            except Exception as e:
                log_message(f"Error processing file {file_path}: {e}")
                processing_stats["failure_count"] += 1
                processing_stats["errors"][file_path] = str(e)
                break
        else:
            log_message(f"Failed to process {file_path} after {retry_attempts} attempts.")
            processing_stats["failure_count"] += 1
            processing_stats["errors"][file_path] = "Exceeded retry attempts"
    finally:
        processing_stats["time_per_file"][file_path] = (datetime.now() - start_time).total_seconds()

def process_all_csv_files(base_dir, chunk_size, max_workers):
    """
    Processes all CSV files in the directory and subdirectories.

    Args:
        base_dir (str): The base directory containing CSV files.
        chunk_size (int): Initial chunk size for processing.
        max_workers (int): Number of parallel threads to use.
    """
    output_dir = create_output_directory(base_dir)
    setup_logging(output_dir)

    csv_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(base_dir)
        for file in files
        if file.startswith(FILE_PREFIX) and file.lower().endswith('.csv')
    ]

    processing_stats["total_files"] = len(csv_files)
    log_message(f"Found {len(csv_files)} files to process.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(convert_csv_to_excel, file_path, output_dir, chunk_size): file_path
            for file_path in tqdm(csv_files, desc="Submitting files", unit="file")
        }
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing files", unit="file"):
            try:
                future.result()
            except Exception as e:
                file_path = future_to_file[future]
                log_message(f"Error processing file {file_path}: {e}")
                processing_stats["failure_count"] += 1
                processing_stats["errors"][file_path] = str(e)

def display_summary():
    """
    Displays a summary of the processing results.
    """
    log_message("\nProcessing Summary:")
    log_message(f"Total files processed: {processing_stats['total_files']}")
    log_message(f"Successful conversions: {processing_stats['success_count']}")
    log_message(f"Failed conversions: {processing_stats['failure_count']}")
    for file, error in processing_stats["errors"].items():
        log_message(f"  - Failed file: {file}, Error: {error}")
    for file, time_taken in processing_stats["time_per_file"].items():
        log_message(f"  - File: {file}, Time taken: {time_taken:.2f} seconds")
    memory_info = psutil.virtual_memory()
    log_message(f"Memory Usage: {memory_info.percent}% of {memory_info.total / (1024**3):.2f} GB")

if __name__ == "__main__":
    try:
        output_dir = create_output_directory(BASE_DIRECTORY)
        setup_logging(output_dir)
        log_message("Script execution started.")

        # Proceed with processing
        process_all_csv_files(BASE_DIRECTORY, get_dynamic_chunk_size(), os.cpu_count())
    except Exception as e:
        log_message(f"Critical error during execution: {e}")
        traceback.print_exc()
    finally:
        display_summary()
        log_message("Script execution completed.")