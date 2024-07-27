import os
import csv
import pandas as pd
from pathlib import Path
import sys
import logging

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("file_processor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


class FileHandler:
    def __init__(self, file, source_file_location, schema, config, output_file_location, scanned_files):
        self.file = file
        self.source_file_location = source_file_location
        self.schema = schema
        self.config = config
        self.output_file_location = output_file_location
        self.scanned_files = scanned_files

    def is_csv_file(self):
        return Path(self.file).suffix.lower() == ".csv"

    def has_records(self):
        file_location = Path(self.source_file_location) / self.file
        try:
            with open(file_location, mode='r', encoding='utf-8') as file:
                reader = csv.reader(file)
                for _ in reader:
                    return True
        except Exception as e:
            logging.error(f"Error checking records in file {file_location}: {e}")
        return False

    def check_if_file_already_scanned(self):
        logging.info(f"Checking if file has been scanned: {self.file}")
        try:
            df = pd.read_csv(self.scanned_files, encoding='utf-8')
            return self.file in df['files_scanned'].values
        except Exception as e:
            logging.error(f"Error reading scanned files: {e}")
        return False

    def null_check(self):
        """Check for null values in specified attributes and return clean records."""
        file_location = Path(self.source_file_location) / self.file
        bad_records = pd.DataFrame()
        try:
            df = pd.read_csv(file_location, encoding='utf-8')
            attributes = self.config.get(Path(self.file).stem, {}).get('null_check', [])
            for attribute in attributes:
                if attribute in df.columns:
                    null_indices = df[df[attribute].isnull()].index
                    if not null_indices.empty:
                        logging.warning(f"Null values found in {attribute} of file {self.file}")
                        bad_records = pd.concat([bad_records, df.loc[null_indices]])

            # Define output file paths
            bad_file = file_location.with_suffix('.bad')
            clean_file = file_location.with_suffix('.out')

            # Save bad records to .bad file
            if not bad_records.empty:
                bad_records.to_csv(bad_file, index=False, encoding='utf-8')

            # Save clean records to .out file
            clean_records = df.drop(bad_records.index)
            clean_records.to_csv(clean_file, index=False, encoding='utf-8')

            logging.info(f"Files created: {clean_file}, {bad_file}")

            # Call handle_failed_results to handle the bad records
            self.handle_failed_results(bad_records)

            return clean_records

        except Exception as e:
            logging.error(f"Error during null check on file {self.file}: {e}")
        return pd.DataFrame()

    def handle_failed_results(self, bad_records):
        """Handle failed results by saving them to a file."""
        if not bad_records.empty:
            file_location = Path(self.output_file_location) / 'failed_records.csv'
            bad_records.to_csv(file_location, index=False, encoding='utf-8')
            logging.info(f"Failed records saved to {file_location}")

    def test_schema(self):
        """Validate if the CSV file's columns match the schema attributes and remove extra columns."""
        file_location = Path(self.source_file_location) / self.file
        try:
            # Load schema columns from schema file
            schema_columns = set(self.schema.keys())

            # Read the CSV file to get its columns
            df = pd.read_csv(file_location, encoding='utf-8')
            file_columns = set(df.columns)

            # Identify extra columns not present in schema
            extra_columns = file_columns - schema_columns

            if extra_columns:
                logging.warning(f"Extra columns in the file '{self.file}': {extra_columns}")
                # Remove extra columns from DataFrame
                df = df[sorted(schema_columns)]  # Sort to maintain the order from schema

            # Define the path for the cleaned file
            cleaned_file_location = file_location.with_suffix('.cleaned.csv')

            # Save the cleaned DataFrame to a new file
            df.to_csv(cleaned_file_location, index=False, encoding='utf-8')

            logging.info(f"Cleaned file saved to {cleaned_file_location}")
            return df  # Return the cleaned DataFrame

        except Exception as e:
            logging.error(f"Error validating schema for file {file_location}: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of error


class FileProcessor:
    def __init__(self, source_file_location, scanned_files, config_file, schema_file, output_file_location):
        self.source_file_location = source_file_location
        self.scanned_files = scanned_files
        self.config_file = config_file
        self.schema_file = schema_file
        self.output_file_location = output_file_location
        self.schema = self.load_schema()
        self.config = self.load_config()

    def load_schema(self):
        """Load and parse the schema file."""
        schema = {}
        try:
            with open(self.schema_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    field_name = row['Field Name'].strip()  # Corrected field name
                    data_type = row['DataType'].strip()  # Corrected field name
                    schema[field_name] = data_type
        except Exception as e:
            logging.error(f"Error loading schema file {self.schema_file}: {e}")
        return schema

    def load_config(self):
        """Load and parse the config file."""
        config = {}
        try:
            with open(self.config_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    file_prefix = row['file_prefix'].strip()
                    test_type = row['test'].strip()
                    attribute = row['attribute'].strip()
                    if file_prefix not in config:
                        config[file_prefix] = {}
                    if test_type not in config[file_prefix]:
                        config[file_prefix][test_type] = []
                    config[file_prefix][test_type].append(attribute)
        except Exception as e:
            logging.error(f"Error loading config file {self.config_file}: {e}")
        return config

    def check_file(self):
        logging.info(f"Source file location: {self.source_file_location}")
        try:
            files = os.listdir(self.source_file_location)
        except Exception as e:
            logging.error(f"Error listing files in {self.source_file_location}: {e}")
            return []

        list_of_files_to_be_tested = []
        for file in files:
            logging.info(f"Processing file: {file}")
            handler = FileHandler(
                file,
                self.source_file_location,
                self.schema,
                self.config,
                self.output_file_location,
                self.scanned_files
            )
            if handler.check_if_file_already_scanned():
                logging.info(f"File already scanned: {file}")
            else:
                logging.info(f"File is new: {file}. Proceeding with further processing.")
                if handler.is_csv_file():
                    logging.info(f"{file} is a CSV file.")
                    if handler.has_records():
                        logging.info(f"File {file} has non-zero records")
                        list_of_files_to_be_tested.append(file)

        return list_of_files_to_be_tested

    def process_files(self):
        file_check_module_passed_files = self.check_file()
        logging.info("Processing files")
        for present_file in file_check_module_passed_files:
            logging.info(f"New test on file: {present_file}")
            handler = FileHandler(
                present_file,
                self.source_file_location,
                self.schema,
                self.config,
                self.output_file_location,
                self.scanned_files
            )
            cleaned_df = handler.test_schema()
            if not cleaned_df.empty:
                handler.null_check()  # Perform null checks after schema validation


if __name__ == '__main__':
    source_file_location = sys.argv[1]
    scanned_files = sys.argv[2]
    config_file = sys.argv[3]
    schema_file = sys.argv[4]
    output_file_location = sys.argv[5]

    processor = FileProcessor(source_file_location, scanned_files, config_file, schema_file, output_file_location)
    processor.process_files()
