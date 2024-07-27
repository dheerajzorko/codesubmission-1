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


class FileProcessor:
    def __init__(self, source_file_location, scanned_files, config_file, schema_file, output_file_location):
        self.source_file_location = source_file_location
        self.scanned_files = scanned_files
        self.config_file = config_file
        self.schema_file = schema_file
        self.output_file_location = output_file_location
        self.schema = self.load_schema()
        self.config = self.load_config()
        self.clean_records = None
        self.bad_records = None

    def load_schema(self):
        """Load and parse the schema file."""
        schema = {}
        try:
            with open(self.schema_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    field_name = row['Field Name'].strip()
                    data_type = row['DataType'].strip()
                    schema[field_name] = data_type
        except Exception as e:
            logging.error(f"Error loading schema file {self.schema_file}: {e}")
        return schema

    def load_config(self):
        """Load and parse the config file."""
        config = {}
        try:
            logging.info("Loading config file")
            with open(self.config_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    file_prefix = row['file_prefix'].strip()[0:-18]
                    logging.info(f"File prefix: {file_prefix}")

                    test_type = row['test'].strip()
                    logging.info(f"Test type: {test_type}")

                    attribute = row['attribute'].strip()
                    logging.info(f"Attribute: {attribute}")

                    if file_prefix not in config:
                        config[file_prefix] = {}
                    if test_type not in config[file_prefix]:
                        config[file_prefix][test_type] = []
                    config[file_prefix][test_type].append(attribute)
        except Exception as e:
            logging.error(f"Error loading config file {self.config_file}: {e}")
        return config

    def check_if_file_already_scanned(self, file):
        """Check if the file has already been scanned."""
        logging.info(f"Checking if file has been scanned: {file}")
        try:
            df = pd.read_csv(self.scanned_files, encoding='utf-8')
            return file in df['files_scanned'].values
        except Exception as e:
            logging.error(f"Error reading scanned files: {e}")
        return False

    def is_csv_file(self, file):
        """Check if the file is a CSV file."""
        return Path(file).suffix.lower() == ".csv"

    def has_records(self, file):
        """Check if the file has any records."""
        file_location = Path(self.source_file_location) / file
        try:
            with open(file_location, mode='r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for _ in reader:
                    return True
        except Exception as e:
            logging.error(f"Error checking records in file {file_location}: {e}")
        return False

    def check_file(self):
        """Check for new files to process."""
        logging.info(f"Source file location: {self.source_file_location}")
        try:
            files = os.listdir(self.source_file_location)
        except Exception as e:
            logging.error(f"Error listing files in {self.source_file_location}: {e}")
            return []

        list_of_files_to_be_tested = []
        for file in files:
            logging.info(f"Processing file: {file}")
            if self.check_if_file_already_scanned(file):
                logging.info(f"File already scanned: {file}")
            else:
                logging.info(f"File is new: {file}. Proceeding with further processing.")
                if self.is_csv_file(file):
                    logging.info(f"{file} is a CSV file.")
                    if self.has_records(file):
                        logging.info(f"File {file} has non-zero records")
                        list_of_files_to_be_tested.append(file)

        return list_of_files_to_be_tested

    def process_files(self):
        """Process the list of files that passed the initial checks."""
        file_check_module_passed_files = self.check_file()
        logging.info("Processing files")

        for present_file in file_check_module_passed_files:
            logging.info(f"New test on file: {present_file}")
            file_location = Path(self.source_file_location) / present_file
            self.clean_records = pd.read_csv(file_location, encoding='utf-8')

            duplicate_check_attributes = self.load_config().get(present_file[0:-18], {}).get('duplicate_check', [])
            print(duplicate_check_attributes)
            self.duplicate_check(present_file, duplicate_check_attributes)

            logging.info(f"bad records in main after DUP test {self.bad_records}")

            null_check_attributes = self.load_config().get(present_file[0:-18], {}).get('null_check', [])
            print(null_check_attributes)
            self.null_check(present_file, null_check_attributes)

            logging.info(f"bad records in main after NULL test {self.bad_records}")

            self.save_good_records(present_file,self.clean_records)

    def null_check(self, file, null_check_attributes):
        """Perform a null check on the specified attributes in the file."""
        file_location = Path(self.source_file_location) / file
        try:
            # Load the CSV file into a DataFrame
            df = pd.read_csv(file_location, encoding='utf-8')

            if df.empty:
                logging.warning(f"The file {file} is empty or improperly formatted. Skipping.")
                return

            # Initialize or clear the bad_records and clean_records DataFrames
            # self.bad_records = pd.DataFrame()
            # self.clean_records = pd.DataFrame()

            # Identify null records and clean records
            bad_records = pd.DataFrame()
            for attribute in null_check_attributes:
                if attribute in df.columns:
                    null_indices = df[df[attribute].isnull()].index
                    if not null_indices.empty:
                        logging.warning(f"Null values found in {attribute} of file {file}")
                        bad_records = pd.concat([bad_records, df.loc[null_indices]])

            self.bad_records = bad_records
            self.clean_records = df.drop(bad_records.index)


        except pd.errors.EmptyDataError:
            logging.error(f"No columns to parse from file {file}. It might be empty or incorrectly formatted.")
        except Exception as e:
            logging.error(f"Error during null check on file {file}: {e}")

    def duplicate_check(self, file, duplicate_check_attributes):
        """Perform a duplicate check on the specified attributes in the file."""
        file_location = Path(self.source_file_location) / file
        try:
            # Load the CSV file into a DataFrame
            df = pd.read_csv(file_location, encoding='utf-8')

            logging.info(f"bad records before dup test{self.bad_records}")

            if df.empty:
                logging.warning(f"The file {file} is empty or improperly formatted. Skipping.")
                return

            # Identify duplicate records based on specified attributes
            if duplicate_check_attributes:
                duplicates = df[df.duplicated(subset=duplicate_check_attributes, keep=False)]
                if not duplicates.empty:
                    logging.warning(f"Duplicate records found based on attributes {duplicate_check_attributes} in file {file}")
                    self.bad_records = pd.concat([self.bad_records, duplicates])
                    logging.info(f"bad records after dup test{self.bad_records}")

            # Update clean records by removing duplicates
            logging.info(f"bad records outside if condition {self.bad_records}")
            logging.info(f"clean records before dup test{self.clean_records}")
            self.clean_records = df.drop_duplicates(subset=duplicate_check_attributes, keep='first')
            logging.info(f"clean records after dup test{self.clean_records}")


            self.save_bad_records(file,self.bad_records)
            logging.info(f"bad records after dup test{self.bad_records}")


        except pd.errors.EmptyDataError:
            logging.error(f"No columns to parse from file {file}. It might be empty or incorrectly formatted.")
        except Exception as e:
            logging.error(f"Error during duplicate check on file {file}: {e}")

    def save_good_records(self, file, df):
        """Save clean records to a file."""
        try:
            if df is not None and not df.empty:
                clean_file_location = Path(self.output_file_location) / file.replace('.csv', '.out.csv')
                df.to_csv(clean_file_location, index=False, encoding='utf-8')
                logging.info(f"Clean records saved to {clean_file_location}")
        except Exception as e:
            logging.error(f"Error saving good records: {e}")

    def save_bad_records(self, file, df):
        """Save bad records to a file."""
        try:
            if df is not None and not df.empty:
                bad_file_location = Path(self.output_file_location) / file.replace('.csv', '.bad.csv')
                if bad_file_location.exists():
                    df.to_csv(bad_file_location, mode='a', index=False, encoding='utf-8')
                else:
                    df.to_csv(bad_file_location, index=False, encoding='utf-8')
                logging.info(f"Bad records saved to {bad_file_location}")
        except Exception as e:
            logging.error(f"Error saving bad records: {e}")


if __name__ == '__main__':
    source_file_location = sys.argv[1]
    scanned_files = sys.argv[2]
    config_file = sys.argv[3]
    schema_file = sys.argv[4]
    output_file_location = sys.argv[5]

    processor = FileProcessor(source_file_location, scanned_files, config_file, schema_file, output_file_location)
    processor.process_files()
