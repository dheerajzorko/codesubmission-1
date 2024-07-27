import re
import os
import csv
import pandas as pd
from pathlib import Path
import sys
import logging
import time


# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(f"file_processor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

class FileManager:
    def __init__(self, source_file_location, scanned_files, output_file_location):
        self.source_file_location = source_file_location
        self.scanned_files = scanned_files
        self.output_file_location = output_file_location

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

    def get_files_to_process(self):
        """Get the list of files that need to be processed."""
        logging.info(f"Source file location: {self.source_file_location}")
        try:
            files = os.listdir(self.source_file_location)
            logging.info(f"files processes are {files}")
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

class SchemaManager:
    def __init__(self, config_file, schema_file):
        self.config_file = config_file
        self.schema_file = schema_file
        self.schema = self.load_schema()
        self.config = self.load_config()

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

class FileProcessor:
    def __init__(self, file_manager, schema_manager):
        self.file_manager = file_manager
        self.schema_manager = schema_manager
        self.clean_records = pd.DataFrame()  # Initialize clean_records as an empty DataFrame
        self.bad_records = pd.DataFrame()    # Initialize bad_records as an empty DataFrame
        self.metadata = []  # List to store metadata about the bad records

    def process_files(self):
        """Process the list of files that passed the initial checks."""
        file_check_module_passed_files = self.file_manager.get_files_to_process()
        logging.info("Processing files")
        try:
            if file_check_module_passed_files:
                for present_file in file_check_module_passed_files:
                    logging.info(f"New test on file: {present_file}")
                    file_location = Path(self.file_manager.source_file_location) / present_file
                    self.clean_records = pd.read_csv(file_location, encoding='utf-8')

                    try:
                        phonenumber_check_attributes = self.schema_manager.config.get(present_file[0:-18], {}).get('phonenumber_check', [])
                        self.clean_phonenumber(present_file,phonenumber_check_attributes)
                    except Exception as e:
                        logging.error(f"phonenumber check failed because {e}")

                    try:
                        duplicate_check_attributes = self.schema_manager.config.get(present_file[0:-18], {}).get('duplicate_check', [])
                        self.duplicate_check(present_file, duplicate_check_attributes)
                    except Exception as e:
                        logging.error(f"duplicate check failed because {e}")
                    #
                    # try:
                    #     null_check_attributes = self.schema_manager.config.get(present_file[0:-18], {}).get('null_check', [])
                    #     self.null_check(present_file, null_check_attributes)
                    # except Exception as e:
                    #     logging.error(f"null check failed because {e}")

                    self.save_good_records(present_file, self.clean_records)

                    # Save metadata after processing the file
                    self.save_metadata(present_file)
            else:
                logging.info(f"no files present in source directory")
        except Exception as e:
            logging.exception(f"processing files failed. error {e}")
    def null_check(self, file, null_check_attributes):
        """Perform a null check on the specified attributes in the file."""
        logging.info(f"Performing null check on file: {file}")
        bad_records = pd.DataFrame()

        for attribute in null_check_attributes:
            if attribute in self.clean_records.columns:
                null_indices = self.clean_records[self.clean_records[attribute].isnull()].index
                if not null_indices.empty:
                    logging.warning(f"Null values found in {attribute} of file {file}")
                    bad_records = pd.concat([bad_records, self.clean_records.loc[null_indices]])

                    # Add metadata for null issue
                    self.metadata.append({
                        'Type_of_issue': 'null',
                        'Row_num_list': null_indices.tolist()
                    })

        # Remove bad records from clean_records
        self.clean_records = self.clean_records.drop(bad_records.index)

        # Append bad records to the main bad_records DataFrame
        self.bad_records = pd.concat([self.bad_records, bad_records])

        self.save_bad_records(file, bad_records)

    def duplicate_check(self, file, duplicate_check_attributes):
        """Perform a duplicate check on the specified attributes in the file."""
        logging.info(f"Performing duplicate check on file: {file}")
        bad_records = pd.DataFrame()

        if duplicate_check_attributes:
            duplicates = self.clean_records[self.clean_records.duplicated(subset=duplicate_check_attributes, keep=False)]
            if not duplicates.empty:
                logging.warning(f"Duplicate records found based on attributes {duplicate_check_attributes} in file {file}")
                bad_records = pd.concat([bad_records, duplicates])

                # Add metadata for duplicate issue
                self.metadata.append({
                    'Type_of_issue': 'duplicate',
                    'Row_num_list': duplicates.index.tolist()
                })

            # Remove duplicates from clean_records
            self.clean_records = self.clean_records.drop_duplicates(subset=duplicate_check_attributes, keep='first')

            # Append bad records to the main bad_records DataFrame
            self.bad_records = pd.concat([self.bad_records, bad_records])

            self.save_bad_records(file, bad_records)


    def clean_phonenumber(self, file, phonenumber_check_attributes):
        """Check and clean phone numbers in the specified attributes."""
        logging.info(f"Performing phone number check on file: {file}")
            # Split numbers into two columns: contact1 and contact2
        try:
            for i in phonenumber_check_attributes:
                df = self.clean_records[i]
                contact1 = []
                contact2 = []
                for j in range(len(df)):
                    valid_numbers = str(df.iloc[j]).strip().replace(r".","").replace(r"\r\n"," ").split(" ")
                    # valid_numbers = str(df.iloc[j]).replace(r".","").strip().replace(r" ","|").replace(r"\r\n","|").split("|")
                    mobile = []
                    phone = []
                    print(f"valid_numbers is{valid_numbers}")
                    for k in range(len(valid_numbers)):
                        print("k = ", k)
                        print("Num selected = ", valid_numbers[k])
                        print("len of num = ", len(valid_numbers[k]))
                        # if phone number is 10 digits
                        if len(valid_numbers[k]) == 10:
                            mobile.append(valid_numbers[k])
                        # if phone number is 8 digits and the prev number is 3 digits
                        elif len(valid_numbers[k]) == 8:
                            print("num len = 8 ")
                            if k > 0 and len(valid_numbers[k-1]) == 3:
                                phone.append(valid_numbers[k-1] + valid_numbers[k])
                            if k > 1 and len(valid_numbers[k-2]) == 3 and len(valid_numbers[k-1]) == 8:
                                phone.append(valid_numbers[k - 2] + valid_numbers[k])
                        print("mobile = ", mobile)
                        print("phone = ", phone)
                    if len(mobile) == 1 and len(phone) == 1:
                        contact1.append(mobile[0])
                        contact2.append(phone[0])
                    elif len(mobile) == 2 and len(phone) == 0:
                        contact1.append(mobile[0])
                        contact2.append(mobile[1])
                    elif len(mobile) == 0 and len(phone) == 2:
                        contact1.append(phone[0])
                        contact2.append(phone[1])
                    elif len(mobile) == 1 and len(phone) == 0:
                        contact1.append(mobile[0])
                        contact2.append('None')
                    elif len(mobile) == 0 and len(phone) == 1:
                        contact1.append(phone[0])
                        contact2.append('None')
                    else:
                        contact1.append('None')
                        contact2.append('None')

                    print("Record Done")

                    print(" contact 1 -> ", contact1)
                    print(" contact 2 -> ", contact2)
                """
                        passed = 0
                        if len(valid_numbers[k])>6 and len(valid_numbers[k])< 11:
                            print(f"clean number {valid_numbers[k]}")
                            if passed == 0:
                                contact1.append(valid_numbers[k])
                                passed += 1
                            elif passed == 1:
                                contact2.append(valid_numbers[k])
                                passed += 1
                            else:
                                contact2.append(None)
                """


                logging.info(f"contact 1 data {contact1}")
                logging.info(f"contact 2 data {contact2}")

                #     valid_numbers[0]= valid_numbers[0].replace(r"+","").replace(r" ","")
                #     try:
                #         if valid_numbers[1]:
                #             valid_numbers[1]=valid_numbers[1].replace(r"+","").replace(r" ","")
                #     except:
                #         pass
                #     if len(valid_numbers)>1:
                #         if len(valid_numbers[0])<13 or len(valid_numbers[0])>7:
                #             contact1.append(valid_numbers[0])
                #         else:
                #             contact1.append(None)
                #
                #         if len(valid_numbers[1])<13 and len(valid_numbers[1])>7:
                #             contact2.append(valid_numbers[1])
                #         else:
                #             contact2.append(None)
                #     else:
                #         if len(valid_numbers[0])<13 and len(valid_numbers[0])>7:
                #             contact1.append(valid_numbers[0])
                #         else:
                #             contact1.append(None)
                #         contact2.append(None)
                #
                # logging.info(f"{len(contact1)}{contact1}")
                # logging.info(f"{len(contact2)}{contact2}")
                #
                try:
                    self.clean_records['contact number 1'] = contact1
                    self.clean_records['contact number 2'] = contact2
                    print(i)
                    self.clean_records = self.clean_records.drop(columns=[i])
                except Exception as e:
                    logging.exception(f"phonenumber write failure because {e}")

        except Exception as e:
            logging.error(f"phone number check failed(within function) because {e}")

    def test_phone_number(self,file,phone_column):
        logging.info(f"testing phone numbers on column: {phone_column}")
        try:
            # # logging.info(f"{type(phone_column)}")
            # bad_records = pd.DataFrame()
            # phone_numbers = self.clean_records[phone_column]
            # # improper_length = phone_numbers[(phone_numbers[phone_numbers].srt.len()<8)|(phone_numbers[phone_column].str.len()>12)]
            # improper_length = phone_numbers[phone_numbers[phone_column].str.len()>12]
            # # print(long_number.to_string())
            pass
        except Exception as e:
            logging.info(f" testing number failed cuz {e}")
        try:
            pass
        except Exception as e:
            logging.exception(f"testinn phone number on column {phone_column} failed because {e}")
        return True

    def split_and_clean_phone_numbers(self):
        """Split and clean phone numbers, returning two separate numbers if possible."""

        contact1 = []
        contact2 = []

        for i in range(len(df)):
            valid_numbers = df.iloc[i].strip().replace(r"\r \n", "|").split("|")
            print(f"{valid_numbers} {len(valid_numbers)}")
            valid_numbers[0] = valid_numbers[0].replace(r"+", "").replace(r" ", "")
            try:
                if valid_numbers[1]:
                    valid_numbers[1] = valid_numbers[1].replace(r"+", "").replace(r" ", "")
            except:
                pass
            if len(valid_numbers) > 1:
                contact1.append(valid_numbers[0])
                contact2.append(valid_numbers[1])
            contact1.append(valid_numbers[0])
            contact2.append(None)

        return [contact1, contact2]

    def save_good_records(self, file, df):
        """Save clean records to a file."""
        try:
            if df is not None and not df.empty:
                clean_file_location = Path(self.file_manager.output_file_location) / file.replace('.csv', '.out.csv')
                df.to_csv(clean_file_location, index=False, encoding='utf-8')
                logging.info(f"Clean records saved to {clean_file_location}")
        except Exception as e:
            logging.error(f"Error saving good records: {e}")

    def save_bad_records(self, file, df):
        """Save bad records to a file."""
        try:
            if df is not None and not df.empty:
                bad_file_location = Path(self.file_manager.output_file_location) / file.replace('.csv', '.bad.csv')
                if bad_file_location.exists():
                    df.to_csv(bad_file_location, mode='a', index=False, encoding='utf-8')
                else:
                    df.to_csv(bad_file_location, index=False, encoding='utf-8')
                logging.info(f"Bad records saved to {bad_file_location}")
        except Exception as e:
            logging.error(f"Error saving bad records: {e}")

    def save_metadata(self, file):
        """Save metadata about the issues found in the file."""
        try:
            metadata_file_location = Path(self.file_manager.output_file_location) / file.replace('.csv', '.metadata.csv')
            metadata_df = pd.DataFrame(self.metadata)
            metadata_df.to_csv(metadata_file_location, index=False, encoding='utf-8')
            logging.info(f"Metadata saved to {metadata_file_location}")
        except Exception as e:
            logging.error(f"Error saving metadata: {e}")


if __name__ == '__main__':
    source_file_location = sys.argv[1]
    scanned_files = sys.argv[2]
    config_file = sys.argv[3]
    schema_file = sys.argv[4]
    output_file_location = sys.argv[5]

    file_manager = FileManager(source_file_location, scanned_files, output_file_location)
    schema_manager = SchemaManager(config_file, schema_file)
    processor = FileProcessor(file_manager, schema_manager)

    processor.process_files()
