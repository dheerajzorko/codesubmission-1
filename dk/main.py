from pathlib import Path
import pandas as pd
import sys
import os
import csv


def check_file():
    print("\tsource_file_location=", source_file_location)
    files = os.listdir(source_file_location)
    list_of_files_to_be_tested = []
    for file in files:
        print(file)
        if check_if_file_already_scanned(file):
            print("\t\tfile exists:", file)
            pass
        else:
            print("\t\tfile", file, "is a new file, proceed with further processing")
            if is_csv_file(file):
                print("\t\t\t", file, "file is .csv ")
                if has_records(file):
                    print("\t\t\t\t", file, "has", has_records(file), "records")
                    list_of_files_to_be_tested.append(file)

    return list_of_files_to_be_tested

def has_records(file_path):
    line_count = 0
    file_location = source_file_location + "/" + file_path
    with open(file_location, mode='r') as file:
        reader = csv.reader(file)
        for i in reader:
            return True
    return False


def is_csv_file(file):
    suffix = Path(file).suffix.lower()
    if suffix == ".csv":
        return True
    return False


def check_if_file_already_scanned(file):
    print("\t\tscanned_files=", scanned_files)
    df = pd.read_csv(scanned_files)
    for s in df['files_scanned']:
        if s == file:
            return True
    return False


def check_schema(file):
    return True


def check_tests_in_config(file):
    where_condition_null_check = ""
    df2 = pd.read_csv(config_file)
    file_df = df2[df2['file_prefix'].str[:-18] == file[:-18]]
    print("file_df\n", file_df['test'])

    nullcheck_df = file_df[file_df['test'] == "nullcheck"]
    print("null check list", nullcheck_df)

    # for i in nullcheck_df['attribute']:
    #     if where_condition_null_check != "":
    #         where_condition_null_check += " and "+i+" is not null"
    #     else:
    #         where_condition_null_check = i+" is not null "

    # null_check_list = nullcheck_df.tolist()
    # print("nullCheckdf\n",null_check_list)
    print(where_condition_null_check)
    #
    # for f in df2['file_prefix']:
    #     if f[:-18] == file[:-18]:
    #         nullcheck_df = df2[df2['test'] == "nullcheck"]
    #
    #         # print(df2[['test']])
    #         # if 'test' in df2.columns:
    #         #     if df2[df2['file_prefix'] == f]['test'].iloc[0] == "nullcheck":
    #         #             print(df2[df2['file_prefix'] == f]['attribute'].iloc[0])
    #         #     else:
    #         #         print("No nullcheck test found for this file.")
    #         # else:
    #         #     print("Test column not found in the configuration.")
    #     else:
    #         print("No matching file prefix found.")
    return True


def test_data_quality(file):
    generate_passed_subset(file)
    #    if check_schema(file):
    #        check_tests_in_config(file)

    return True


def generate_passed_subset(file):
    print("generate_passed_subset", file)
    file_location = source_file_location+"/"+file
    passed_df = pd.read_csv(file_location)
    passed_df.show()
    return True


def generate_failed_subset(file):
    pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    source_file_location = sys.argv[1]
    scanned_files = sys.argv[2]
    config_file = sys.argv[3]
    schema = sys.argv[4]
    output_file_location = sys.argv[5]
    file_check_module_passed_files = check_file()
    print("MAIN")
    for present_files in file_check_module_passed_files:
        print("NEW TEST on FILE", present_files)
        test_data_quality(present_files)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
