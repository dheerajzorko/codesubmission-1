import chardet

with open('..\..\Documents\\testing\source_data\data_file_20210527182732.csv', 'rb') as f:
    result = chardet.detect(f.read())
print(result)
