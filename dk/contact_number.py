# import re
# import pandas as pd
#
# contact_df = pd.read_csv(r'..\..\Documents\testing\source_data\data_file_20240726129048.csv',encoding='utf-8',delimiter=',')
#
# contact_df['phone'] = contact_df['phone'].str.strip()
#
# split_phones = contact_df['phone'].str.split(r'\r\n',n=1,expand=True)
#
# contact_df['Contact number 1'] = split_phones[0].str.strip()
# contact_df['Contact number 2'] = split_phones[1].str.strip() if len(split_phones.columns) > 1 else None
#
# print(contact_df.to_string())

import pandas as pd
import re

file_loc = r'..\..\Documents\testing\source_data\data_file_20240726129048.csv'

# Read the input file (assuming it's a CSV file with a single column 'phone')
df = pd.read_csv(file_loc)

# Extract the phone numbers from the 'phone' column
content = df['phone'].apply(split_numbers())

print(content)

# Replace \r\n with a placeholder and remove unnecessary spaces
content = re.sub(r'\s*\r\n\s*', '|', content.strip())
# Replace multiple spaces with a single space
#print(content)
content = re.sub(r'\s+', ' ', content)

# Split by the placeholder to get groups of phone numbers
phone_groups = content.split('|')

# Prepare lists to store the phone numbers
col1 = []
col2 = []

for group in phone_groups:
    numbers = re.split(r'\s+', group.strip())
    # Pair the numbers
    for i in range(0, len(numbers), 2):
        if i + 1 < len(numbers):
            col1.append(numbers[i])
            col2.append(numbers[i + 1])
        else:
            col1.append(numbers[i])
            col2.append('')

# Create a DataFrame
df_output = pd.DataFrame({
    'contact number 1': col1,
    'contact number 2': col2
})

# Display the DataFrame
#print(df_output)
