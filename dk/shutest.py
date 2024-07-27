import pandas as pd

file_loc = r'../../Documents/testing/source_data/data_file_20240726129048.csv'

df = pd.read_csv(file_loc,encoding='utf-8')
df = df['phone']

new_contacts_group = []
contact1 = []
contact2 = []

for i in range(len(df)):
    valid_numbers = df.iloc[i].strip().replace(r"\r\n","|").split("|")
    print(f"{valid_numbers} {len(valid_numbers)}")
    valid_numbers[0]=valid_numbers[0].replace(r"+","").replace(r" ","")
    try:
        if valid_numbers[1]:
            valid_numbers[1]=valid_numbers[1].replace(r"+", "").replace(r" ", "")
    except:
        pass
    if len(valid_numbers)>1:
        contact1.append(valid_numbers[0])
        contact2.append(valid_numbers[1])
    contact1.append(valid_numbers[0])
    contact2.append(None)
































































    # for j in range(len(valid_numbers)):
    #     print(valid_numbers[j])

print(contact1)
print(contact2)