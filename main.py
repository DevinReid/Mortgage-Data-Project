import pandas as pd
from sqlalchemy import create_engine
print(pd.__version__)
print("Hello World")

file_path = r"C:\Users\Dreid\Documents\Projects\MortgageData\sample data.xlsx"
engine = create_engine('sqlite:///mortgage_data.db')

if not r'C:\Users\Dreid\Documents\Projects\MortgageData\mortgage_data.db':
    df = pd.read_excel(file_path)

    df.to_sql('mortgages', engine, if_exists='replace', index=False)

    print("Data has been successfully written to the SQL database")
else:
    print("File Already Exists")
    

