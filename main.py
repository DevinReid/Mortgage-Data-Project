import pandas as pd
import numpy as np
import sqlite3
from sqlalchemy import create_engine
print(pd.__version__)
print("Hello World")

file_path = r"C:\Users\Dreid\Documents\Projects\MortgageData\sample data.xlsx"
db_path = r'C:\Users\Dreid\Documents\Projects\MortgageData\mortgage_data.db'

source_connect = None
source_cursor = None

def initialize_db():
    global source_cursor, source_connect, db_path
    source_connect = sqlite3.connect(db_path)
    source_cursor = source_connect.cursor()

def create_main_table():
    global source_connect, source_cursor
   
    source_cursor.execute('''
                   SELECT name FROM sqlite_master WHERE type='table' AND name='mortgages';
                   ''')
    table_exists =source_cursor.fetchone()

    if not table_exists:
        engine = create_engine('sqlite:///mortgage_data.db')
        
        df = pd.read_excel(file_path)


        #This looks at all of the columns in my excel file and checks for the type int64 and converts it to int 32 so powerBI can read it
        for column in df.select_dtypes(include=['int64']).columns:
            df[column] = df [column].astype('int16')


        # debug print for above filter
        print("Data types before saving:")
        print(df.dtypes)

        df.to_sql('mortgages', engine, if_exists='replace', index=False)

        print("Data has been successfully written to the SQL database")
    else:
        print("File Already Exists")


def add_range_column_MortgageAmount():
    global source_connect, source_cursor

    if source_connect is None:
        initialize_db()

    df =pd.read_sql_query("SELECT * FROM mortgages", source_connect)

    if 'MortgageAmountRange' in df.columns and df['MortgageAmountRange'].notnull().all():
        print("MortgageAmountRange and SortOrderMortgageAmount columns already exist with data. Skipping execution.")
        return

    column_to_catagorize ='MortgageAmount'
    num_bins = 6
    bin_edges = np.linspace(df[column_to_catagorize].min(), df[column_to_catagorize].max(), num_bins + 1)
    bin_labels = [f'${bin_edges[i]/1000:.1f}k - ${bin_edges[i+1]/1000:.1f}k' for i in range(num_bins)]

    df['MortgageAmountRange'] = pd.cut(df[column_to_catagorize], bins=bin_edges, labels=bin_labels, include_lowest=True)
    

    try:
        source_cursor.execute('ALTER TABLE mortgages ADD COLUMN MortgageAmountRange TEXT')
    except sqlite3.OperationalError:
        # Column already exists, no need to add
        pass


    def calculate_sort_order(range_label):
        if pd.isnull(range_label):
            return np.nan
        
        min_value= float(range_label.split(' - ')[0].replace('$', '').replace('k', '')) * 1000
        max_value = float(range_label.split(' - ')[1].replace('$', '').replace('k', '')) * 1000
        return min_value + (max_value - min_value)/2
    
    df['SortOrderMortgageAmount'] = df['MortgageAmountRange'].apply(calculate_sort_order)

    try:
        source_cursor.execute('ALTER TABLE mortgages ADD COLUMN SortOrderMortgageAmount REAL')
    except sqlite3.OperationalError:
        # Column already exists, no need to add
        pass

    for index, row in df.iterrows():
        source_cursor.execute('''
                        UPDATE mortgages
                        SET MortgageAmountRange = ?, SortOrderMortgageAmount = ?
                        WHERE RowID = ?
                        ''', (row['MortgageAmountRange'], row['SortOrderMortgageAmount'], index + 1))
        
    
        
    source_connect.commit()
    


    df_updated = pd.read_sql_query("SELECT * FROM mortgages", source_connect)


    print("Data with MortgageAmountRange column has been successfully updated in the SQL database")


def add_range_column_MortgageRate():
    global source_connect, source_cursor, db_path

    if source_connect is None:
        initialize_db()

    df = pd.read_sql_query("SELECT * FROM mortgages", source_connect)

    if 'MortgageRateBand' in df.columns and df['MortgageRateBand'].notnull().all():
        print("MortgageRateBand column already exists with data. Skipping execution.")
        return

    column_to_catagorize = 'MortgageRate'

    bins =np.arange(5.5, 9.0, 0.5)
    labels =[f'{bins[i]}% - {bins[i+1]}%' for i in range(len(bins)-1)]

    df['MortgageRateBand'] = pd.cut(df[column_to_catagorize], bins=bins, labels=labels, include_lowest=True)

    try:
        source_connect.execute('ALTER TABLE mortgages ADD COLUMN MortgageRateBand TEXT')
    except sqlite3.OperationalError:
        pass

    def calculate_sort_order(range_label):
        if pd.isnull(range_label):
            return np.nan
        
        min_value= float(range_label.split(' - ')[0].replace('%', '')) 
        max_value = float(range_label.split(' - ')[1].replace('%', ''))
        return min_value + (max_value - min_value)/2
    
    df['SortOrderMortgageRateBand'] = df['MortgageRateBand'].apply(calculate_sort_order)

    try: 
        source_connect.execute('ALTER TABLE mortgages ADD COLUMN SortORderMortgageRateBand REAL')
    except sqlite3.OperationalError:
        pass

    for index, row in df.iterrows():
        source_cursor.execute('''
                              UPDATE mortgages
                              SET MortgageRateBand = ?, SortOrderMortgageRateBand = ?
                              WHERE RowID = ?
                              ''', (row['MortgageRateBand'], row['SortOrderMortgageRateBand'], index + 1))
        
        
    source_connect.commit()

    print("Data with MortgageRateBand column has been successfully updated in the SQL database")

    

# def chart_one():
#     source_cursor.execute('''
#         SELECT AmortTerm, COUNT(*) AS LoanCount
#         FROM mortgages
#         GROUP BY AmortTerm
#         ORDER BY AmortTerm; ''')

#     data = source_cursor.fetchall()

#     source_cursor.execute('''
#         CREATE TABLE IF NOT EXISTS AmortTermLoanCount (
#             AmortTerm INTEGER,
#             LoanCount INTEGER
#         )
#     ''')

#     # Insert the fetched data into the new table
#     source_cursor.executemany('''
#         INSERT INTO AmortTermLoanCount (AmortTerm, LoanCount)
#         VALUES (?, ?)
#     ''', data)

#     # Commit the changes and close the connections
#     source_connect.commit()
#     source_connect.close()


#     print("Data Transfer ONE Sucess")


# # def chart_two():
# #     source_connect = sqlite3.connect('mortgage_data.db')
# #     source_cursor = source_connect.cursor()

# #     source_cursor.execute('''
# #         SELECT Branch, COUNT(*) as total_loans
# #         FROM mortgages
# #         GROUP BY Branch ''')
    
# #     df = pd.read_sql_query('''
# #                             SELECT Branch, COUNT (*) as total_loans
# #                             FROM mortgages
# #                             GROUP BY Branch ''', source_connect)

# #     df['percentage'] = (df['total_loans'] / df['total_loans'].sum()) * 100

# #     source_cursor.execute('''
# #         CREATE TABLE IF NOT EXISTS  BranchLoanCount (
# #         Branch TEXT,
# #         total_loans INTEGER
# #         percentage REAL)
# #         ''')
        
# #     source_connect.commit

# #     df.to_sql('BranchLoanCount', source_connect, if_exists='replace', index=False)

# #     source_connect.close()

# #     print("Data transfer TWO sucess")

# # def chart_three():
#     source_connect =sqlite3.connect('mortgage_data.db')
#     source_cursor = source_connect.cursor()

#     source_cursor.execute('''
#                         SELECT ClientLoanProgram, COUNT(*) as count
#                         FROM mortgages
#                         GROUP BY ClientLoanProgram
#                           ''')
    
#     df = pd.read_sql_query('''
#                         SELECT ClientLoanProgram, COUNT(*) as count
#                         FROM mortgages
#                         GROUP BY ClientLoanProgram
#                           ''', source_connect)
    
#     source_cursor.execute('''
#                           CREATE TABLE IF NOT EXISTS LoanProgramCount (
#                           ClientLoanProgram TEXT,
#                           count INTEGER
                        
#                           )''')
    
#     source_connect.commit

#     df.to_sql('LoanProgramCount', source_connect, if_exists='replace',index=False)
    
#     source_connect.close()

#     print("Loan Program Typer Counted and transfered into table successfully.")

initialize_db()
create_main_table()

add_range_column_MortgageAmount()

add_range_column_MortgageRate()

# chart_one()
# chart_two()
# chart_three()

source_connect.close()
