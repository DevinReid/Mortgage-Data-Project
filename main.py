import pandas as pd
import numpy as np
import sqlite3
import random
from sqlalchemy import create_engine
import populateDB as popDB
print(pd.__version__)
print("Hello World")

file_path = r"C:\Users\Dreid\Desktop\Brain\Projects\MortgageData\sample data.xlsx"
db_path = r'C:\Users\Dreid\Desktop\Brain\Projects\MortgageData\mortgage_data.db'

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

    
def add_range_column_FICO():
    global source_connect, source_cursor

    if source_connect is None:
        initialize_db()

    df =pd.read_sql_query("SELECT * FROM mortgages", source_connect)

    if 'FICOScoreBand' in df.columns and df['FICOScoreBand'].notnull().all():
        print("FICOScoreBand and SortOrderFICOScoreBand columns already exist with data. Skipping execution.")
        return

    column_to_catagorize ='FICO'
    
    bin_edges = [300,580,620,650,700,740,800]
    bin_labels = ['300 - 579', '580 - 619', '620 - 649', '650 - 699', '700 - 739', '740 - 800']


    df['FICOScoreBand'] = pd.cut(df[column_to_catagorize], bins=bin_edges, labels=bin_labels, include_lowest=True)
    

    try:
        source_cursor.execute('ALTER TABLE mortgages ADD COLUMN FICOScoreBand TEXT')
    except sqlite3.OperationalError:
        # Column already exists, no need to add
        pass


    def calculate_sort_order(range_label):
        if pd.isnull(range_label):
            return np.nan
        
        min_value= float(range_label.split(' - ')[0])
        max_value = float(range_label.split(' - ')[1]) * 1000
        return min_value + (max_value - min_value)/2
    
    df['SortOrderFICOScoreBand'] = df['FICOScoreBand'].apply(calculate_sort_order)

    try:
        source_cursor.execute('ALTER TABLE mortgages ADD COLUMN SortOrderFICOScoreBand REAL')
    except sqlite3.OperationalError:
        # Column already exists, no need to add
        pass

    for index, row in df.iterrows():
        source_cursor.execute('''
                        UPDATE mortgages
                        SET FICOScoreBand = ?, SortOrderFICOScoreBand = ?
                        WHERE RowID = ?
                        ''', (row['FICOScoreBand'], row['SortOrderFICOScoreBand'], index + 1))
        
    
        
    source_connect.commit()
    

    print("Data with SortOrderFICOScoreBand and FICOScoreBand column has been successfully updated in the SQL database")


def add_calculation_column_ProfitMarginPercentage():
    global source_connect, source_cursor

    if source_connect is None:
        initialize_db()

    df =pd.read_sql_query("SELECT * FROM mortgages", source_connect)

    if 'ProfitMarginPercentage' in df.columns and df['ProfitMarginPercentage'].notnull().all():
        print("ProfitMarginPercentage column already exist with data. Skipping execution.")
        return



    df['ProfitMarginPercentage'] = ((df['InvestorExpectedPrice'] - df['MortgageCost'])/df['InvestorExpectedPrice'])


    print(df['ProfitMarginPercentage'].head())
    

    try:
        source_cursor.execute('ALTER TABLE mortgages ADD COLUMN ProfitMarginPercentage REAL')
    except sqlite3.OperationalError:
        # Column already exists, no need to add
        pass



    for index, row in df.iterrows():
        source_cursor.execute('''
                        UPDATE mortgages
                        SET ProfitMarginPercentage = ?
                        WHERE RowID = ?
                        ''', (row['ProfitMarginPercentage'], index + 1))
        
    
        
    source_connect.commit()
    

    print("Data with ProfitMarginPercentage column has been successfully updated in the SQL database")

def edit_data_for_charts_InvestorPrice():
    global source_connect, source_cursor

    if source_connect == None:
        initialize_db()

    df = pd.read_sql_query("SELECT * FROM mortgages", source_connect)

    investor_expected_price = 'InvestorExpectedPrice'
    mortgage_cost_column  = 'MortgageCost'

    # source_cursor.execute(f"SELECT id, {mortgage_cost} FROM mortgages WHERE {investor_expected_price} IS NULL")
    # rows = source_cursor.fetchall()

    empty_cells = df[investor_expected_price].isna()

    for index, row in df[empty_cells].iterrows():
       
        mortgage_cost = row[mortgage_cost_column]
        random_value = round(random.uniform(0.1, 3.0), 1)
        new_value = 1 + mortgage_cost +random_value
        df.at[index,investor_expected_price] = new_value
        
    



    df.to_sql('mortgages', source_connect, if_exists='replace', index= False)

    print("Empty Cells Populated")

    add_calculation_column_ProfitMarginPercentage()


    source_connect.commit()

def add_column_LoanOfficer():
    global source_connect, source_cursor

    branch_officers = {
        'The Cottonport Bank': [
            'M. Reed', 'S. Morgan', 'J. Patel', 'E. Grant', 'R. Foster',
            'O. Diaz', 'T. James', 'A. Bennett'
        ],
        'WINNSBORO STATE BANK & TRUST COMPANY': [
            'J. Ward', 'K. Brooks', 'M. Cooper', 'B. Hughes', 'A. Coleman',
            'J. Parker', 'L. Phillips'
        ],
        'Progressive Bank': [
            'E. Ross', 'M. Jenkins', 'C. Ramirez', 'A. Perry', 'N. Simmons',
            'A. Fisher'
        ],
        'Merchants Bank of Indiana': [
            'E. Collins', 'M. Price', 'N. Sanders', 'A. Murphy', 'D. Roberts',
            'V. Gomez', 'L. Hill'
        ],
        'Marion State Bank': [
            'B. Rogers', 'E. Howard', 'C. Hayes', 'C. Richardson', 'J. Ward',
            'L. Martinez', 'J. Mitchell'
        ],
        'First State Bank of the South, Inc': [
            'A. Carter', 'H. Kelly', 'W. White', 'S. Green', 'D. Turner',
            'E. Thomas'
        ],
        'First Bank': [
            'A. Thompson', 'M. Brown', 'I. Lee', 'A. Wright', 'G. Hall',
            'C. Moore'
        ],
        'Farmers-Merchants Bank of Illinois': [
            'Z. Johnson', 'H. Walker', 'E. Baker', 'S. Wood', 'L. Perez',
            'J. Collins'
        ],
        'Citizens Savings Bank': [
            'C. King', 'E. Butler', 'S. Price', 'J. Bell', 'H. Turner',
            'O. Gray'
        ]}

    try:
        source_cursor.execute("ALTER TABLE mortgages ADD COLUMN LoanOfficer TEXT;")
        print("Column 'LoanOfficer' added")
    except sqlite3.OperationalError:
        print("Column 'LoanOfficer' may already exist")

    source_cursor.execute("SELECT LoanID, Branch FROM mortgages WHERE LoanOfficer IS NULL OR LoanOfficer = '';")
    rows = source_cursor.fetchall()

    for row in rows:
        row_id, branch = row
        if branch in branch_officers:
            loan_officer = random.choice(branch_officers[branch])
            source_cursor.execute("UPDATE mortgages SET LoanOfficer = ? WHERE LoanID = ?;", (loan_officer,row_id))

    source_connect.commit()
    print("Loan officer names have been assigned to blank cells.")


initialize_db()
create_main_table()

popDB.main()
add_range_column_MortgageAmount()

add_range_column_MortgageRate()

add_range_column_FICO()

add_calculation_column_ProfitMarginPercentage()

#Plump up the sample data we received

edit_data_for_charts_InvestorPrice()

add_column_LoanOfficer()

source_connect.close()
