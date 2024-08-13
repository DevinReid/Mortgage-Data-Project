import pandas as pd
import sqlite3
from sqlalchemy import create_engine
print(pd.__version__)
print("Hello World")

file_path = r"C:\Users\Dreid\Documents\Projects\MortgageData\sample data.xlsx"
db_path = r'C:\Users\Dreid\Documents\Projects\MortgageData\mortgage_data.db'

def create_main_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
                   SELECT name FROM sqlite_master WHERE type='table' AND name='mortgages';
                   ''')
    table_exists =cursor.fetchone()

    if not table_exists:
        engine = create_engine('sqlite:///mortgage_data.db')
        
        df = pd.read_excel(file_path)

        df.to_sql('mortgages', engine, if_exists='replace', index=False)

        print("Data has been successfully written to the SQL database")
    else:
        print("File Already Exists")

    conn.close

    

source_connect = sqlite3.connect('mortgage_data.db')
source_cursor = source_connect.cursor()

def chart_one():
    source_cursor.execute('''
        SELECT AmortTerm, COUNT(*) AS LoanCount
        FROM mortgages
        GROUP BY AmortTerm
        ORDER BY AmortTerm; ''')

    data = source_cursor.fetchall()

    source_cursor.execute('''
        CREATE TABLE IF NOT EXISTS AmortTermLoanCount (
            AmortTerm INTEGER,
            LoanCount INTEGER
        )
    ''')

    # Insert the fetched data into the new table
    source_cursor.executemany('''
        INSERT INTO AmortTermLoanCount (AmortTerm, LoanCount)
        VALUES (?, ?)
    ''', data)

    # Commit the changes and close the connections
    source_connect.commit()
    source_connect.close()


    print("Data Transfer ONE Sucess")


def chart_two():
    source_connect = sqlite3.connect('mortgage_data.db')
    source_cursor = source_connect.cursor()

    source_cursor.execute('''
        SELECT Branch, COUNT(*) as total_loans
        FROM mortgages
        GROUP BY Branch ''')
    
    df = pd.read_sql_query('''
                            SELECT Branch, COUNT (*) as total_loans
                            FROM mortgages
                            GROUP BY Branch ''', source_connect)

    df['percentage'] = (df['total_loans'] / df['total_loans'].sum()) * 100

    source_cursor.execute('''
        CREATE TABLE IF NOT EXISTS  BranchLoanCount (
        Branch TEXT,
        total_loans INTEGER
        percentage REAL)
        ''')
        
    source_connect.commit

    df.to_sql('BranchLoanCount', source_connect, if_exists='replace', index=False)

    source_connect.close()

    print("Data transfer TWO sucess")

def chart_three():
    source_connect =sqlite3.connect('mortgage_data.db')
    source_cursor = source_connect.cursor()

    source_cursor.execute('''
                        SELECT ClientLoanProgram, COUNT(*) as count
                        FROM mortgages
                        GROUP BY ClientLoanProgram
                          ''')
    
    df = pd.read_sql_query('''
                        SELECT ClientLoanProgram, COUNT(*) as count
                        FROM mortgages
                        GROUP BY ClientLoanProgram
                          ''', source_connect)
    
    source_cursor.execute('''
                          CREATE TABLE IF NOT EXISTS LoanProgramCount (
                          ClientLoanProgram TEXT,
                          count INTEGER
                        
                          )''')
    
    source_connect.commit

    df.to_sql('LoanProgramCount', source_connect, if_exists='replace',index=False)
    
    source_connect.close()

    print("Loan Program Typer Counted and transfered into table successfully.")

create_main_table()
chart_one()
chart_two()
chart_three()