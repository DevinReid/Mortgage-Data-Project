import random
import sqlite3
import numpy as np
from datetime import datetime, timedelta



db_path = r'C:\Users\Dreid\Desktop\Brain\Projects\MortgageData\mortgage_data.db'

source_connect = None
source_cursor = None


def initialize_db():
    global source_cursor, source_connect, db_path
    source_connect = sqlite3.connect(db_path)
    source_cursor = source_connect.cursor()

def set_DataDate():
    data_date = '2023-12-29 00:00:00.000000'
    return data_date

def generate_LoanID():
    global source_cursor, source_connect

    def get_existing_loan_ids():
        source_cursor.execute("SELECT LoanID FROM mortgages")
        result = source_cursor.fetchall()
        return {row[0] for row in result}

    def generate_unique_loan_id(existing_ids):
        while True:
            new_loan_id = random.randint(7000,17000)
            if new_loan_id not in existing_ids:
                return new_loan_id
            
    new_loan_id = generate_unique_loan_id(get_existing_loan_ids())
    return new_loan_id
def generate_LoanProgram():
    programs = ['CONV30', 'CONV15', 'CONV10', 'CONV20', 'FHA30', 'USDA30', 'VA30']
    return random.choice(programs)

def generate_InterestRate():
    interest_rate = random.choice(np.arange( 5.5, 8.75, 0.25))
    return interest_rate

def generate_MortgageAmount():
    random_amount = random.randint(54000,800000)
    return random_amount

def generate_LockEffectiveDate_and_LockExpDate():
    start_date = datetime(2023, 2, 23, 0, 0, 0)
    end_date = datetime(2023,12,30,0,0,0)

    day_difference = (end_date - start_date).days

    random_days = random.randint(0, day_difference)

    random_effective_date = start_date + timedelta(days=random_days)

    lock_lengths = [30,45,60]

    random_expire_date = random_effective_date + timedelta(days=random.choice(lock_lengths))


    return random_effective_date, random_expire_date

def generate_AmortTerm():
    term_range = [120,180,240,360]
    rand_term = random.choice(term_range)
    return rand_term

def generate_BackEndRatio():
    rand_number = random.uniform(29.7, 55.61)
    return rand_number

def generate_branch():
    branches = ['The Cottonport Bank','WINNSBORO STATE BANK & TRUST COMPANY','Progressive Bank',  'Marion State Bank', 'First State Bank of the South, Inc', 'First Bank', 'Farmers-Merchants Bank of Illinois', 'Citizens Savings Bank']
    branch = random.choice(branches)
    return branch

def generate_BusinessChannel():
    channels =[ 'CorrNonDel', 'Correspondent', 'Banked - Retail', 'WholesaleBCU']
    channel = random.choice(channels)
    return channel

def generate_fico():
    rand_fico = random.uniform(640,800)
    return rand_fico

def gen_MortgageCost():
    rand_cost = random.uniform(95.949, 106.928)
    return rand_cost

def gen_InvestorExpectedPrice():
    rand_price = random.uniform(103.788, 107.888)
    return rand_price


def populate_spreadsheet():
    random_effective_date, random_expire_date = generate_LockEffectiveDate_and_LockExpDate()
    mortgage_amount = generate_MortgageAmount()
    return {
        'DataDate': set_DataDate(),
        'LoanID':generate_LoanID(),
        'LoanProgram': generate_LoanProgram(),
        'MortgageRate' : generate_InterestRate(),
        'MortgageAmount':mortgage_amount,
        'LockEffectiveDate': random_effective_date,
        'LockExpDate': random_expire_date,
        'OriginalBalance': mortgage_amount,
        'AmortTerm' : generate_AmortTerm(),
        'BackEndRatio' : generate_BackEndRatio(),
        'Branch' : generate_branch(),
        'BusinessChannel' : generate_BusinessChannel(),
        'FICO' : generate_fico(),
        'MortgageCost' : gen_MortgageCost(),
        'InvestorExpectedPrice' : gen_InvestorExpectedPrice(),
    }

def insert_data(num_rows):
    global source_connect, source_cursor
    for row in range(num_rows):
        data = populate_spreadsheet()
        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        sql = f"INSERT INTO mortgages ({columns}) VALUES ({placeholders})"
        source_cursor.execute(sql,tuple(data.values()))
    source_connect.commit()
    print(f"Inserted {num_rows} new rows into mortgages table")


def main():
    initialize_db()
    insert_data(300)

