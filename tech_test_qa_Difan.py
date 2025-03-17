from sqlalchemy import create_engine, inspect
import pandas as pd
import os 
from datetime import datetime

# Load database credentials from environment variables or any methods that can keep credentials secured. 
RDS_URL = os.getenv('RDS_URL')
PORT = os.getenv('PORT')
DB_NAME = os.getenv('DB_NAME')
USERNAME = os.getenv('DB_USERNAME')
PASSWORD = os.getenv('DB_PASSWORD')

# Assuming the company has the table/list to manage valid currencies and symbols. 
VALID_CURRENCIES = [
    "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "HKD", "NZD"
]

VALID_SYMBOLS = ['EURUSD', 'XAUUSD', 'EURGBP', 'USDJPY', 'GBPCAD', 'EURCHF',
    'NZDUSD', 'XTIUSD', 'AUDUSD', 'CADCHF', 'EURCAD', 'AUDNZD',
    'XBRUSD', 'XAGUSD', 'NZDJPY', 'USDCHF', 'GBPUSD', 'AUDJPY',
    'GBPAUD', 'EURJPY', 'US30', 'GBPJPY', 'USDCAD', 'EURAUD', 'GBPNZD',
    'GBPCHF', 'NZDCAD', 'AUDSGD', 'USDSGD', 'CHFJPY', 'AUDCAD',
    'EURNZD', 'USDNOK', 'NZDCHF', 'CHCUSD', 'NAS100', 'USIDX', 'GER30',
    'COFFEE', 'XPTUSD', 'CADJPY', 'BTCUSD', 'HK50', 'EURSGD', 'AUDCHF',
    'EURCNH', 'US500', 'NZDSEK', 'AUDCNH', 'NZDSGD', 'GBPSGD',
    'USDZAR', 'JPN225', 'HSCHKD', 'USDSEK', 'USDCNH', 'AUS200',
    'FRA40', 'VIX', 'USDMXN', 'SGDJPY', 'UK100', 'USDCZK', 'USD,CHF',
    'EURHKD', 'GBPSEK', 'BCHUSD', 'ETHUSD', 'EURNOK', 'USDRUB',
    'USDDKK'
    ]


# Check whether string value contains the unexpected characters/strings.
def unexpected_strings(table_name, df):
    unvalid_pattern = r'[^a-zA-Z0-9]'  # Matches any character that is not a letter or digit.

    if table_name == "trades":
        # Filter the unexpect characters appearing in the "ticket_hash","login_hash","server_hash", assuming the hash value doesn't contain any special symbols like !@#$%^.
        unexpected_character = df[
            df['ticket_hash'].str.contains(unvalid_pattern) | df['login_hash'].str.contains(unvalid_pattern) | df['server_hash'].str.contains(unvalid_pattern)
            ]
        if not unexpected_character.empty:
            print(f"Unexpected character are found in table {table_name}.")

        # Filter the invalid symbols which doesn't appear in the system. 
        unexpected_symbols = df[~df["symbol"].isin(VALID_SYMBOLS)]
        if not unexpected_symbols.empty:
            print(f"Unexpected symbols are found in table {table_name}")

    elif table_name == "users":
        # Filter the unexpect characters appearing in the "login_hash","server_hash", assuming the hash value doesn't contain any special symbols like !@#$%^.
        unexpected_character = df[
            df['login_hash'].str.contains(unvalid_pattern) | df['server_hash'].str.contains(unvalid_pattern)
            ]
        if not unexpected_character.empty:
            print(f"Unexpected character are found in table {table_name}.")

        # Filter the invalid currency which doesn't appear in the system. 
        unexpected_currency = df[~df["currency"].isin(VALID_CURRENCIES)]
        if not unexpected_currency.empty:
            print(f"Unexpected currency are found in table {table_name}")

# Check whether numerical values contain the unexpected numerical values.
def unexpected_nums(table_name, df):

    if table_name == "trades":
        # Filter the rows with invalid cmd. (cmd can only be 0 = buy, 1 = sell) 
        invalid_cmd = df[~df["cmd"].isin([0,1])]
        if not invalid_cmd.empty:
            print(f"Invalid cmd are found in table {table_name}")
        
        # Filter the rows with negative values from column "digits","volume","contractsize". 
        negatve_num = df[
                (df['digits'] < 0) | (df['volume'] < 0) | (df['contractsize'] < 0)
            ]
        if not negatve_num.empty:
            print(f"Invalid(Negative) values are found in table {table_name}.")

        # Filter the rows that open_price can not coorresponding to the digits.
        # invalid_digit_price = df[~df.apply(is_correct_digit, axis=1)]
        # if not invalid_digit_price.empty:
        #     print(f"Invalid digits with open_price are found in table {table_name}.")

    elif table_name == "users":
        # Filter the rows with invalid enabled. (enabled can only be 0 or 1.) 
        invalid_enables = df[~df["enable"].isin([0,1])]
        if not invalid_enables.empty:
            print(f"Invalid enable are found in table {table_name}")

# Check whether trade data contain the unexpected dates.
def unexpected_dates(table_name,df):
    # Filter the rows with invalid date, which close time is eralier than open time.
    invalid_dates = df[df["open_time"] > df["close_time"]]
    if not invalid_dates.empty:
        print(f"Ivalid time for trading, which open_time is late than close_time in {table_name}")

    # Filter the rows with date which is after the current date (epoch). 
    # It is suggested that epoch should be like 9999-01-01.
    # If it is not epoch, for example 2025-08-01, then should be considered as a disputed data.
    current_time = pd.to_datetime(datetime.now())
    epoch_dates = df[df["close_time"] > current_time]
    if not epoch_dates.empty:
        print("These trade is still open.")

# Check whether login in trades are recorded in users table.
def cross_validate(trades_data, users_data):
    merged_table = trades_data.merge(users_data, on=['login_hash', 'server_hash'], how='left', indicator=True)
    invalid_ticket = merged_table[merged_table['_merge'] == 'left_only']
    if not invalid_ticket.empty:
        print("Trades table contains accounts which can not be found in the users table.")

def edge_cases(table_name,df):
    if table_name == "users":
        # Find IDs that have both enable = 1 and enable = 0
        login_with_both_enables = df[df.groupby(['login_hash', 'server_hash'])['enable'].transform(lambda x: set(x) == {0, 1})]
        if not login_with_both_enables.empty:
            print("User table contains accounts with both eanble =1 and =0 in the same server.")

# Check wether the open price has valid digits with value in "digit" column.   
# Currently not work. 
# def is_correct_digit(row):
#     price_str = (str(row['open_price']))
#     decimal_part = price_str.split(".")[1] if "." in price_str else ""
#     return len(decimal_part) >= row['digits']

if __name__ == "__main__":

    db_connection_str = f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{RDS_URL}:{PORT}/{DB_NAME}'

    try:
        # Create a database connection using SQLAlchemy
        db_engine = create_engine(db_connection_str)

        database = {}
        # Fetch users data
        users_query = "SELECT * FROM users"
        database["users"] = pd.read_sql(users_query, db_engine)
        print("Users data fetched successfully.")

        # Fetch trades data
        trades_query = "SELECT * FROM trades"
        database["trades"] = pd.read_sql(trades_query, db_engine)
        print("Trades data fetched successfully.")

        for table_name, df in database.items():
            # Check whether contains the null value.
            null_values = df[df.isnull().any(axis=1)]
            if not null_values.empty:
                print(f"The {table_name} table contains null value.")

            # Check whether contains duplicate data.
            duplicates = df[df.duplicated()]
            if not duplicates.empty:
                print(f"The {table_name} table contains duplicated data.")

            # Check unexpected strings.
            unexpected_strings(table_name, df)
            # Check unexpected numeric values.
            unexpected_nums(table_name, df)
            # Check unexpected dates.
            if table_name == "trades":
                unexpected_dates(table_name, df)
            # Test any edge cases that I think should be investigated.
            edge_cases(table_name, df)
        
        # Check that any joins I need to ensure data integrity. 
        cross_validate(database["trades"], database["users"])

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Dispose of the engine to close the connection
        db_engine.dispose()


'''
Conclusion:

User table:
    During the assessment of the user table, it was discovered that the dataset contains 334 duplicate entries.
    This redundancy can lead to data integrity issues and may skew analytical results.

    Recommendation:
        Remove Duplicates: It is advisable to identify and remove these duplicate records to ensure a cleaner and more reliable dataset. 
        This will enhance the accuracy of user-related analyses and reporting.

Trades table:
    The examination of the trades table revealed several data quality issues:
       -Null Values: There are rows with null values present, particularly in the contractsize and volume fields. 
                    Further investigation showed that these null values are associated with the financial instrument "COFFEE," which is currently in dispute with a leading forex and CFD trading company.
                    This discrepancy should be addressed to maintain the integrity of trade data.

        -Missing User Accounts: Additionally, the trades table includes entries for accounts that cannot be found in the user table. 
                            This issue likely arises from incomplete user information being provided in the users dataset. 

        -Digits and Price Discrepancies: A significant problem was identified concerning the digits and open_price columns.
                                        The digits column is intended to represent the number of significant digits after the decimal point for the corresponding price.
                                        However, upon examination of the open_price values, it was noted that some prices have fewer decimal places than indicated by the digits. 
                                        For instance, the open_price is recorded as "0.7175", but the digits indicate "5". This discrepancy can lead to inaccuracies in pricing and calculations.


    Recommendations:
        Address Null Values: Investigate the source of null values in the trades table and determine if valid data can be obtained or if these entries should be removed.
        Ensure User Account Integrity: Implement validation checks to ensure that all trades are linked to valid and complete user records in the user table.
        Correct Digits and Price Discrepancies: Perform a thorough review of the digits and open_price columns to ensure consistency and correctness. Correct any discrepancies where the number of digits does not match the actual format of the price.

        
Challenges:
    In my implementation, the is_correct_digit() function exhibits limitations in accurately validating the relationship between open_price and digits.
    Specifically, it fails to handle scenarios where the open_price is represented as "0.1000" while the digits indicate "2". 
    In this case, the representation is valid because the open_price effectively rounds to 2 significant digits.

    The primary issue arises during the conversion of the float to a string. The function does not preserve trailing zeros, resulting in "0.1000" being formatted as "0.1". 
    This alteration leads to an invalid comparison since the necessary trailing zeros are lost, making it impossible to accurately assess if the digits match the formatted price.
'''