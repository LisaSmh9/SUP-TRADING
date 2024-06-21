import requests
import pandas as pd
from sqlalchemy import create_engine
import io

def fetch_intraday_data():
    url = "https://www.boursedirect.fr/api/instrument/download/intraday/XPAR/PX1/EUR"
    response = requests.get(url)
    
    # Convert the response content to a StringIO object
    data = pd.read_csv(io.StringIO(response.text))
    
    # Save to Excel
    excel_path = "intraday_data.xlsx"
    data.to_excel(excel_path, index=False)
    
    return excel_path

def save_to_database(excel_path):
    # Create SQLAlchemy engine to connect to the database
    engine = create_engine('sqlite:///trading_data.db')  # Example using SQLite, change to your database
    
    # Read the data from Excel
    data = pd.read_excel(excel_path)
    
    # Save to database
    data.to_sql('intraday_data', engine, if_exists='append', index=False)

if __name__ == "__main__":
    excel_path = fetch_intraday_data()
    save_to_database(excel_path)
