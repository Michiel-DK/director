from pyairtable import Api
from params import *
import requests
from pyairtable.formulas import match, Formula
from datetime import datetime, timedelta



def connect_table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRABLE_TABLE_NAME):
    api = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, AIRABLE_TABLE_NAME)
    
    records = table.all()
    print(f"Found {len(records)} records")

    return table
    

def update_records(table, k=5):

    for i in range(k):
        # Create a test record
        new_record = table.create({
            "Name": f"Test Product_{i}",
            "Description": f"This is a product {i} created via API",
            "Price": i+10,
            "Status": "In progress",
        })
        
        print(f"Created new record with ID: {new_record['id']}")
        
def get_updated_records_since(table, since_datetime: str, modified_field="Last Modified"):
    """
    Fetch records updated since a given datetime.
    
    Parameters:
        table: Airtable table object from pyairtable
        since_datetime (str): ISO8601 datetime string, e.g. '2024-03-21T00:00:00.000Z'
        modified_field (str): Field name that tracks last modified datetime
    """
    formula = Formula(f"IS_AFTER({{{modified_field}}}, '{since_datetime}')")
    
    updated_records = table.all(formula=formula)
    
    print(f" Found {len(updated_records)} records updated since {since_datetime}")
    
    return updated_records


def get_recently_updated_records(table, modified_field="Last Modified"):
    """
    Fetch records updated in the last hour based on a 'Last Modified' datetime field.
    
    Parameters:
        table: Airtable table object from pyairtable
        modified_field (str): Name of the field storing last modified datetime
    """
    one_hour_ago = (datetime.utcnow() - timedelta(hours=1)).isoformat() + "Z"
    formula = Formula(f"IS_AFTER({{{modified_field}}}, '{one_hour_ago}')")
    
    updated_records = table.all(formula=formula)
    
    print(f"Found {len(updated_records)} records updated in the last hour.")
    return updated_records


if __name__=='__main__':
    
    table = connect_table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    
    update_records(table)
    
    recent_updates = get_recently_updated_records(table)
    
    print(recent_updates)
    
    #check_tables()
    
    
    

