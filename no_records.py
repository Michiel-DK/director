from params import *
from notion_client import Client as NotionClient
import os
from at_records import get_recently_updated_records, connect_table


notion = NotionClient(auth=NOTION_API_KEY)

#import ipdb;ipdb.set_trace()


def create_or_update_notion_page(record):
    """Push a single Airtable record to Notion"""
    fields = record["fields"]
    name = fields.get("Name", "Unnamed")
    description = fields.get("Description", "")
    price = fields.get("Price", 0)
   # status = fields.get("Status", "Unknown")

    notion.pages.create(
        parent={"database_id": NOTION_DATABASE_ID},
        properties={
            "Name": {"title": [{"text": {"content": name}}]},
            "Description": {"rich_text": [{"text": {"content": description}}]},
            "Price": {"number": price},
         #   "Status": {"select": {"name": status}},
        }
    )
    print(f"✅ Synced record '{name}' to Notion.")
    
if __name__=='__main__':
    table = connect_table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    records = get_recently_updated_records(table)
    
    for record in records:
        create_or_update_notion_page(record)