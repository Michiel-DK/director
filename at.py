import os
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

# Third-party libraries
import requests
from pyairtable import Api as AirtableApi
from notion_client import Client as NotionClient
# Note: For Moloni, we'll use their REST API via requests since there's no official Python client

# For task orchestration
from prefect import task, flow
from prefect.tasks import task_input_hash
from prefect_dask import DaskTaskRunner

from params import *

# Environment variables for API keys
#AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
NOTION_API_KEY = os.environ.get("NOTION_SECRET")
MOLONI_CLIENT_ID = os.environ.get("MOLONI_CLIENT_ID")
MOLONI_CLIENT_SECRET = os.environ.get("MOLONI_CLIENT_SECRET")

# Initialize clients
airtable = AirtableApi(AIRTABLE_API_KEY)
notion = NotionClient(auth=NOTION_API_KEY)

# Moloni authentication
def get_moloni_access_token() -> str:
    """Get Moloni access token using client credentials flow"""
    url = "https://api.moloni.com/v1/grant"
    data = {
        "grant_type": "client_credentials",
        "client_id": MOLONI_CLIENT_ID,
        "client_secret": MOLONI_CLIENT_SECRET
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json().get("access_token")

# Airtable functions
@task(cache_key_fn=task_input_hash, cache_expiration=datetime.timedelta(minutes=10))
def get_airtable_records(base_id: str, table_name: str, modified_since: Optional[str] = None) -> List[Dict]:
    """Get records from Airtable that were modified since the given timestamp"""
    table = airtable.table(base_id, table_name)
    
    formula = None
    if modified_since:
        # Formula to filter by last modified time
        formula = f"LAST_MODIFIED_TIME() > '{modified_since}'"
    
    return table.all(formula=formula)

@task
def track_airtable_sync_time() -> str:
    """Record the current time for future syncs"""
    return datetime.utcnow().isoformat()

# Notion functions
@task
def update_notion_database(notion_database_id: str, airtable_records: List[Dict]) -> List[Dict]:
    """Update Notion database with data from Airtable"""
    updated_pages = []
    
    for record in airtable_records:
        # Map Airtable fields to Notion properties
        # This mapping will need to be customized for your specific schema
        properties = map_airtable_to_notion(record["fields"])
        
        # Check if page already exists in Notion (based on some unique identifier)
        existing_page = find_notion_page(notion_database_id, record["id"])
        
        if existing_page:
            # Update existing page
            updated_page = notion.pages.update(
                page_id=existing_page["id"],
                properties=properties
            )
        else:
            # Create new page
            updated_page = notion.pages.create(
                parent={"database_id": notion_database_id},
                properties=properties
            )
        
        updated_pages.append(updated_page)
    
    return updated_pages

def map_airtable_to_notion(airtable_fields: Dict) -> Dict:
    """Map Airtable fields to Notion properties format"""
    # This is a simplified example - you'll need to customize based on your schema
    notion_properties = {}
    
    # Example mapping for common property types
    if "Name" in airtable_fields:
        notion_properties["Name"] = {"title": [{"text": {"content": airtable_fields["Name"]}}]}
    
    if "Description" in airtable_fields:
        notion_properties["Description"] = {"rich_text": [{"text": {"content": airtable_fields["Description"]}}]}
    
    if "Status" in airtable_fields:
        notion_properties["Status"] = {"select": {"name": airtable_fields["Status"]}}
    
    if "Price" in airtable_fields:
        notion_properties["Price"] = {"number": airtable_fields["Price"]}
    
    # Add more mappings as needed
    
    return notion_properties

def find_notion_page(database_id: str, airtable_id: str) -> Optional[Dict]:
    """Find a Notion page that corresponds to the given Airtable record ID"""
    # You'll need to store the Airtable ID in a property in Notion
    filter_params = {
        "filter": {
            "property": "AirtableID",
            "rich_text": {
                "equals": airtable_id
            }
        }
    }
    
    results = notion.databases.query(database_id=database_id, **filter_params)
    
    if results["results"]:
        return results["results"][0]
    
    return None

@task
def get_updated_notion_pages(notion_database_id: str, last_sync_time: str) -> List[Dict]:
    """Get Notion pages that were updated since the last sync time"""
    filter_params = {
        "filter": {
            "timestamp": "last_edited_time",
            "last_edited_time": {
                "after": last_sync_time
            }
        }
    }
    
    results = notion.databases.query(database_id=notion_database_id, **filter_params)
    return results["results"]

@task
def track_notion_sync_time() -> str:
    """Record the current time for future syncs"""
    return datetime.utcnow().isoformat()

# Moloni functions
@task
def update_moloni_data(notion_pages: List[Dict], moloni_entity_type: str) -> List[Dict]:
    """Update Moloni with data from Notion pages"""
    token = get_moloni_access_token()
    updated_entities = []
    
    for page in notion_pages:
        # Map Notion properties to Moloni fields
        # This mapping will need to be customized for your specific schema
        moloni_data = map_notion_to_moloni(page["properties"], moloni_entity_type)
        
        # Check if entity already exists in Moloni (based on some unique identifier)
        existing_entity = find_moloni_entity(token, moloni_entity_type, page["id"])
        
        if existing_entity:
            # Update existing entity
            updated_entity = update_moloni_entity(token, moloni_entity_type, existing_entity["id"], moloni_data)
        else:
            # Create new entity
            updated_entity = create_moloni_entity(token, moloni_entity_type, moloni_data)
        
        updated_entities.append(updated_entity)
    
    return updated_entities

def map_notion_to_moloni(notion_properties: Dict, entity_type: str) -> Dict:
    """Map Notion properties to Moloni fields format"""
    # This is a simplified example - you'll need to customize based on your schema and entity type
    moloni_data = {}
    
    # Example mapping for a product
    if entity_type == "products":
        if "Name" in notion_properties:
            moloni_data["name"] = notion_properties["Name"]["title"][0]["text"]["content"]
        
        if "Description" in notion_properties:
            moloni_data["description"] = notion_properties["Description"]["rich_text"][0]["text"]["content"]
        
        if "Price" in notion_properties:
            moloni_data["price"] = notion_properties["Price"]["number"]
    
    # Add mappings for other entity types as needed
    
    return moloni_data

def find_moloni_entity(token: str, entity_type: str, notion_id: str) -> Optional[Dict]:
    """Find a Moloni entity that corresponds to the given Notion page ID"""
    # You'll need to store the Notion ID in a custom field in Moloni
    # This is a simplified example - actual implementation will depend on Moloni API
    url = f"https://api.moloni.com/v1/{entity_type}/getAll"
    headers = {"Authorization": f"Bearer {token}"}
    
    # Search parameters will depend on how you store the reference
    data = {
        "where": [
            {"field": "reference", "operator": "=", "value": f"notion-{notion_id}"}
        ]
    }
    
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    
    results = response.json()
    if results:
        return results[0]
    
    return None

def update_moloni_entity(token: str, entity_type: str, entity_id: int, data: Dict) -> Dict:
    """Update an existing entity in Moloni"""
    url = f"https://api.moloni.com/v1/{entity_type}/update"
    headers = {"Authorization": f"Bearer {token}"}
    
    # Add the entity ID to the request
    data["id"] = entity_id
    
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    
    return response.json()

def create_moloni_entity(token: str, entity_type: str, data: Dict) -> Dict:
    """Create a new entity in Moloni"""
    url = f"https://api.moloni.com/v1/{entity_type}/insert"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    
    return response.json()

# Main flow
@flow(task_runner=DaskTaskRunner())
def sync_data_flow(
    airtable_base_id: str,
    airtable_table_name: str,
    notion_database_id: str,
    moloni_entity_type: str,
    last_airtable_sync: Optional[str] = None,
    last_notion_sync: Optional[str] = None
):
    """Main data sync flow from Airtable to Notion to Moloni"""
    # 1. Sync from Airtable to Notion
    airtable_records = get_airtable_records(airtable_base_id, airtable_table_name, last_airtable_sync)
    new_airtable_sync_time = track_airtable_sync_time()
    
    if airtable_records:
        updated_notion_pages = update_notion_database(notion_database_id, airtable_records)
        print(f"Updated {len(updated_notion_pages)} pages in Notion from Airtable")
    
    # 2. Sync from Notion to Moloni
    notion_pages = get_updated_notion_pages(notion_database_id, last_notion_sync or "1970-01-01T00:00:00Z")
    new_notion_sync_time = track_notion_sync_time()
    
    if notion_pages:
        updated_moloni_entities = update_moloni_data(notion_pages, moloni_entity_type)
        print(f"Updated {len(updated_moloni_entities)} entities in Moloni from Notion")
    
    return {
        "last_airtable_sync": new_airtable_sync_time,
        "last_notion_sync": new_notion_sync_time,
        "airtable_records_processed": len(airtable_records) if airtable_records else 0,
        "notion_pages_processed": len(notion_pages) if notion_pages else 0
    }

# Example usage
if __name__ == "__main__":
    # Configuration
    config = {
        "airtable_base_id": "app12345678",
        "airtable_table_name": "Products",
        "notion_database_id": "12345678-1234-1234-1234-1234567890ab",
        "moloni_entity_type": "products",
        # Get last sync times from storage (e.g., file, database)
        "last_airtable_sync": None,  # First run
        "last_notion_sync": None     # First run
    }
    
    # Run the flow
    result = sync_data_flow(**config)
    print(f"Sync completed: {result}")
    
    # Store the new sync times for the next run
    # (In a real implementation, you'd save these to a file or database)
    print(f"New Airtable sync time: {result['last_airtable_sync']}")
    print(f"New Notion sync time: {result['last_notion_sync']}")