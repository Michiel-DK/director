from prefect import task, flow
import time

from workflow_director.airtable.records import get_recently_updated_records, connect_table, update_records
from workflow_director.notion.records import update_notion_table

from params import *

@task
def create_records():
    table = connect_table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    return update_records(table=table)

@task
def get_airtable_records():
    table = connect_table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    records = get_recently_updated_records(table)
    return records

@task
def update_notion(records):
    return update_notion_table(records)

@flow
def update_flow(name='integration_test'):
    created_records = create_records.submit()
    created_records = created_records.result()
    airtable_records = get_airtable_records.submit(wait_for=[created_records])
    airtable_records = airtable_records.result()
    update_notion.submit(records=airtable_records, wait_for=[airtable_records])

if __name__ == "__main__":
    update_flow()
