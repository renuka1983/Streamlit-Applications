import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd
from io import BytesIO
import os
import time
from dotenv import load_dotenv



# Connect to Elasticsearch with authentication
load_dotenv()


ES_HOST = os.environ['ES_HOST'] # Update with actual host
ES_USER = os.environ['ES_USER'] # Update with actual username
ES_PASSWORD = os.environ['ES_PASSWORD']  # Update with actual password

es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USER, ES_PASSWORD)
)

# Function to list indices based on keyword
def list_indices(keyword):
    indices = es.cat.indices(format="json")
    return [idx['index'] for idx in indices if keyword.lower() in idx['index'].lower()]

# Function to get column names from Elasticsearch
def get_columns(index):
    res = es.search(index=index, body={"size": 1})
    if res['hits']['hits']:
        return list(res['hits']['hits'][0]['_source'].keys())
    return []

# Function to get unique values of a column
def get_unique_values(index, column):
    query = {
        "size": 0,
        "aggs": {
            "unique_values": {
                "terms": {"field": column, "size": 100}
            }
        }
    }
    res = es.search(index=index, body=query)
    return [bucket['key'] for bucket in res['aggregations']['unique_values']['buckets']]

# Function to extract data from Elasticsearch
def extract_data(index, date_field, start_date, end_date, filter_field, filter_values, selected_columns):
    start_epoch = int(time.mktime(start_date.timetuple()) * 1000)
    end_epoch = int(time.mktime(end_date.timetuple()) * 1000)
    
    must_conditions = [{"range": {date_field: {"gte": start_epoch, "lte": end_epoch}}}]
    
    if filter_values:
        must_conditions.append({"terms": {filter_field: filter_values}})
    
    query = {"query": {"bool": {"must": must_conditions}}}
    
    res = es.search(index=index, body=query, size=10000)  # Fetch up to 10k results
    data = []
    
    for doc in res['hits']['hits']:
        source = doc['_source']
        for key, value in source.items():
            if value is None:
                source[key] = "NULL"  # Replace missing values with NULL
            elif isinstance(value, list):
                source[key] = ', '.join(map(str, value))  # Convert lists to comma-separated strings
        data.append(source)
    
    df = pd.DataFrame(data)
    if selected_columns and "Select All" not in selected_columns:
        df = df[selected_columns]  # Keep only selected columns
    return df

# Streamlit UI
st.title("Elasticsearch Data Extractor")

# User inputs
keyword = st.text_input("Enter keyword to search indices", key="keyword_input")
if keyword:
    indices = list_indices(keyword)
    selected_index = st.selectbox("Select an index", indices, key="index_select") if indices else None
    
    if selected_index:
        columns = get_columns(selected_index)
        
        if columns:
            date_field = st.selectbox("Select the date field", columns, key="date_field")
            start_date = st.date_input("Start Date", key="start_date")
            end_date = st.date_input("End Date", key="end_date")
            filter_field = st.selectbox("Select a column to filter", columns, key="filter_field")
            
            # Add 'Select All' option for columns
            column_options = ["Select All"] + columns
            selected_columns = st.multiselect("Select columns to download", column_options, default=["Select All"], key="selected_columns")
            
            if filter_field:
                unique_values = get_unique_values(selected_index, filter_field)
                filter_values = st.multiselect("Select values for filtering", unique_values, key="filter_values")
                
                if st.button("Extract Data", key="extract_button"):
                    df = extract_data(selected_index, date_field, start_date, end_date, filter_field, filter_values, selected_columns)
                    if not df.empty:
                        st.write(df)
                        csv = df.to_csv(index=False).encode("utf-8")
                        
                        # Let user choose download location
                        
                        download_path = st.text_input("Enter file path to save CSV", "./downloaded_data.csv", key="download_path")
                        if st.button("Save CSV Locally", key="save_csv"):
                            try:
                                df.to_csv(download_path, index=False)
                                st.success(f"File saved at {download_path}")
                            except Exception as e:
                                st.error(f"Error saving file: {e}")
                        
                        st.download_button("Download CSV", data=csv, file_name=f"{selected_index}_data.csv", mime="text/csv", key="download_csv")
                    else:
                        st.error("No data found for the given filters.")
        else:
            st.error("No columns found for the selected index.")
