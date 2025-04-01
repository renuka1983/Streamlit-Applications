import streamlit as st
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
from dotenv import load_dotenv
import time
import os
import uuid

# Connect to Elasticsearch with authentication
load_dotenv()


#ES_HOST = os.environ['ES_HOST'] # Update with actual host
#ES_USER = os.environ['ES_USER'] # Update with actual username
#ES_PASSWORD = os.environ['ES_PASSWORD']  # Update with actual password

ES_HOST = 'https://sqml-dhmp.es.us-west-2.aws.found.io:9243'
ES_USER = 'elastic'
ES_PASSWORD = 'f7VU4FcOvsnrYr4poj9H4LG0'

es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USER, ES_PASSWORD), timeout=60, max_retries=3, retry_on_timeout=True
)

# Valid Elasticsearch data types
VALID_ES_TYPES = {"text", "keyword", "integer", "long", "float", "double", "boolean", "date"}
actions = []
failed_rows = []
# Streamlit UI
st.title("CSV to Elasticsearch Uploader")

# File uploads
schema_file = st.file_uploader("Upload Schema CSV file", type=["csv"], key="schema")
data_file = st.file_uploader("Upload Data CSV file", type=["csv"], key="data")

if schema_file and data_file:
    schema_df = pd.read_csv(schema_file)
    data_df = pd.read_csv(data_file)
    
    st.write("### Preview of Schema File")
    st.dataframe(schema_df, height=400)
    
    st.write("### Preview of Data File")
    st.dataframe(data_df.head(), height=400)
    
    # Extract column names, data types, and formats from schema file
    schema = {
        row["column name"].strip(): {"type": row["data type"].strip()}
        if pd.isna(row["format"]) else {"type": row["data type"].strip(), "format": row["format"].strip()}
        for _, row in schema_df.iterrows()
    }
    
    st.write("### Extracted Schema")
    st.json(schema, expanded=True)
    
    # Convert date columns to epoch format
    epoch_columns = {}
    for col, details in schema.items():
        if details["type"] == "date" and col in data_df:
            # Attempt to infer date format
            detected_format = None
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    pd.to_datetime(data_df[col], format=fmt, errors='raise')
                    detected_format = fmt
                    break
                except Exception:
                    continue
            
            if detected_format:
                new_col = f"{col}_asEpoch"
                try:
                    data_df[new_col] = pd.to_datetime(data_df[col], format=detected_format, errors='coerce')
                    data_df[new_col] = data_df[new_col].apply(lambda x: int(x.timestamp()) if pd.notnull(x) else None)
                    epoch_columns[new_col] = {"type": "date", "format": "epoch_second"}
                except Exception as e:
                    st.error(f"Error converting column {col} to epoch: {e}")
    
    # Update schema after iteration
    schema.update(epoch_columns)
    
    # Update schema DataFrame with new epoch columns
    epoch_df = pd.DataFrame([{ "column name": new_col, "data type": details["type"], "format": details.get("format") } for new_col, details in epoch_columns.items()])
    schema_df = pd.concat([schema_df, epoch_df], ignore_index=True)
    
    st.write("### Updated Data Schema")
    st.dataframe(schema_df, height=400)
    
    st.write("### Updated Data File with Epoch Dates")
    st.dataframe(data_df.head(), height=400)
    
    # User input for index name and _id column
    index_name = st.text_input("Enter Elasticsearch Index Name", "csv_index")
    id_column_options = ["Generate Automatically"] + list(data_df.columns)
    id_column = st.selectbox("Select _id Column", id_column_options)
    
    # Check for duplicate or null _id values if user selects a column
    if id_column != "Generate Automatically":
        if data_df[id_column].isnull().any():
            st.error(f"Selected _id column '{id_column}' contains null values!")
        if data_df[id_column].duplicated().any():
            st.error(f"Selected _id column '{id_column}' contains duplicates!")
    
    # Allow user to edit schema mapping
    edited_schema = st.text_area("Edit Schema JSON if needed", json.dumps(schema, indent=4), height=400)
    try:
        schema = json.loads(edited_schema)
        # Validate the edited schema
        for col, details in schema.items():
            if details["type"] not in VALID_ES_TYPES:
                st.error(f"Invalid data type '{details['type']}' for column '{col}'. Valid types: {', '.join(VALID_ES_TYPES)}")
    except json.JSONDecodeError:
        st.error("Invalid JSON format in schema.")
    
    if st.button("Create Index in Elasticsearch"):
        # Define mappings, only include "format" if it exists
        mappings = {
            "mappings": {
                "properties": {
                    col: ({"type": schema[col]["type"], "format": schema[col]["format"]} if "format" in schema[col] else {"type": schema[col]["type"]})
                    for col in schema if schema[col]["type"]
                }
            }
        }
        
        # Create index
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
        es.indices.create(index=index_name, body=mappings)
        st.success(f"Index '{index_name}' created successfully!")
    
    if st.button("Upload Data to Elasticsearch"):
        #actions = []
        
        #failed_rows = []
        
        for _, row in data_df.iterrows():
            try:
                doc = row.dropna().to_dict()  # Remove NaN values
                
                # Convert float NaN to None explicitly
                for key, value in doc.items():
                    if isinstance(value, float) and pd.isna(value):
                        doc[key] = None
                
                action = {
                    "_index": index_name,
                    "_id": str(row[id_column]) if id_column != "Generate Automatically" else str(uuid.uuid4()),
                    "_source": doc
                }
                actions.append(action)
            except Exception as e:
                failed_rows.append({"row": row.to_dict(), "error": str(e)})
        
        try:
            success, failed = bulk(es, actions, raise_on_error=False)
            if failed:
                st.error(f"Some documents failed to index: {len(failed)}")
                for fail in failed[:5]:  # Show only first 5 errors for brevity
                    st.error(fail)
            else:
                st.success("Data uploaded successfully!")
        except Exception as e:
            st.error(f"Bulk indexing failed: {e}")

    # Display failed rows (if any)
    if failed_rows:
        st.write("### Failed Rows")
        st.json(failed_rows[:5])  # Show first 5 failed rows
