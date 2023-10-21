import pandas as pd
from elasticsearch import exceptions
from elasticsearch import Elasticsearch
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import string
import streamlit as st

nltk.download('wordnet')
nltk.download('punkt')
nltk.download('stopwords')

brand_category_df = pd.read_csv("data/brand_category.csv")
categories_df = pd.read_csv("data/categories.csv")
offer_retailer_df = pd.read_csv("data/offer_retailer.csv")

brand_category_df.dropna(inplace=True)
brand_category_df.reset_index(drop=True, inplace=True)

offer_retailer_df.fillna("No Retailer Information", inplace=True)

brand_belongs_to_category = brand_category_df['BRAND_BELONGS_TO_CATEGORY'].unique()
prod_category = categories_df['PRODUCT_CATEGORY'].unique()

brand_category_df.rename(columns={'BRAND_BELONGS_TO_CATEGORY': "CATEGORY"}, inplace=True)
categories_df.rename(columns={'PRODUCT_CATEGORY': "CATEGORY"}, inplace=True)

brand_categories_df = brand_category_df.merge(categories_df, on = 'CATEGORY', how = 'left')

es = Elasticsearch(
  "https://d4e658413dda45e0abf84a436aa46b0d.us-central1.gcp.cloud.es.io:443",
  api_key="b1AxN1VJc0JrT3VYSkxwa3RURE06dk5JRS1hR2JUY2ViVFlBc1kySlppUQ=="
)

mappings = {
        "properties":
        {
            "BRAND": {"type": "text", "analyzer": "standard"},
            "CATEGORY": {"type": "text", "analyzer": "english"},
            "IS_CHILD_CATEGORY_TO": {"type": "text", "analyzer": "english"}
        }
    }

try:
    es.indices.create(index="search-products", mappings=mappings)
    for i, row in brand_categories_df.iterrows():
        doc = {
            "BRAND": row["BRAND"],
            "CATEGORY": row["CATEGORY"],
            "IS_CHILD_CATEGORY_TO": row["IS_CHILD_CATEGORY_TO"],
        }

        es.index(index="search-products", id=i, document=doc)

    es.indices.refresh(index="search-products")
    es.cat.count(index="search-products", format="json")
    
except exceptions.RequestError as ex:
    if ex.error == 'resource_already_exists_exception':
        pass
    else:
        raise ex

mappings = {
    "properties":
    {
        "OFFER": {"type": "text", "analyzer": "english"},
        "RETAILER": {"type": "text", "analyzer": "standard"},
        "BRAND": {"type": "text", "analyzer": "standard"}
    }
}

try:
    es.indices.create(index="search-offers", mappings=mappings)
    for i, row in offer_retailer_df.iterrows():
        doc = {
            "OFFER": row["OFFER"],
            "RETAILER": row["RETAILER"],
            "BRAND": row["BRAND"]
        }

        es.index(index="search-offers", id=i, document=doc)

    es.indices.refresh(index="search-offers")
    es.cat.count(index="search-offers", format="json")
    
except exceptions.RequestError as ex:
    if ex.error == 'resource_already_exists_exception':
        pass
    else:
        raise ex

st.title("Fetch Offers")
search_query = st.text_input("Search Offers by Category, Brand or Retailer")

def preprocess_query_nltk(query):
    words = word_tokenize(query)
    words = [word for word in words if word not in string.punctuation]
    
    lemmatizer = WordNetLemmatizer()
    words = [lemmatizer.lemmatize(word) for word in words]
    
    processed_query = ' '.join(words)
    
    return processed_query

search_query_best = preprocess_query_nltk(search_query)
words = search_query_best.split()
search_query_prefix = '* '.join(words) + "*"

resp = es.search(
    index="search-offers",
    query={
            "bool": 
            {
              "should": 
              [
                  {
                    "multi_match": 
                      {
                        "query": search_query_best,
                        "fields": ["OFFER", "RETAILER", "BRAND"],
                        "type": "best_fields"
                      }
                  },
                  {
                    "multi_match": 
                      {
                        "query": search_query_prefix,
                        "fields": ["OFFER", "RETAILER", "BRAND"],
                        "type": "phrase_prefix"
                      }
                  }
              ]
            }
          }          
    )

valid_entries = []
for entry in resp['hits']['hits']:
    brand = entry['_source']['BRAND']
    resp1 = es.search(
        index="search-products",
        query={
                "bool": 
                {
                    "must": 
                    {
                        "match_phrase": 
                        {
                            "BRAND": brand
                        }
                    }
                }
            }            
    )
    if resp1['hits']['total']['value'] > 0:
        valid_entries.append(entry)

if len(valid_entries) == 0:
    st.write("Oops, No Offers Found!")
  
for i, entry in enumerate(valid_entries):
    st.write(i+1, entry["_source"]["OFFER"])
    st.write("Available at", entry["_source"]["RETAILER"])
    st.write("From ", entry["_source"]["BRAND"])
    st.write("With similarity score of ", entry["_score"])
