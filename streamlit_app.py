
import snowflake.connector
import pyarrow
import pandas as pd
import requests
import streamlit as st
import streamlit as st
from llama_index import VectorStoreIndex, ServiceContext, Document
from llama_index.llms import OpenAI
import openai
from llama_index import SimpleDirectoryReader

def get_retail_db_query(question):
    tables = [
      {'table_name': '"RETAIL.BIG_SUPPLY_CO.PRODUCT"',
      'columns': 'PRODUCT_CARD_ID,PRODUCT_CATEGORY_ID,PRODUCT_DESCRIPTION,PRODUCT_IMAGE,PRODUCT_NAME,PRODUCT_PRICE,PRODUCE_STATUS.',
      'data_types': 'NUMBER(38,0),NUMBER(38,0),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),NUMBER(38,0),NUMBER(38,0).',
      'description': 'This is a table of all the products we sell at our retail company'},

      {'table_name': '"RETAIL.BIG_SUPPLY_CO.ORDERS"',
      'columns': 'ORDER_ID,ORDER_ITEM_CRADPROD_ID,ORDER_CUSTOMER_ID,ORDER_DEPARTMENT_ID,MARKET,ORDER_CITY,ORDER_COUNTRY,ORDER_REGION,ORDER_STATE,ORDER_STATUS,ORDER_ZIPCODE,ORDER_DATE,ORDER_ITEM_DISCOUNT,ORDER_ITEM_DISCOUNT_RATE,ORDER_ITEM_ID,ORDER_ITEM_QUANTITY,SALES,ORDER_ITEM_TOTAL,ORDER_PROFIT,TYPE,DAYS_FOR_SHIPPING,DAYS_FOR_SHIPPING_ESTIMATED,DELIVERY_STATUS,LATE_DELIVERY_RISK.',
      'data_types': 'NUMBER(38,0),NUMBER(38,0),NUMBER(38,0),NUMBER(38,0),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),NUMBER(38,0),VARCHAR(16777216),NUMBER(38,0),NUMBER(38,0),NUMBER(38,0),NUMBER(38,0),NUMBER(38,0),NUMBER(38,0),NUMBER(38,0),VARCHAR(16777216),NUMBER(38,0),NUMBER(38,0),VARCHAR(16777216),NUMBER(38,0).',
      'description': 'This is a table of all the orders we have recieved at our retail company'},

      {'table_name': '"RETAIL.BIG_SUPPLY_CO.DEPARTMENT"',
      'columns': 'DEPARTMENT_ID,DEPARTMENT_NAME,LATITUDE,LONGITUDE.',
      'data_types': 'NUMBER(38,0),VARCHAR(16777216),NUMBER(38,0),NUMBER(38,0).',
      'description': 'This is a table of all the departments in the retail company'},

      {'table_name': '"RETAIL.BIG_SUPPLY_CO.CUSTOMER"',
      'columns': 'CUSTOMER_ID,CUSTOMER_CITY,CUSTOMER_COUNTRY,CUSTOMER_EMAIL,CUSTOMER_FIRST_NAME,CUSTOMER_LAST_NAME,CUSTOMER_PASSWORD,CUSTOMER_SEGMENT,CUSTOMER_STATE,CUSTOMER_STREET,CUSTOMER_ZIPCODE.',
      'data_types': 'NUMBER(38,0),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),VARCHAR(16777216),NUMBER(38,0).',
      'description': 'This is a table of all our customer data for people who have shopped at our retail company'},

      {'table_name': '"RETAIL.BIG_SUPPLY_CO.CATEGORIES"',
      'columns': 'CATEGORY_ID,CATEGORY_NAME.',
      'data_types': 'NUMBER(38,0),VARCHAR(16777216).',
      'description': 'This is a table of all the product categories we have for the products we sell at the retail company'},
    ]
    api_key = st.secrets.openai_key
    
    background_info = ""
    for table in tables:
        table_name = table['table_name']
        columns = table['columns']
        data_types = table['data_types']
        description = table['description']
        table_info = f'We have a table called {table_name} with the columns [{columns}] with the datatypes [{data_types}]. {description}.'
        background_info = f'{background_info} {table_info}'
        
    full_prompt = f'{background_info}. Using this information, please answer return a Snowflake SQL query that answers the following question: {question}'
    
    payload = {
      "model": "gpt-3.5-turbo",
      "messages": [
        {
          "role": "system",
          "content": "You will be given a prompt describing tables in a snowflake database and a question about the tables. Read the prompt and return a Snowflake SQL Query that answers the question."
        },
        {
          "role": "user",
          "content": full_prompt
        }
      ],
      "temperature": 0,
      "max_tokens": 256
        }
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.post(url = "https://api.openai.com/v1/chat/completions", json = payload ,headers = headers)
    query = response.json()['choices'][0]['message']['content'].replace("\n",' ')
    error_list = ['DELETE','UPDATE','CREATE','ALTER','GRANT','PERMISSION','INSERT','DROP','REPLACE','PASSWORD']
    if  any([x in query.upper() for x in error_list]):
        return "'Your query was malformed or your question could not be answered with the given data. Please try again with a new question'"
    else:
        return response.json()['choices'][0]['message']['content'].replace("\n",' ')
def get_query_df(question):
    try:
        r = get_retail_db_query(question)
        conn = snowflake.connector.connect(
            user='jacksonmakl0531',
            password= st.secrets.snowflake_db_pass,
            account='thzvyjm-clb79676',
        )
        cursor = conn.cursor()
        snowflake_response = cursor.execute(r)
        df = snowflake_response.fetch_pandas_all()
    except:
        df = pd.DataFrame([])
        r = 'Your query was malformed or your question could not be answered with the given data. Please try again with a new question'
    return df, r

openai.api_key = "********************************************"
st.header("Retail Explorer Chatbot 🛍️")
a = st.radio("Show SQL Query or Hide", ['Show', 'Hide'], 1)
if "messages" not in st.session_state.keys(): # Initialize the chat message history
    st.session_state.messages = [
        {"role": "assistant", "content": "Ask me a question about data in Big Supply Co's Retail Snowfalke data warehouse "}
    ]
@st.cache_resource(show_spinner=False)
def load_data():
    with st.spinner(text="Loading..."):
        reader = SimpleDirectoryReader(input_dir="./data", recursive=True)
        docs = reader.load_data()
        service_context = ServiceContext.from_defaults(llm=OpenAI(model="gpt-3.5-turbo", temperature=0.5, system_prompt="You are an expert on the Streamlit Python library and your job is to answer technical questions. Assume that all questions are related to the Streamlit Python library. Keep your answers technical and based on facts – do not hallucinate features."))
        index = VectorStoreIndex.from_documents(docs, service_context=service_context)
        return index

index = load_data()

chat_engine = index.as_chat_engine(chat_mode="condense_question", verbose=True)

if prompt := st.chat_input("Your question"): # Prompt for user input and save to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

for message in st.session_state.messages: # Display the prior chat messages
    with st.chat_message(message["role"]):
        st.write(message["content"])

# If last message is not from assistant, generate a new response
if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            response = get_query_df(prompt)
            desc_text = "Sure, here is what I found: "
            message = {"role": "assistant", "content": desc_text}
            st.session_state.messages.append(message) 
            st.write(desc_text)

            message = {"role": "assistant", "content": response[0]}
            st.session_state.messages.append(message) 
            st.write(response[0])

            
            if a == 'Show':
                message = {"role": "assistant", "content": response[1]}
                st.session_state.messages.append(message) 
                st.write(response[1])






            
