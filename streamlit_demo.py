# Imports
from snowflake.snowpark.session import Session
import streamlit as st
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
import numpy as np

# Set Streamlit Page
st.set_page_config(
    page_title="Streamlit App Demo ",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://developers.snowflake.com',
        'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace"
    }
)


# Functions

# Create Session object
def create_session_object():
    connection_parameters = {
        "account": "yhrcdps-ie79268",
        "user": "NILESHSINHA",
        "password": "Hummer$123",
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "COVID19_EPIDEMIOLOGICAL_DATA",
        "schema": "PUBLIC"
    }
    session = Session.builder.configs(connection_parameters).create()
    print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
    return session


# Load Data from Snowflake
def load_data(session, query):
    snow_df = session.sql(query)
    pd_df = snow_df.to_pandas()
    return pd_df


# Name Tabs
tab1, tab2 = st.tabs(
    ["1. Main Page", "2. Analytics Reults"])

with tab1:
    # Add header and a subheader
    st.header("Streamlit-Snowflake Demo")
    st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")

    st.subheader("Covid 19 Date-dependent case fatality ratio by Country")


with tab2:
    # Add header and a subheader
    st.header("Calculate case-fatality ratio for a given date")

    session = create_session_object()

    pd_df_analysis = load_data(session, 'SELECT m.COUNTRY_REGION, m.DATE,DATE_PART(YEAR,m.DATE) as YEAR,DATE_PART(QUARTER,m.DATE) as QTR, m.CASES, m.DEATHS, m.DEATHS / m.CASES as CFR FROM (SELECT COUNTRY_REGION, DATE, AVG(CASES) AS CASES, AVG(DEATHS) AS DEATHS FROM ECDC_GLOBAL GROUP BY COUNTRY_REGION, DATE) m WHERE m.CASES > 0')

    with st.container():
        st.subheader('Results')
        st.dataframe(pd_df_analysis)
        df_aggr = pd_df_analysis.groupby(['COUNTRY_REGION','YEAR','QTR'],as_index=False).sum()
        sel_country = st.selectbox('**Select country**', df_aggr.COUNTRY_REGION)
        sel_Year = st.selectbox('**Select Year**', df_aggr.YEAR.unique())
        sel_QTR = st.selectbox('**Select QTR**', df_aggr.QTR.unique())
        fil_df = df_aggr[(df_aggr.COUNTRY_REGION == sel_country) & (df_aggr.YEAR == sel_Year) & (df_aggr.QTR == sel_QTR)]

        new_df = pd.melt(fil_df, id_vars=['COUNTRY_REGION'], var_name="feature",
                              value_vars=['CASES','DEATHS','CFR'])
        title = f'country name: {sel_country}'
        title = f'Year: {sel_Year}'
        title = f'QTR: {sel_QTR}'
        st.bar_chart(new_df, x ='feature' , y = 'value')














