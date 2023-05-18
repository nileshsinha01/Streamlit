# Imports
import snowflake.connector
from snowflake.snowpark.session import Session
import streamlit as st
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
import numpy as np

## user credentials

user="nileshsinha"
password="Hummer$123",
account_identifier="yhrcdps-ie79268"
database_name="COVID19_EPIDEMIOLOGICAL_DATA"
schema_name="PUBLIC"

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

# Load Data from Snowflake
def load_data(query):
    ctx = snowflake.connector.connect(
        user='NILESHSINHA',
        password='Hummer$123',
        account='yhrcdps-ie79268',
        warehouse='COMPUTE_WH',
        database='COVID19_EPIDEMIOLOGICAL_DATA',
        schema='PUBLIC'
    )
    cs = ctx.cursor()
    cs.execute(query)
    results = cs.fetch_pandas_all()
    pd_df = pd.DataFrame(results)
    # while True:
    #     results = cs.fetchall()
    #     if not results:
    #         break
    #     pd_df = pd.DataFrame(results, columns=cs.description)
    print(pd_df)
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

    pd_df_analysis = load_data('SELECT m.COUNTRY_REGION, YEAR,QTR, m.CASES, m.DEATHS, m.DEATHS / m.CASES as CFR FROM (SELECT COUNTRY_REGION, DATE_PART(YEAR,DATE) as YEAR,DATE_PART(QUARTER,DATE) as QTR, AVG(CASES) AS CASES, AVG(DEATHS) AS DEATHS FROM ECDC_GLOBAL GROUP BY COUNTRY_REGION, YEAR,QTR) m WHERE m.CASES > 0')

    with st.container():
        st.subheader('Results')
        st.dataframe(pd_df_analysis)
        df_aggr = pd_df_analysis.groupby(['COUNTRY_REGION','YEAR','QTR'],as_index=False).sum()
        sel_country = st.selectbox('**Select country**', df_aggr.COUNTRY_REGION.unique())
        sel_Year = st.selectbox('**Select Year**', df_aggr.YEAR.unique())
        sel_QTR = st.selectbox('**Select QTR**', df_aggr.QTR.unique())
        fil_df = df_aggr[(df_aggr.COUNTRY_REGION == sel_country) & (df_aggr.YEAR == sel_Year) & (df_aggr.QTR == sel_QTR)]

        new_df = pd.melt(fil_df, id_vars=['COUNTRY_REGION'], var_name="feature",
                              value_vars=['CASES','DEATHS','CFR'])
        title = f'country name: {sel_country}'
        title = f'Year: {sel_Year}'
        title = f'QTR: {sel_QTR}'
        st.bar_chart(new_df, x ='feature' , y = 'value')














