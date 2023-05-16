# Imports
from snowflake.snowpark.session import Session
import streamlit as st
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy.dialects import registry
import pandas as pd
import numpy as np
import datetime
from io import StringIO
from PIL import Image
import openpyxl
import datetime
from st_aggrid import GridOptionsBuilder, AgGrid
from st_aggrid.shared import GridUpdateMode, DataReturnMode

# Set Streamlit Page
st.set_page_config(
     page_title="KPMG Tax Snowflake Demo ",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace"
     }
)

#Functions

# Create Session object
def create_session_object():
   connection_parameters = {
      "account": "KPMGUSADVISORY-KPMG_US_TAX_NONPRD_SANDBOX",
      "user": "DLACHARITE",
      "password": "r!p5Vnet",
      "role": "ANALYST_ROLE",
      "warehouse": "COMPUTE_WH",
      "database": "DEMO_CURATE_TEST",
      "schema": "DATA_MART"
   }
   session = Session.builder.configs(connection_parameters).create()
   print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
   return session

# Load Snowflake Table
def load_table(session, tableName):
    # Forex Table
    df_table = session.table(tableName)

    # Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
    pd_df_table = df_table.to_pandas()

    return pd_df_table

#Load Data from Snowflake
def load_data(session, snowflake_table):
    snow_df = session.table(snowflake_table)
    pd_df = snow_df.to_pandas().set_index('ENTITYNAME')
    return pd_df

# Load FOREX Data
def load_forex_data(session, forex_table):
    # Forex Table
    snow_df_forex = session.table(forex_table)

    # Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
    pd_df_forex = snow_df_forex.to_pandas()

    return pd_df_forex

# Merge General Ledger with Forex table(Master Data) and calculate local currencies.
def calculate_local_currency(data, pd_df_forex):
    #merge tables
    merged_df = pd.merge(data, pd_df_forex, on="ENTITYNAME", how="inner")

    # Add Local Currency Calculation Column
    merged_df['LOCAL_CURRENCY'] = merged_df.ENDINGBALANCE * merged_df.EXCHANGE_RATE
    merged_df.reset_index(drop=True, inplace=True)
    return merged_df

def load_df_to_snowflake(df, snowflake_table_name):

     account_identifier = 'KPMGUSADVISORY-KPMG_US_TAX_NONPRD_SANDBOX'
     user = 'DLACHARITE'
     password = 'r!p5Vnet'
     database_name = 'DEMO_CURATE_TEST'
     schema_name = 'DATA_MART'
     warehouse_name = "COMPUTE_WH"
     role_name = "ANALYST_ROLE"

     registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

     conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}&role={role_name}"
     engine = create_engine(conn_string)
     connection = engine.connect()

     with engine.connect() as con:
         df.to_sql(name=snowflake_table_name, con=engine, if_exists='replace', index=False, method=pd_writer)


def data_frame_from_xlsx(xlsx_file, range_name):
    """ Get a single rectangular region from the specified file.
    range_name can be a standard Excel reference ('Sheet1!A2:B7') or
    refer to a named region ('my_cells')."""
    wb = openpyxl.load_workbook(xlsx_file, data_only=True, read_only=True)
    if '!' in range_name:
        # passed a worksheet!cell reference
        ws_name, reg = range_name.split('!')
        if ws_name.startswith("'") and ws_name.endswith("'"):
            # optionally strip single quotes around sheet name
            ws_name = ws_name[1:-1]
        region = wb[ws_name][reg]
    else:
        # passed a named range; find the cells in the workbook
        full_range = wb.get_named_range(range_name)
        if full_range is None:
            raise ValueError(
                'Range "{}" not found in workbook "{}".'.format(range_name, xlsx_file)
            )
        # convert to list (openpyxl 2.3 returns a list but 2.4+ returns a generator)
        destinations = list(full_range.destinations)
        if len(destinations) > 1:
            raise ValueError(
                'Range "{}" in workbook "{}" contains more than one region.'
                .format(range_name, xlsx_file)
            )
        ws, reg = destinations[0]
        # convert to worksheet object (openpyxl 2.3 returns a worksheet object
        # but 2.4+ returns the name of a worksheet)
        if isinstance(ws, str):
            ws = wb[ws]
        region = ws[reg]
    # an anonymous user suggested this to catch a single-cell range (untested):
    # if not isinstance(region, 'tuple'): df = pd.DataFrame(region.value)
    df = pd.DataFrame([cell.value for cell in row] for row in region)
    return df


# Name Tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(["1. Table of Contents", "2. Source Data", "3. Master Data", "4. Calculations", "5. Reporting", "6. Load Supporting File", "7. Review","Test"])

with tab1:
    # Add header and a subheader
    st.header("KPMG Streamlit-Snowflake Tax Demo")
    st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")
    
    username = st.text_input("Input Username:")
    
    st.subheader("Table of Contents")

    st.write("1. TOC")
    st.write("2. Source Data")
    st.write("3. Master Data")
    st.write("4. Calculations")
    st.write("5. Reporting")
    st.write("6. Supporting Files")

    st.subheader("Instructions")
    st.write("1. Master Data Tab - Upload Master Data file (.xlsx format)")
    st.write("2. Master Data Tab - Upload Current Foreign Exchange Rates Report (.csv format)")
    st.write("4. Master Data Tab - Select Date")
    st.write("5. Master Data Tab - Manually adjust Foreign Exchange Rate Master Data and make note in comments column")
    st.write("6. Master Data Tab - Load Modified FOREX Table to Snowflake Database  ")
    st.write("7. Calculations Tab - Select Entities for Local Currency Calculation")
    st.write("8. Calculations Tab - Select Date of Calculation")
    st.write("9. Calculationd Tab - Load Loacal Currency Calculations Table to Snowflake Database")
    st.write("10. Load Supporting File Tab - Load Supporting Files for viewing")

with tab2:

    # Add header and a subheader
    st.header("Source Data")
    
    session = create_session_object()

    # Create General Ledger that loads data from KPMG Sandbox Data: General Ledger
    pd_df_generalledger = load_data(session, 'DATA_MART.GENERALLEDGER')

    # Use columns to display the three dataframes side-by-side along with their headers
    with st.container():
        st.subheader('General Ledger')
        st.dataframe(pd_df_generalledger)

with tab3:

    st.header("Master Data")
    
    pd_df_generalledger_noidx = pd_df_generalledger.reset_index(drop=False)
    pd_df_generalledger_idx = pd_df_generalledger.set_index("ENTITYNUMBER")
    editable_forex_table = pd_df_forex = load_forex_data(session, 'DATA_MART.FOREX_RATES')


    st.subheader("Load Master Data")

    master_data = st.file_uploader("Choose Master Data File (.xlxs file)")

    if master_data is not None:
        df_entity_list = pd.read_excel(master_data, sheet_name="Entity")
        df_accounts = pd.read_excel(master_data, sheet_name="Accounts")

        col1,col2 = st.columns(2)
        
        with col1:
            with st.container():
                st.subheader("Entities")
                #st.dataframe(pd_df_generalledger_noidx[["ENTITYNUMBER", "ENTITYNAME"]])
                st.dataframe(df_entity_list)

        with col2:
            with st.container():
                st.subheader("Accounts")
                #st.dataframe(pd_df_generalledger_noidx[["ACCOUNTNUMBER","ACCOUNTDESCRIPTION"]])
                st.dataframe(df_accounts)

    with st.container():
        st.subheader("Foreign Exchange Rates")

        uploaded_csv = st.file_uploader("Import FOREX file (.csv format)")

        if uploaded_csv is not None:
            USD_JPY_exchange_rates = pd.read_csv(uploaded_csv)
            st.write(USD_JPY_exchange_rates)

        mod_date = st.date_input("Select the date of modifications")

        st.subheader("Foreign Exchange Rate Master Data")

        editable_forex_table["Comments"] = "N/A"
        edited_df = st.experimental_data_editor(editable_forex_table, num_rows="dynamic" )

    with st.form("write1"):
        submit_form = st.form_submit_button('Load Modified FOREX Table to Snowflake Database')

    modified_table_name = "FOREX_RATES_" + mod_date.strftime('%m.%d.%Y')

    if submit_form:
        load_df_to_snowflake(edited_df, modified_table_name)

with tab4:

    session = create_session_object()

    # Create General Ledger that loads data from KPMG Sandbox Data: General Ledger
    pd_df_generalledger = load_data(session, 'DATA_MART.GENERALLEDGER')

    entities = st.multiselect(
        "Select Entities for Local Currency Calculation", list(pd_df_generalledger.index.unique()))

    # Use columns to display the three dataframes side-by-side along with their headers
    with st.container():
        st.subheader('General Ledger')
        st.dataframe(pd_df_generalledger.loc[entities])
        pd_df_generalledger_filtered = pd_df_generalledger.loc[entities].reset_index()

    pd_df_forex = load_forex_data(session, 'DATA_MART.FOREX_RATES')

    with st.form("calculation"):
        calculate_local_currencies = st.form_submit_button("Calculate Local Currencies")

    pd_df_local_currency = calculate_local_currency(pd_df_generalledger_filtered, edited_df)
    
    
    multiplier_column1 = "ENDINGBALANCE"
    multiplier_column2 = "EXCHANGE_RATE"
    product_column = "LOCAL_CURRENCY"
    def highlight_col(x):
        if x.name in multiplier_column1:
            return ['background-color: #67c5a4']*x.shape[0]
        elif x.name in multiplier_column2:
            return ['background-color: #67c5a4']*x.shape[0]
        elif x.name in product_column:
            return ['background-color: #ff9090']*x.shape[0]
        else:
            return ['background-color: None']*x.shape[0]
        
    tooltips_df = pd.DataFrame({
        'LOCAL_CURRENCY': 'ENDINGBALANCE * EXCHANGE_RATE'
    }, index=[0])
        
    if calculate_local_currencies:
        st.subheader('Local Currency Calculations')
        st.dataframe(pd_df_local_currency)
        
        
        col_loc_add1 = pd_df_local_currency.columns.get_loc(multiplier_column1) + 2
        col_loc_add2 = pd_df_local_currency.columns.get_loc(multiplier_column2) + 2
        col_loc_drop = pd_df_local_currency.columns.get_loc(product_column) + 2
        
        #pd_df_local_currency.style.apply(highlight_max, color="red", subset = "LOCAL_CURRENCY")
        st.table(pd_df_local_currency.head(10).style.apply(highlight_col, axis=0)\
                    .set_table_styles(
                        [{'selector': f'th:nth-child({col_loc_add1})',
                            'props': [('background-color', '#67c5a4')]},
                         {'selector': f'th:nth-child({col_loc_add2})',
                            'props': [('background-color', '#67c5a4')]},
                         {'selector': f'th:nth-child({col_loc_drop})',
                            'props': [('background-color', '#ff9090')]}]))
                    


    calculation_date = st.date_input("Select the Date of Calculation")

    calculation_table_name = "LOCAL_CURRENCY_CALCULATION_" + calculation_date.strftime('%m.%d.%Y')

    with st.form("write"):
        submit_form = st.form_submit_button('Write Local Currency Calculations Table to Snowflake Database')

    if submit_form:
        load_df_to_snowflake(pd_df_local_currency, calculation_table_name)

with tab5:
    
    st.subheader("USD to JPY Exchange Rate")
    if uploaded_csv is not None:
        st.line_chart(USD_JPY_exchange_rates, x="Date", y="Close")

with tab6:
    uploaded_jpg = st.file_uploader("Choose a PDF", type="jpg")

    if uploaded_jpg is not None:
        f1120 = uploaded_jpg
        st.image(f1120, caption = 'f1120')

with tab7:
    
    session=create_session_object()

    #username = st.text_input("Input Username:")

    initial_review_check = st.checkbox("Initial Review")
    initial_review = False
    if initial_review_check:
        initial_review = True

    final_review = st.checkbox("Final Review")

    with st.form("update review table"):
        submit_review = st.form_submit_button("Submit Review")

    #ts = datetime.now()
    #st.write(ts)

    if submit_review:
        session.sql(f"""INSERT INTO DATA_MART.DEMO_REVIEW_TRAIL (USERNAME,INITIALREVIEW,FINALREVIEW) VALUES ('{username}','{initial_review}','{final_review}')""").collect()
        st.success('Success!', icon="✅")

    review_table = load_table(session, 'DATA_MART.DEMO_REVIEW_TRAIL')

    
    
    with st.container():
        st.dataframe(review_table)
    


with tab8:
    tdf = USD_JPY_exchange_rates

    #st.dataframe(tdf)

    gb = GridOptionsBuilder.from_dataframe(tdf)
    gb.configure_column('Close', header_name=('Close'), editable=True)
    #gb.configure_column(“Amt”, header_name=(“Amount”), editable=True, type=[“numericColumn”,“numberColumnFilter”,“customNumericFormat”], precision=0)
    
    
    gridOptions = gb.build()
    dta = AgGrid(tdf,
    gridOptions=gridOptions,
    reload_data=False,
    height=200,
    editable=True,
    theme='streamlit',
    data_return_mode=DataReturnMode.AS_INPUT,
    update_mode=GridUpdateMode.MODEL_CHANGED)

    st.write('Please change an amount to test this')

    if st.button('Iterate through aggrid dataset'):
        for i in range(len(dta['data'])): # or you can use for i in range(tdf.shape[0]):
            st.caption(f"df line: {tdf.loc[i][0]} || AgGrid line: {dta['data']['Close'][i]}") 
    

        # check if any change has been done to any cell in any col by writing a caption out
        if tdf.loc[i]['Close'] != dta['data']['Close'][i]:
            st.caption(f"Name column data changed from {tdf.loc[i]['Close']} to {dta['data']['Close'][i]}...")
            # consequently, you can write changes to a database if/as required

        # if tdf.loc[i]['Amt'] != dta['data']['Amt'][i]:
        #     st.caption(f"Amt column data changed from {tdf.loc[i]['Amt']} to {dta['data']['Amt'][i]}...")

    tdf = dta['data']    # overwrite df with revised aggrid data; complete dataset at one go
    #tdf.to_csv(vpth + 'file1.csv', index=False)  # re/write changed data to CSV if/as required
    st.dataframe(tdf)    # confirm changes to df
    











