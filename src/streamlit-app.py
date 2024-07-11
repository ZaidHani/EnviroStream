# Streamlit app
import streamlit as st
import psycopg2
import matplotlib.pyplot as plt
import plotly.express as px 
import plotly.graph_objects as go
from plotly.offline import iplot
import statistics
import pandas as pd
import time

def query_data():
    conn = psycopg2.connect("dbname='envirostream' user='postgres' host='localhost' password='anon'")
    cur = conn.cursor()
    cur.execute("""
                SELECT *
                FROM Sensor 
                WHERE ts <= (
                    SELECT MAX(ts) 
                    FROM Sensor 
                    WHERE device='b8:27:eb:bf:9d:51'
                    );
                """)
    rows = cur.fetchall()
    return rows

def clean_outliers(df:pd.DataFrame) -> pd.DataFrame:
    numerical_columns = ['co', 'humidity', 'lpg', 'smoke', 'temp']
    for i in numerical_columns:
        mean = df[i].mean(axis=0)
        std = df[i].std(axis=0)
        df[i] = df[i][(mean - 2 * std <= df[i]) & (df[i] <= mean + 2 * std)].bfill()
    return df
    
def linechart(dfs:dict, measure:str):
    # plot the data
    linechart = go.Figure()
    for i in dfs:
        linechart = linechart.add_trace(go.Scatter(
            x = dfs[i].index,
            y = dfs[i][measure], 
            name = i,
            hovertext=dfs[i][measure]))
    return linechart

placeholder = st.empty()

def update_data():
    # query & clean data
    data = query_data()
    ts, device, co, humidity, light, lpg, motion, smoke, temp = zip(*data)
    data = {'ts':list(ts),
            'device':list(device),
            'co':list(co),
            'humidity':list(humidity),
            'light':list(light),
            'lpg':list(lpg),
            'motion':list(motion),
            'smoke':list(smoke),
            'temp':list(temp)}
    data = pd.DataFrame(data)
    # First Machine
    m1 = data[data['device']=='b8:27:eb:bf:9d:51']  # stable conditions, warmer and dryer
    m1 = clean_outliers(m1)
    m1 = m1.groupby(['ts'])[['co','humidity','lpg','smoke','temp']].mean()
    # Second Machine
    m2 = data[data['device']=='1c:bf:ce:15:ec:4d'] # highly variable temperature and humidity
    m2 = clean_outliers(m2)
    m2 = m2.groupby(['ts'])[['co','humidity','lpg','smoke','temp']].mean()
    # Third Machine
    m3 = data[data['device']=='00:0f:00:70:91:0a'] # stable conditions, cooler and more humid
    m3 = clean_outliers(m3)
    m3 = m3.groupby(['ts'])[['co','humidity','lpg','smoke','temp']].mean()

    dfs = {'m1':m1, 'm2':m2, 'm3':m3}

    fig_col1, fig_col2 = st.columns(2)

    with fig_col1:
        st.text('Tempreture')
        st.plotly_chart(linechart(dfs, 'temp'))
        st.text('Humidity')
        st.plotly_chart(linechart(dfs, 'humidity'))
        st.text('Liquefied Petroleum Gas')
        st.plotly_chart(linechart(dfs, 'lpg'))
    with fig_col2:
        st.text('Machine Readings')
        st.text('Smoke')
        st.plotly_chart(linechart(dfs, 'smoke'))
        st.text('Carbon Monoxide')
        st.plotly_chart(linechart(dfs, 'co'))

    st.session_state['last_update'] = time.time()
    time.sleep(1)
    
while True:
    with placeholder.container():
        st.title('EnviroStream')

        update_data()