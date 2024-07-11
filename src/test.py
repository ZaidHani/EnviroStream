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
                SELECT 
                    device,
                    ts, 
                    AVG(co) AS co, 
                    AVG(humidity) AS humidity,
                    AVG(lpg) AS lpg,  
                    AVG(smoke) AS smoke, 
                    AVG(temp) AS temp
                FROM 
                    Sensor
                GROUP BY
                    device, ts""")
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
            x = dfs[i]["ts"],
            y = dfs[i][measure], 
            name = i,
            hovertext=dfs[i][measure]))
    return linechart

placeholder = st.empty()

def update_data():
    # query & clean data
    data = query_data()
    device, ts, co, humidity, lpg, smoke, temp = zip(*data)
    data = {'ts':list(ts),
            'device':list(device),
            'co':list(co),
            'humidity':list(humidity),
            'lpg':list(lpg),
            'smoke':list(smoke),
            'temp':list(temp)}
    data = pd.DataFrame(data)

    line_chart = go.Figure()
    line_chart = line_chart.add_trace(go.Scatter(
        x = data['ts'],
        y = data['temp'],
        legend = data['device']))
        
    #line_chart = px.line(x=data['ts'], y=data['temp'], labels=data['device'])
    st.plotly_chart(line_chart)
    st.session_state['last_update'] = time.time()
    time.sleep(1)
    
st.title('EnviroStream')    
update_data()

# while True:
#     with placeholder.container():
#         st.title('EnviroStream')

#         update_data()