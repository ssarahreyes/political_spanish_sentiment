import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px


st.write("""
 # Sentimiento político
 
 ¿Qué partido político español es más popular y mejor valorado en Twitter?
 
 """)

# import data
df = pd.read_csv('../data/data_bases/political_spanish_sentiment_ddbb.csv')

# sidebar
add_selectbox = st.sidebar.selectbox(
    'Elige el partido político',
    ('Email', 'Home phone', 'Mobile phone'))

left_column, right_column = st.beta_columns(2)
# You can use a column just like st.sidebar:


mean_sentiment_party = df.groupby('partido').agg({'sentimiento': 'mean'})
px.bar(data_frame=mean_sentiment_party, title='Sentimiento medio por partido');

