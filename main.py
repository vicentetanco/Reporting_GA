import urllib
from typing import List, Any
from io import BytesIO
import streamlit as st
import pandas as pd
import numpy as np
import urllib
from datetime import timedelta
from datetime import date
import base64
import os
import sys
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
import time as t
from random import random
import time
import scipy
from SessionState import get





###    FUNCTIONS   ###
def to_excel(df):
    (max_row, max_col) = df.shape
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1', float_format="%.2f" )
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    for i, col in enumerate(df.columns):
        # find length of column i
        column_len = df[col].astype(str).str.len().max()
        # Setting the length if the column header is larger
        # than the max column value length
        column_len = max(column_len, len(col)) + 2
        # set the column length
        worksheet.set_column(i, i, column_len)
    (max_row, max_col) = df.shape

    # Create a list of column headers, to use in add_table().
    column_settings = [{'header': column} for column in df.columns]

    # Add the Excel table structure. Pandas will add the data.
    worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

    # Make the columns wider for clarity.
    worksheet.set_column(0, max_col - 1, 12)
    writer.save()
    processed_data = output.getvalue()
    return processed_data




def get_table_download_link(df, filename):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    val = to_excel(df)
    b64 = base64.b64encode(val)  # val looks like b'...'
    return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="{filename}.xlsx">Download Excel file</a>'  # decode b'abc' => abc





def main():
    st.sidebar.title('Reporting App | GA')
    st.sidebar.image('logo2.png',use_column_width=True)
    file = st.sidebar.file_uploader('Upload the csv file','csv')
    if file:
        st.sidebar.success('File uploaded successfully')
        generate_report = st.sidebar.button('GENERATE REPORT')
        if generate_report:
            df = pd.read_csv(file)
            df = df.drop(0)
            df_reducido = df[['Student', 'ID', 'SIS User ID', 'SIS Login ID', 'Section',
                   'Actividad evaluable 1.1 (439)', 'Actividad evaluable 1.2 (426)',
                   '★ Actividad evaluable 2 (428)', '★ Actividad evaluable 3.1 (411)',
                   'Actividad evaluable 3.2 (398)', 'Actividad evaluable 4 (422)',
                   'Actividad evaluable 5 (408)', 'Actividad evaluable 6 (397)',
                   '★ Caso de estudio 4.1. Envío y revisión (456)',
                   '★ Caso de estudio 6.1. Envío y revisión (457)',
                   ' Actividades Evaluables  Current Score',
                   ' Actividades Evaluables  Final Score',
                   'Casos de Estudio Current Score',
                   'Casos de Estudio Final Score',
                   'Tareas Current Score',
                   'Tareas Final Score', 'Current Score', 'Final Score']]


            df_reducido['Final Score'] = df_reducido['Final Score'].astype(float)
            df_reducido['PASS'] = ''
            df_reducido = df_reducido.reset_index(drop=True)
            for i in range(len(df_reducido)):
                if df_reducido['Final Score'][i]>60:
                    df_reducido['PASS'][i] = 'YES'
                else:
                    df_reducido['PASS'][i] = 'NO'

            df_aprobados = df_reducido[df_reducido['PASS']== 'YES']
            df_count = df_reducido.groupby(['PASS']).size().reset_index(name='counts')
            fig = px.pie(df_count, values='counts',names='PASS')
            aprobados_mail = list(df_aprobados['SIS Login ID'].unique())
            pass_count = len(df_aprobados)
            total_student_count = len(df_reducido)
            pass_porcentaje = round(((pass_count/total_student_count)*100),2)
            st.title('RESULTS')
            st.metric('Pass ratio','{} %'.format(pass_porcentaje))
            st.plotly_chart(fig)
            with st.expander('View students with the course passed'):
                st.text_area('mail list:', aprobados_mail)
            filename = 'DSBD-ESP-006-SEPT21.xlsx'
            st.markdown(get_table_download_link(df_reducido, filename), unsafe_allow_html=True)
            st.dataframe(df_reducido)










session_state = get(password='')

if session_state.password != 'mitxpro':
    col1111, mid, col2111 = st.columns([1, 3, 1])
    logo = mid.empty()
    logo.image('logo2.png',use_column_width=True)
    pwd_placeholder = mid.empty()
    pwd = pwd_placeholder.text_input("Password:", value="", type="password")
    session_state.password = pwd
    if session_state.password == 'mitxpro':
        pwd_placeholder.empty()
        logo.empty()
        main()
    elif session_state.password != '':
        st.error("the password you entered is incorrect")
        with st.expander("Olvide mi contraseña"):
            st.write('Please contact Vicente Tanco  :  lfmitxpro5@globalalumni.org')
elif session_state.password == 'mitxpro':
    main()


hide_streamlit_style = """ 
            <style> 
            #MainMenu {visibility: hidden;} 
            footer {visibility: hidden;} 
            </style> 
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
