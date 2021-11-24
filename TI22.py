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
import sqlalchemy
from sqlalchemy.exc import (SQLAlchemyError)
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
import time as t
from random import random
import time
import scipy
from SessionState import get


session_state = get(password='')




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

def to_excel_report(df,Annotation,fig,fig2,fig3,resume):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='LastInspection', float_format="%.2f" )
    workbook = writer.book
    worksheet = writer.sheets['LastInspection']
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



    worksheet2 = workbook.add_worksheet('ReadMe')
    worksheet2.write(0, 0, Annotation)
    image_data = BytesIO(fig.to_image(format="png"))
    image_data2 = BytesIO(fig2.to_image(format="png"))
    image_data3 = BytesIO(fig3.to_image(format="png"))

    # Create a new worksheet and add the image to it.
    worksheet3 = workbook.add_worksheet('Plots')
    worksheet3.insert_image(2, 3, 'plotly.png', {'image_data': image_data})
    worksheet3.insert_image(42, 3, 'plotly.png', {'image_data': image_data2})
    worksheet3.insert_image(82, 3, 'plotly.png', {'image_data': image_data3})

    resume.to_excel(writer, index=False, sheet_name='Summary', float_format="%.2f")
    worksheet4 = writer.sheets['Summary']
    for i, col in enumerate(resume.columns):
        # find length of column i
        column_len = resume[col].astype(str).str.len().max()
        # Setting the length if the column header is larger
        # than the max column value length
        column_len = max(column_len, len(col)) + 2
        # set the column length
        worksheet4.set_column(i, i, column_len)
    (max_row, max_col) = resume.shape

    # Create a list of column headers, to use in add_table().
    column_settings = [{'header': column} for column in resume.columns]

    # Add the Excel table structure. Pandas will add the data.
    worksheet4.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

    # Make the columns wider for clarity.
    worksheet4.set_column(0, max_col - 1, 12)


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

def get_table_download_report(df, filename,Annotation,fig,fig2,fig3,resume):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    val = to_excel_report(df,Annotation,fig,fig2,fig3,resume)
    b64 = base64.b64encode(val)  # val looks like b'...'
    return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="{filename}.xlsx">Download Excel file</a>'  # decode b'abc' => abc

def get_table_download_link_raw(df, filename):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    val = to_excel(df)
    b64 = base64.b64encode(val)  # val looks like b'...'
    return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="{filename}.xlsx">Download full database</a>'  # decode b'abc' => abc


def ETL(info, df,comment):
    info = info[['Unnamed: 1', 'Unnamed: 2']]
    info = info.iloc[:12]
    Windfarm = info.iloc[0, 1]
    Windfarm = Windfarm.upper()
    Wtg = info.iloc[1, 1]
    Wtg = Wtg.upper()
    blade_pos = info.iloc[2, 1]
    blade_type = info.iloc[3, 1]
    blade_type = blade_type.upper()
    Manufacturer = info.iloc[4, 1]
    Manufacturer = Manufacturer.upper()
    SRN = info.iloc[5, 1]
    inspection_date = info.iloc[6, 1]
    service_provider = info.iloc[7, 1]
    service_provider = service_provider.upper()
    inspector = info.iloc[8, 1]
    inspector = inspector.upper()
    inspection_folder = info.iloc[9, 1]
    inspection_folder = inspection_folder.upper()
    validation_date = info.iloc[10, 1]
    validation_person = info.iloc[11, 1]
    validation_person = validation_person.upper()
    df.columns = ["Blade radious", "Red coloured area?", "Excessive adhesive?", "Wrinkle or step?", "H", "L",
                  "Damages at LE?", "Damage description", "Z ini (m)", "Z end (m)",
                  "Defect max spanwise dimension", "Defect max chordwise", "Severity",'observations']

    df = df.reset_index(drop=True)
    for i in range(len(df)):
        if pd.isnull(df['Severity'][i]):
            df = df.drop([i], axis=0)
    df['Comment'] = comment
    df['WindFarm'] = Windfarm
    df['WTG'] = Wtg
    df['Blade position'] = blade_pos
    df['Blade type'] = blade_type
    df['Manufacturer'] = Manufacturer
    df['SRN'] = SRN
    df['inspection_date'] = inspection_date
    df['service_provider'] = service_provider
    df['inspector'] = inspector
    df['inspection_folder'] = inspection_folder
    df['validation_date'] = validation_date
    df['validation_person'] = validation_person
    return df


def LECTURA_BD():
    with open(file='Tracasa.txt', mode="r",encoding="utf-8") as f:
        sql_source = f.read()

    df_tracasa = pd.read_sql(
        sql=sql_source,
        con=CONEXION_SQL()
    )
    df_tracasa['inspection_date'] = df_tracasa['inspection_date'].dt.date
    return df_tracasa


def CONEXION_SQL():
    tracasa_fast_executemany: bool = False
    tracasa_timeout: int = 0

    tracasa = 'PRO'
    if tracasa == "DEV":
        tracasa_host = r"TRASQL02V\SQL2016:1433"
        tracasa_host_ip = r"10.199.2.43:1433"
        tracasa_pwd = os.getenv(key="TRACASA_PRE")
    elif tracasa == "PRE":
        tracasa_host = r"TRASQL125V\SQL2016:1433"
        tracasa_host_ip = r"10.199.2.184:1433"
        tracasa_pwd = os.getenv(key="TRACASA_QA")
    elif tracasa == "PRO":
        tracasa_host = r"TRASQL04V\SQL2016:1433"
        tracasa_host_ip = r"10.199.2.45:1433"
        tracasa_pwd = os.getenv(key="TRACASA_PRO")

    tracasa_database = r"DataAnalyticsDINT"

    if tracasa_fast_executemany is False:
        tracasa_driver = r"SQL+Server"
        tracasa_fast_executemany = False
    elif tracasa_fast_executemany is True:
        tracasa_driver = r"ODBC+Driver+17+for+SQL+Server"
        tracasa_fast_executemany = True

    try:
        if tracasa is None:
            conn_tracasa = None

        elif tracasa_pwd is None:
            conn_tracasa: sqlalchemy.engine.base.Engine = sqlalchemy.create_engine(
                r"mssql+pyodbc://{host}/{database}?trusted_connection=yes&driver={driver}" \
                    .format(
                    host=tracasa_host,
                    database=tracasa_database,
                    driver=tracasa_driver
                ),
                fast_executemany=tracasa_fast_executemany,
                connect_args={
                    'timeout': tracasa_timeout
                }
            )

            conn_tracasa \
                .connect()

        else:
            conn_tracasa: sqlalchemy.engine.base.Engine = sqlalchemy.create_engine(
                r"mssql+pyodbc://{user}:{password}@{host}/{database}?driver={driver}" \
                    .format(
                    user="UsrDataAnalyticsDINT",
                    password=urllib.parse.quote_plus(
                        string=tracasa_pwd
                    ),
                    host=tracasa_host_ip,
                    database=tracasa_database,
                    driver=tracasa_driver
                ),
                fast_executemany=tracasa_fast_executemany,
                connect_args={
                    'timeout': tracasa_timeout
                }
            )

            conn_tracasa \
                .connect()

        conn_tracasa = conn_tracasa

    except SQLAlchemyError as e:
        print("[SQLAlchemy/ODBC ERROR]: {0}".format(e))
        sys.exit('Connection to TRACASA Server: "' + str(tracasa_host) + '" ("' + str(
            tracasa_host_ip) + '") not possible')


    return conn_tracasa


def FILTER(windfarm, wtg, blade_type, manufacturer, srn,blade_criticidad, df_tracasa):
    if windfarm[0] != '<all>':
        df_tracasa = df_tracasa[df_tracasa['WindFarm'].isin(windfarm)]
    if wtg[0] != '<all>':
        df_tracasa = df_tracasa[df_tracasa['WTG'].isin(wtg)]
    if blade_type[0] != '<all>':
        df_tracasa = df_tracasa[df_tracasa['Blade type'].isin(blade_type)]
    if manufacturer[0] != '<all>':
        df_tracasa = df_tracasa[df_tracasa['Manufacturer'].isin(manufacturer)]
    if srn[0] != '<all>':
        df_tracasa = df_tracasa[df_tracasa['SRN'].isin(srn)]
    if blade_criticidad[0] != '<all>':
        df_tracasa = df_tracasa[df_tracasa['Blade severity'].isin(blade_criticidad)]
    if len(df_tracasa) == 0:
        st.info('No results')
    else:
        return df_tracasa


@st.experimental_memo
def ETL_REGION_CLIENTE():
    Cliente = pd.read_excel(r'CLIENTE_REGION_TI22.xlsx', sheet_name='Hoja2')
    CLIENTE_REGION = Cliente[['WindFarm','CUSTOMER','COUNTRY','REGION']]
    MOLDE = pd.read_excel(r'MOLDE_TI22.xlsx')
    return CLIENTE_REGION, MOLDE,Cliente


@st.experimental_memo
def ETL_ESTADO_ACTUAL(df_tracasa):
    ID = df_tracasa['BLADE ID Z'].unique()
    ESTADO_ACTUAL = pd.DataFrame(columns=df_tracasa.columns)
    for i in ID:
        aux = df_tracasa[df_tracasa['BLADE ID Z'] == i]
        aux = aux.sort_values(by='inspection_date',ascending=False)
        aux = aux.reset_index(drop=True)
        aux = aux.iloc[[0]]
        ESTADO_ACTUAL = ESTADO_ACTUAL.append(aux)
    return ESTADO_ACTUAL

@st.experimental_memo
def ETL_ESTADO_ACTUAL_SEM(df_tracasa):
    ID = df_tracasa['BLADE ID Z'].unique()
    ESTADO_ACTUAL = pd.DataFrame(columns=df_tracasa.columns)
    for i in ID:
        aux = df_tracasa[df_tracasa['BLADE ID Z'] == i]
        aux = aux.sort_values(by='inspection_date',ascending=False)
        aux = aux.reset_index(drop=True)
        aux = aux.iloc[[0]]
        ESTADO_ACTUAL = ESTADO_ACTUAL.append(aux)
    return ESTADO_ACTUAL

@st.experimental_memo
def CRITICIDAD_PALA(df_tracasa):
    BLADES = df_tracasa['BLADE ID'].unique()
    severity_b = pd.DataFrame(index=range(0,len(BLADES)),columns=['BLADE ID','Blade severity'])
    for i in BLADES:
        df_tracasa_blade = df_tracasa[df_tracasa['BLADE ID'] == i]
        df_tracasa_blade = df_tracasa_blade.sort_values(by='Severity',ascending=False)
        df_tracasa_blade = df_tracasa_blade.reset_index(drop=True)
        severity = df_tracasa_blade['Severity'][0]
        severity_b = severity_b.append({'BLADE ID':i,'Blade severity':severity},ignore_index=True)
    severity_b = severity_b.dropna()
    df_tracasa = df_tracasa.merge(severity_b, how='left', on='BLADE ID')
    return df_tracasa

@st.experimental_memo
def CRITICIDAD_PALA_SEM(df_tracasa):
    BLADES = df_tracasa['BLADE ID'].unique()
    severity_b = pd.DataFrame(index=range(0,len(BLADES)),columns=['BLADE ID','Blade severity'])
    for i in BLADES:
        df_tracasa_blade = df_tracasa[df_tracasa['BLADE ID'] == i]
        df_tracasa_blade = df_tracasa_blade.sort_values(by='Severity',ascending=False)
        df_tracasa_blade = df_tracasa_blade.reset_index(drop=True)
        severity = df_tracasa_blade['Severity'][0]
        severity_b = severity_b.append({'BLADE ID':i,'Blade severity':severity},ignore_index=True)
    severity_b = severity_b.dropna()
    df_tracasa = df_tracasa.merge(severity_b, how='left', on='BLADE ID')
    return df_tracasa

@st.experimental_memo
def SEGUNDA_CRITICIDAD(df_tracasa_actual):
    BLADES = df_tracasa_actual['BLADE ID'].unique()
    severity_b = pd.DataFrame(columns=['BLADE ID', 'GLOBAL Blade severity'])
    Zs = list(df_tracasa_actual["Blade radious"].unique())
    for i in BLADES:
        df_tracasa_blade = df_tracasa_actual[df_tracasa_actual['BLADE ID'] == i]
        sc = list()
        for x in Zs:
            df_tracasa_blade_Z = df_tracasa_blade[df_tracasa_blade["Blade radious"] == x]
            if len(df_tracasa_blade_Z) == 0:
                sc.append(0)
            else:
                df_tracasa_blade_Z = df_tracasa_blade_Z.reset_index(drop=True)
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('0 - No defect', int(0))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('1 - Initial', int(1))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('2 - Intermediate', int(2))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('3 - Advanced', int(3))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('4 - Severe', int(4))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('5 - Critical', int(5))
                df_tracasa_blade_Z = df_tracasa_blade_Z.sort_values(by='Severity', ascending=False)
                sc.append(df_tracasa_blade_Z['Severity'][0])
        suma = sum(sc)
        severity_b = severity_b.append({'BLADE ID':i, 'GLOBAL Blade severity':suma},ignore_index=True)
    severity_b = severity_b.dropna()
    df_tracasa_actual = df_tracasa_actual.merge(severity_b, how='left', on='BLADE ID')
    return df_tracasa_actual



@st.experimental_memo
def SEGUNDA_CRITICIDAD_SEM(df_tracasa_actual):
    BLADES = df_tracasa_actual['BLADE ID'].unique()
    severity_b = pd.DataFrame(columns=['BLADE ID', 'GLOBAL Blade severity'])
    Zs = list(df_tracasa_actual["Blade radious"].unique())
    for i in BLADES:
        df_tracasa_blade = df_tracasa_actual[df_tracasa_actual['BLADE ID'] == i]
        sc = list()
        for x in Zs:
            df_tracasa_blade_Z = df_tracasa_blade[df_tracasa_blade["Blade radious"] == x]
            if len(df_tracasa_blade_Z) == 0:
                sc.append(0)
            else:
                df_tracasa_blade_Z = df_tracasa_blade_Z.reset_index(drop=True)
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('0 - No defect', int(0))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('1 - Initial', int(1))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('2 - Intermediate', int(2))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('3 - Advanced', int(3))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('4 - Severe', int(4))
                df_tracasa_blade_Z = df_tracasa_blade_Z.replace('5 - Critical', int(5))
                df_tracasa_blade_Z = df_tracasa_blade_Z.sort_values(by='Severity', ascending=False)
                sc.append(df_tracasa_blade_Z['Severity'][0])
        suma = sum(sc)
        severity_b = severity_b.append({'BLADE ID':i, 'GLOBAL Blade severity':suma},ignore_index=True)
    severity_b = severity_b.dropna()
    df_tracasa_actual = df_tracasa_actual.merge(severity_b, how='left', on='BLADE ID')
    return df_tracasa_actual


@st.experimental_memo
def NEXT_INSP(df_tracasa_actual):
    BLADES = df_tracasa_actual['BLADE ID'].unique()
    severity_b = pd.DataFrame(index=range(0, len(BLADES)), columns=['BLADE ID', 'PROXIMA_INSP'])
    for i in BLADES:
        df_tracasa_blade = df_tracasa_actual[df_tracasa_actual['BLADE ID'] == i]
        df_tracasa_blade = df_tracasa_blade.sort_values(by='Fecha', ascending=True)
        df_tracasa_blade = df_tracasa_blade.reset_index(drop=True)
        fecha = df_tracasa_blade['Fecha'][0]
        severity_b = severity_b.append({'BLADE ID': i, 'PROXIMA_INSP': fecha}, ignore_index=True)
    severity_b = severity_b.dropna()
    df_tracasa_actual = df_tracasa_actual.merge(severity_b, how='left', on='BLADE ID')
    del df_tracasa_actual['Fecha']
    return df_tracasa_actual



@st.experimental_memo
def NEXT_INSP_SEM(df_tracasa_actual):
    BLADES = df_tracasa_actual['BLADE ID'].unique()
    severity_b = pd.DataFrame(index=range(0, len(BLADES)), columns=['BLADE ID', 'PROXIMA_INSP'])
    for i in BLADES:
        df_tracasa_blade = df_tracasa_actual[df_tracasa_actual['BLADE ID'] == i]
        df_tracasa_blade = df_tracasa_blade.sort_values(by='Fecha', ascending=True)
        df_tracasa_blade = df_tracasa_blade.reset_index(drop=True)
        fecha = df_tracasa_blade['Fecha'][0]
        severity_b = severity_b.append({'BLADE ID': i, 'PROXIMA_INSP': fecha}, ignore_index=True)
    severity_b = severity_b.dropna()
    df_tracasa_actual = df_tracasa_actual.merge(severity_b, how='left', on='BLADE ID')
    del df_tracasa_actual['Fecha']
    return df_tracasa_actual


@st.experimental_memo
def ARRUGAS(df_tracasa):
    df_tracasa = df_tracasa.reset_index(drop=True)
    df_tracasa = df_tracasa.reindex(columns=df_tracasa.columns.tolist() + ["AR", "Criterio CS", 'Criterio Circular'])
    for i in range(len(df_tracasa)):
        if not (pd.isnull(df_tracasa["H"][i]) and df_tracasa["L"][i]):
            H = df_tracasa["H"][i]
            L = df_tracasa["L"][i]
            AR = (2 * L) / H
            if H < 2:
                df_tracasa['AR'][i] = AR
                df_tracasa['Criterio CS'][i] = 'OK'
                df_tracasa['Criterio Circular'][i] = 'OK'
            elif 2 <= H <= 5:
                df_tracasa['AR'][i] = AR
                if AR > 10:
                    df_tracasa['Criterio CS'][i] = 'OK'
                else:
                    df_tracasa['Criterio CS'][i] = 'NO OK'
                if AR >= 7:
                    df_tracasa['Criterio Circular'][i] = 'OK'
                else:
                    df_tracasa['Criterio Circular'][i] = 'NO OK'
            elif H > 5:
                df_tracasa['AR'][i] = AR
                df_tracasa['Criterio CS'][i] = 'NO OK'
                df_tracasa['Criterio Circular'][i] = 'NO OK'

    return df_tracasa


@st.experimental_memo
def ARRUGAS_SEM(df_tracasa):
    df_tracasa = df_tracasa.reset_index(drop=True)
    df_tracasa = df_tracasa.reindex(columns=df_tracasa.columns.tolist() + ["AR", "Criterio CS", 'Criterio Circular'])
    for i in range(len(df_tracasa)):
        if not (pd.isnull(df_tracasa["H"][i]) and df_tracasa["L"][i]):
            H = df_tracasa["H"][i]
            L = df_tracasa["L"][i]
            AR = (2 * L) / H
            if H < 2:
                df_tracasa['AR'][i] = AR
                df_tracasa['Criterio CS'][i] = 'OK'
                df_tracasa['Criterio Circular'][i] = 'OK'
            elif 2 <= H <= 5:
                df_tracasa['AR'][i] = AR
                if AR > 10:
                    df_tracasa['Criterio CS'][i] = 'OK'
                else:
                    df_tracasa['Criterio CS'][i] = 'NO OK'
                if AR >= 7:
                    df_tracasa['Criterio Circular'][i] = 'OK'
                else:
                    df_tracasa['Criterio Circular'][i] = 'NO OK'
            elif H > 5:
                df_tracasa['AR'][i] = AR
                df_tracasa['Criterio CS'][i] = 'NO OK'
                df_tracasa['Criterio Circular'][i] = 'NO OK'

    return df_tracasa

@st.experimental_memo
def FECHAS_NEXT_INSP(df_tracasa_actual):
    pala_next = pd.DataFrame(columns=['BLADE ID Z','Fecha'])
    for i in range(len(df_tracasa_actual)):
        if pd.isnull(df_tracasa_actual["Z ini (m)"][i]):
            Z = df_tracasa_actual["Blade radious"][i]
            Z = Z[1:]
            Z = float(Z)
        else:
            Z = float(df_tracasa_actual["Z ini (m)"][i])

        S = df_tracasa_actual["Severity"][i]
        I = df_tracasa_actual["inspection_date"][i]
        B = df_tracasa_actual['BLADE ID Z'][i]
        T = df_tracasa_actual['Blade type'][i]
        if T != 'AW68.7':
            if Z <= 5.8:
                if S =='0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z':B,'Fecha':N},ignore_index=True)
                if S =='1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
            if 5.8 < Z <= 9:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
            if 9 < Z <= 14:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=90)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=15)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
            if 14 < Z <= 21:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
        else:
            if Z <= 9.8:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
            if 9.8 < Z <= 13:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
            if 13 < Z <= 18:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=90)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=15)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N},ignore_index=True)
            if 18 < Z <= 21:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)


    pala_next = pala_next.dropna()
    df_tracasa_actual = df_tracasa_actual.merge(pala_next, how='left', on='BLADE ID Z')
    return df_tracasa_actual

@st.experimental_memo
def FECHAS_NEXT_INSP_SEM(df_tracasa_actual):
    pala_next = pd.DataFrame(columns=['BLADE ID Z', 'Fecha'])
    for i in range(len(df_tracasa_actual)):
        if pd.isnull(df_tracasa_actual["Z ini (m)"][i]):
            Z = df_tracasa_actual["Blade radious"][i]
            Z = Z[1:]
            Z = float(Z)
        else:
            Z = float(df_tracasa_actual["Z ini (m)"][i])

        S = df_tracasa_actual["Severity"][i]
        I = df_tracasa_actual["inspection_date"][i]
        B = df_tracasa_actual['BLADE ID Z'][i]
        T = df_tracasa_actual['Blade type'][i]
        if T != 'AW68.7':
            if Z <= 5.8:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
            if 5.8 < Z <= 9.8:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
            if 9.8 < Z <= 13.2:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=90)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=15)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
            if 13.2 < Z <= 21:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
        else:
            if Z <= 9.8:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
            if 9.8 < Z <= 13.8:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
            if 13.8 < Z <= 17.2:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=90)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=15)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
            if 17.2 < Z <= 21:
                if S == '0 - No defect':
                    N = I + timedelta(days=365)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '1 - Initial':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '2 - Intermediate':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '3 - Advanced':
                    N = I + timedelta(days=180)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '4 - Severe':
                    N = I + timedelta(days=30)
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)
                if S == '5 - Critical':
                    N = I
                    pala_next = pala_next.append({'BLADE ID Z': B, 'Fecha': N}, ignore_index=True)

    pala_next = pala_next.dropna()
    df_tracasa_actual = df_tracasa_actual.merge(pala_next, how='left', on='BLADE ID Z')
    return df_tracasa_actual

def REPORT_GENERATION_CLIENT(df_tracasa,df_cliente_info,df_tracasa_actual,CLIENTE):

    df_cliente_info = df_cliente_info[df_cliente_info['CUSTOMER']==CLIENTE]
    parque = list(df_cliente_info['WindFarm'].unique())
    df_tracasa = df_tracasa.replace('IND (INDUTCH)', 'IND')
    df_tracasa = df_tracasa.replace('NBM (CONCAMEX)', 'NBM')
    df_tracasa = df_tracasa.replace('NBS (ACCIONA BLADES)', 'NBS')
    df_tracasa = df_tracasa.replace('AER (AERIS)', 'AER')
    df_tracasa_actual = df_tracasa_actual.replace('IND (INDUTCH)', 'IND')
    df_tracasa_actual = df_tracasa_actual.replace('NBM (CONCAMEX)', 'NBM')
    df_tracasa_actual = df_tracasa_actual.replace('NBS (ACCIONA BLADES)', 'NBS')
    df_tracasa_actual = df_tracasa_actual.replace('AER (AERIS)', 'AER')
    today = date.today()
    today_str = str(today)
    df_tracasa_actual = df_tracasa_actual[df_tracasa_actual['WindFarm'].isin(parque)]
    df_tracasa_actual = df_tracasa_actual.reset_index(drop=True)
    df_cliente_info = df_cliente_info.reset_index(drop=True)
    df_tracasa_wf = df_tracasa
    df_tracasa_wf = df_tracasa_wf[df_tracasa_wf['WindFarm'].isin(parque)]
    df_tracasa_wf = df_tracasa_wf.reset_index(drop=True)
    index = ['installed blades', '     inspected blades', '     inspected blades [% of installed]',
             '          NOT affected blades', '          Affected blades CUMULATIVE',
             '          Affected blades CUMULATIVE [% of inspected]', '               Repaired blades',
             '               Repaired blades [% of affected]', '               Affected blades NOW',
             '                    1 - Initial', '                    2 - Intermediate',
             '                    3 - Advanced', '                    4 - Severe',
             '                    5 - Critical', 'Blades meeting repair criteria',
             'Blades not affected but AR out of spec']
    MANUFACTURER_LIST = sorted(list(df_tracasa['Manufacturer'].unique()))

    report = pd.DataFrame(index=index, columns=MANUFACTURER_LIST)
    st.header('{}'.format(parque))
    for i in MANUFACTURER_LIST:

        df_tracasa_wf_manufacturer = df_tracasa_wf[df_tracasa_wf['Manufacturer'] == i]
        df_tracasa_actual_manufacturer = df_tracasa_actual[df_tracasa_actual['Manufacturer'] == i]
        if len(df_tracasa_wf_manufacturer) == 0:
            installed = df_cliente_info['{}'.format(i)][0]
            aux = [int(installed), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            aux = list(aux)
            report['{}'.format(i)] = aux
        else:
            if len(df_cliente_info) == 1:
                installed = df_cliente_info['{}'.format(i)][0]
                inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                p_installed = round(((inspected / installed) * 100), 2)
                affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                    ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                affected_now = len(affected_now['BLADE ID'].unique())
                affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                    ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                affected_cum = len(affected_cum['BLADE ID'].unique())
                p_inspected = round(((affected_cum / inspected) * 100), 2)
                torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                    ['3 - Advanced', '4 - Severe', '5 - Critical'])]
                torepair = len(torepair['BLADE ID'].unique())
                repaired = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Repair'] == 'YES']
                repaired = len(repaired['BLADE ID'].unique())
                if affected_cum == 0:
                    p_affected = 0
                else:
                    p_affected = round(((repaired / affected_cum) * 100), 2)
                cero = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                cero = len(cero['BLADE ID'].unique())
                uno = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                uno = len(uno['BLADE ID'].unique())
                dos = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                dos = len(dos['BLADE ID'].unique())
                tres = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                tres = len(tres['BLADE ID'].unique())
                cuatro = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                cuatro = len(cuatro['BLADE ID'].unique())
                cinco = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                cinco = len(cinco['BLADE ID'].unique())

                AR_7 = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['AR'] < 8]
                AR_7 = AR_7[AR_7['Blade severity'] == '0 - No defect']
                AR_7 = AR_7['BLADE ID'].unique()
                AR_7 = len(AR_7)
                aux = [int(installed), inspected, p_installed, cero, affected_cum, p_inspected, repaired,
                       p_affected,
                       affected_now, uno, dos, tres, cuatro, cinco, torepair, AR_7]
                aux = list(aux)
                report['{}'.format(i)] = aux
            elif len(df_cliente_info) > 1:

                installed = df_cliente_info['{}'.format(i)].sum()
                inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                p_installed = round(((inspected / installed) * 100), 2)
                affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                    ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                affected_cum = len(affected_cum['BLADE ID'].unique())
                affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                    ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                affected_now = len(affected_now['BLADE ID'].unique())
                p_inspected = round(((affected_cum / inspected) * 100), 2)
                torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                    ['3 - Advanced', '4 - Severe', '5 - Critical'])]
                torepair = len(torepair['BLADE ID'].unique())
                repaired = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Repair'] == 'YES']
                repaired = len(repaired['BLADE ID'].unique())
                if affected_cum == 0:
                    p_affected = 0
                else:
                    p_affected = round(((repaired / affected_cum) * 100), 2)
                cero = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                cero = len(cero['BLADE ID'].unique())
                uno = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                uno = len(uno['BLADE ID'].unique())
                dos = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                dos = len(dos['BLADE ID'].unique())
                tres = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                tres = len(tres['BLADE ID'].unique())
                cuatro = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                cuatro = len(cuatro['BLADE ID'].unique())
                cinco = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                cinco = len(cinco['BLADE ID'].unique())
                AR_7 = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']

                AR_7 = AR_7[AR_7['AR'] < 8]
                AR_7 = AR_7['BLADE ID'].unique()
                AR_7 = len(AR_7)
                aux = [int(installed), inspected, p_installed, cero, affected_cum, p_inspected, repaired,
                       p_affected,
                       affected_now, uno, dos, tres, cuatro, cinco, torepair, AR_7]
                aux = list(aux)
                report['{}'.format(i)] = aux

    installed_total = (df_cliente_info['TOTAL INSTALLED'].sum())
    inspected_total = report.iloc[[1]].sum()
    inspected_total = inspected_total.sum()
    por_isnpected = round((inspected_total / installed_total) * 100, 2)
    not_affected_total = report.iloc[[3]].sum()
    not_affected_total = not_affected_total.sum()
    affected_cumu = report.iloc[[4]].sum()
    affected_cumu = affected_cumu.sum()
    por_effected = round((affected_cumu / inspected_total) * 100, 2)
    repaired_total = report.iloc[[6]].sum()
    repaired_total = repaired_total.sum()
    por_reppaired = round((repaired_total / affected_cumu) * 100,2)
    afected_now_total = report.iloc[[8]].sum()
    afected_now_total = afected_now_total.sum()
    uno_total = report.iloc[[9]].sum()
    uno_total = uno_total.sum()
    dos_total = report.iloc[[10]].sum()
    dos_total = dos_total.sum()
    tres_total = report.iloc[[11]].sum()
    tres_total = tres_total.sum()
    cuatro_total = report.iloc[[12]].sum()
    cuatro_total = cuatro_total.sum()
    cinco_total = report.iloc[[13]].sum()
    cinco_total = cinco_total.sum()
    AR_7_total = report.iloc[[15]].sum()
    AR_7_total = AR_7_total.sum()
    torepair_tot = report.iloc[[14]].sum()
    torepair_tot = torepair_tot.sum()
    total = [installed_total, inspected_total, por_isnpected, not_affected_total, affected_cumu, por_effected,
             repaired_total, por_reppaired, afected_now_total, uno_total, dos_total, tres_total, cuatro_total,
             cinco_total, torepair_tot, AR_7_total]
    fig = go.Figure(data=[go.Pie(
        labels=['0 - No defect', '1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'],
        values=[not_affected_total, uno_total, dos_total, tres_total, cuatro_total, cinco_total], hole=.4,
        marker_colors=['#2ca02c', 'blue', 'yellow', 'orange', 'red', 'black'], sort=False)])
    fig.update_layout(
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="RebeccaPurple"
        )
    )
    report['TOTAL'] = total

    color_discrete_map = {'IND': 'rgb(42,9,4)', 'NBM': 'rgb(111,203,209)', 'NBS': 'rgb(55,165,172),',
                          'TMT': 'rgb(29,127,136)', 'TPI': 'rgb(2,84,92)'}
    fig2 = ff.create_table(report, index=index, index_title= today_str)
    today = date.today()
    today_str = str(today)
    filename_r = 'REPORT_{}'.format(today_str)

    report['index'] = report.index
    cols = report.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    report = report[cols]
    st.markdown(get_table_download_link(report, filename_r), unsafe_allow_html=True)
    del report['index']

    fig.update_layout(width=1000, height=800)
    fig2.update_layout(width=2200, height=800)
    st.plotly_chart(fig2)

    st.plotly_chart(fig)
    report_ratios = report
    report = report.drop(['     inspected blades [% of installed]',
                          '          NOT affected blades',
                          '          Affected blades CUMULATIVE [% of inspected]',
                          '               Repaired blades [% of affected]', '               Affected blades NOW',
                          '                    1 - Initial', '                    2 - Intermediate',
                          '                    3 - Advanced', '                    4 - Severe',
                          '                    5 - Critical',
                          'Blades meeting repair criteria', 'Blades not affected but AR out of spec'])

    del report['TOTAL']
    report_ratios = report_ratios.drop(['installed blades', '     inspected blades [% of installed]',
                                        '          NOT affected blades', '          Affected blades CUMULATIVE',
                                        '          Affected blades CUMULATIVE [% of inspected]',
                                        '               Repaired blades',
                                        '               Repaired blades [% of affected]',
                                        '               Affected blades NOW',
                                        'Blades meeting repair criteria', 'Blades not affected but AR out of spec'])
    del report_ratios['TOTAL']

    inspected_list = report_ratios.iloc[0]
    uno_list = report_ratios.iloc[1]
    dos_list = report_ratios.iloc[2]
    tres_list = report_ratios.iloc[3]
    cuatro_list = report_ratios.iloc[4]
    cinco_list = report_ratios.iloc[5]

    uno_list_ratio = 100 * (uno_list / inspected_list)
    dos_list_ratio = 100 * (dos_list / inspected_list)
    tres_list_ratio = 100 * (tres_list / inspected_list)
    cuatro_list_ratio = 100 * (cuatro_list / inspected_list)
    cinco_list_ratio = 100 * (cinco_list / inspected_list)

    report_ratios = report_ratios.drop(
        ['     inspected blades', '                    1 - Initial', '                    2 - Intermediate',
         '                    3 - Advanced', '                    4 - Severe', '                    5 - Critical'])

    report_ratios = report_ratios.append(uno_list_ratio, ignore_index=True)
    report_ratios = report_ratios.append(dos_list_ratio, ignore_index=True)
    report_ratios = report_ratios.append(tres_list_ratio, ignore_index=True)
    report_ratios = report_ratios.append(cuatro_list_ratio, ignore_index=True)
    report_ratios = report_ratios.append(cinco_list_ratio, ignore_index=True)

    report_ratios.set_axis(['1 - %', '2 - %', '3 - %', '4 - %', '5 - %'])

    report_ratios = report_ratios.T
    report_ratios.columns = ['1 - %', '2 - %', '3 - %', '4 - %', '5 - %']
    color_discrete_map_sev = {'1 - %': 'blue', '2 - %': 'yellow', '3 - %': 'orange', '4 - %': 'red',
                              '5 - %': 'black'}

    fig4 = px.bar(report_ratios, title='RATIOS' + today_str,
                  color_discrete_map=color_discrete_map_sev)

    fig4.update_layout(width=1000, height=800)
    fig3 = px.bar(report, title=today_str)

    fig3.update_layout(width=1000, height=800)
    st.plotly_chart(fig3)
    st.plotly_chart(fig4)
    df_rep = df_tracasa[df_tracasa['WindFarm'].isin(parque)]
    df_tracasa_actual_wf = df_tracasa_actual[df_tracasa_actual['WindFarm'].isin(parque)]
    blades_id = sorted(list(df_tracasa_actual_wf['BLADE ID'].unique()))
    resume = pd.DataFrame(
        columns=['BLADE ID', 'WF', 'WTG', 'BLADE POS', 'BLADE TYPE', 'MANUFACTURER', 'SRN', 'INSPECTION DATE',
                 'SEVERITY(Z4)', 'SEVERITY(Z8)', 'SEVERITY(Z7)', 'SEVERITY(Z10)', 'SEVERITY(Z13)', 'SEVERITY(Z6)',
                 'SEVERITY(Z11,5)', 'SEVERITY(Z16,6)', 'SEVERITY(Z15,5)', 'SEVERITY(Z19)', 'SEVERITY(Z14)',
                 'SEVERITY(Z16)', 'SEVERITY(Z11)', 'SEVERITY(Z20)', 'SEVERITY(Z20,6)', 'SEVERITY(Z17)',
                 'SEVERITY(Z18)', 'SEVERITY(OTHER01)', 'SEVERITY(OTHER02)', 'BLADE SEVERITY (WORST)',
                 'BLADE SEVERITY (ACUMULATED)', 'NEXT INSPECTION', 'REPAIRED'])
    for i in blades_id:
        df_blade = df_tracasa_actual_wf[df_tracasa_actual_wf['BLADE ID'] == i]
        df_blade = df_blade.reset_index(drop=True)
        wf = df_blade['WindFarm'][0]
        wtg = df_blade['WTG'][0]
        pos = df_blade['Blade position'][0]
        type_b = df_blade['Blade type'][0]
        manufact = df_blade['Manufacturer'][0]
        srn = df_blade['SRN'][0]
        ins = df_blade['inspection_date'][0]
        sev_w = df_blade['Blade severity'][0]
        sev_g = df_blade['GLOBAL Blade severity'][0]
        next = df_blade['PROXIMA_INSP'][0]
        df_blade_rep = df_rep[df_rep['BLADE ID'] == i]
        df_blade_rep = df_blade_rep.reset_index(drop=True)
        df_blade_rep = df_blade_rep[df_blade_rep['Repair'] == 'YES']
        if len(df_blade_rep) == 0:
            rep = 'NO'
        else:
            rep = 'YES'

        bladeid_z19 = df_blade[df_blade['Blade radious'] == 'Z19']
        if len(bladeid_z19) > 0:
            bladeid_z19 = bladeid_z19.reset_index(drop=0)
            z19 = bladeid_z19['Severity'][0]
        else:
            z19 = 'NA'

        bladeid_z18 = df_blade[df_blade['Blade radious'] == 'Z18']
        if len(bladeid_z18) > 0:
            bladeid_z18 = bladeid_z18.reset_index(drop=0)
            z18 = bladeid_z18['Severity'][0]
        else:
            z18 = 'NA'

        bladeid_z16 = df_blade[df_blade['Blade radious'] == 'Z16']
        if len(bladeid_z16) > 0:
            bladeid_z16 = bladeid_z16.reset_index(drop=0)
            z16 = bladeid_z16['Severity'][0]
        else:
            z16 = 'NA'

        bladeid_z14 = df_blade[df_blade['Blade radious'] == 'Z14']
        if len(bladeid_z14) > 0:
            bladeid_z14 = bladeid_z14.reset_index(drop=0)
            z14 = bladeid_z14['Severity'][0]
        else:
            z14 = 'NA'

        bladeid_z6 = df_blade[df_blade['Blade radious'] == 'Z6']
        if len(bladeid_z6) > 0:
            bladeid_z6 = bladeid_z6.reset_index(drop=0)
            z6 = bladeid_z6['Severity'][0]
        else:
            z6 = 'NA'

        bladeid_z13 = df_blade[df_blade['Blade radious'] == 'Z13']
        if len(bladeid_z13) > 0:
            bladeid_z13 = bladeid_z13.reset_index(drop=0)
            z13 = bladeid_z13['Severity'][0]
        else:
            z13 = 'NA'

        bladeid_z4 = df_blade[df_blade['Blade radious'] == 'Z4']
        if len(bladeid_z4) > 0:
            bladeid_z4 = bladeid_z4.reset_index(drop=0)

            z4 = bladeid_z4['Severity'][0]
        else:
            z4 = 'NA'

        bladeid_z16_6 = df_blade[df_blade['Blade radious'] == 'Z16.6']
        if len(bladeid_z16_6) > 0:
            bladeid_z16_6 = bladeid_z16_6.reset_index(drop=0)
            z16_6 = bladeid_z16_6['Severity'][0]
        else:
            z16_6 = 'NA'

        bladeid_z7 = df_blade[df_blade['Blade radious'] == 'Z7']
        if len(bladeid_z7) > 0:
            bladeid_z7 = bladeid_z7.reset_index(drop=0)
            z7 = bladeid_z7['Severity'][0]
        else:
            z7 = 'NA'

        bladeid_zother2 = df_blade[df_blade['Blade radious'] == 'Others (2)']
        if len(bladeid_zother2) > 0:
            bladeid_zother2 = bladeid_zother2.reset_index(drop=0)
            zother2 = bladeid_zother2['Severity'][0]
        else:
            zother2 = 'NA'

        bladeid_zother1 = df_blade[df_blade['Blade radious'] == 'Others (1)']
        if len(bladeid_zother1) > 0:
            bladeid_zother1 = bladeid_zother1.reset_index(drop=0)
            zother1 = bladeid_zother1['Severity'][0]
        else:
            zother1 = 'NA'

        bladeid_z10 = df_blade[df_blade['Blade radious'] == 'Z10']
        if len(bladeid_z10) > 0:
            bladeid_z10 = bladeid_z10.reset_index(drop=0)
            z10 = bladeid_z10['Severity'][0]
        else:
            z10 = 'NA'

        bladeid_z11_5 = df_blade[df_blade['Blade radious'] == 'Z11.5']
        if len(bladeid_z11_5) > 0:
            bladeid_z11_5 = bladeid_z11_5.reset_index(drop=0)
            z11_5 = bladeid_z11_5['Severity'][0]
        else:
            z11_5 = 'NA'

        bladeid_z11 = df_blade[df_blade['Blade radious'] == 'Z11']
        if len(bladeid_z11) > 0:
            bladeid_z11 = bladeid_z11.reset_index(drop=0)
            z11 = bladeid_z11['Severity'][0]
        else:
            z11 = 'NA'

        bladeid_z17 = df_blade[df_blade['Blade radious'] == 'Z17']
        if len(bladeid_z17) > 0:
            bladeid_z17 = bladeid_z17.reset_index(drop=0)
            z17 = bladeid_z17['Severity'][0]
        else:
            z17 = 'NA'

        bladeid_z20 = df_blade[df_blade['Blade radious'] == 'Z20']
        if len(bladeid_z20) > 0:
            bladeid_z20 = bladeid_z20.reset_index(drop=0)
            z20 = bladeid_z20['Severity'][0]
        else:
            z20 = 'NA'

        bladeid_z20_6 = df_blade[df_blade['Blade radious'] == 'Z20.6']
        if len(bladeid_z20_6) > 0:
            bladeid_z20_6 = bladeid_z20_6.reset_index(drop=0)
            z20_6 = bladeid_z20_6['Severity'][0]
        else:
            z20_6 = 'NA'
        bladeid_z15_5 = df_blade[df_blade['Blade radious'] == 'Z15.5']
        if len(bladeid_z15_5) > 0:
            bladeid_z15_5 = bladeid_z15_5.reset_index(drop=0)
            z15_5 = bladeid_z15_5['Severity'][0]
        else:
            z15_5 = 'NA'

        bladeid_z8 = df_blade[df_blade['Blade radious'] == 'Z8']
        if len(bladeid_z8) > 0:
            bladeid_z8 = bladeid_z8.reset_index(drop=0)
            z8 = bladeid_z8['Severity'][0]
        else:
            z8 = 'NA'
        resume = resume.append(
            {'BLADE ID': i, 'WF': wf, 'WTG': wtg, 'BLADE POS': pos, 'BLADE TYPE': type_b, 'MANUFACTURER': manufact,
             'SRN': srn, 'INSPECTION DATE': ins, 'SEVERITY(Z4)': z4, 'SEVERITY(Z8)': z8, 'SEVERITY(Z7)': z7,
             'SEVERITY(Z10)': z10, 'SEVERITY(Z13)': z13, 'SEVERITY(Z6)': z6, 'SEVERITY(Z11,5)': z11_5,
             'SEVERITY(Z15,5)': z15_5, 'SEVERITY(Z16,6)': z16_6, 'SEVERITY(Z19)': z19, 'SEVERITY(Z14)': z14,
             'SEVERITY(Z16)': z16, 'SEVERITY(Z11)': z11, 'SEVERITY(Z20)': z20, 'SEVERITY(Z20,6)': z20_6,
             'SEVERITY(Z17)': z17, 'SEVERITY(Z18)': z18, 'SEVERITY(OTHER01)': zother1, 'SEVERITY(OTHER02)': zother2,
             'BLADE SEVERITY (WORST)': sev_w, 'BLADE SEVERITY (ACUMULATED)': sev_g, 'NEXT INSPECTION': next,
             'REPAIRED': rep}, ignore_index=True)

    resume = resume.sort_values(by='WTG')
    filename_res = 'REPORT_RESUME_{}'.format(today_str)
    resume = resume.replace('0 - No defect', str(0))
    resume = resume.replace('1 - Initial', str(1))
    resume = resume.replace('2 - Intermediate', str(2))
    resume = resume.replace('3 - Advanced', str(3))
    resume = resume.replace('4 - Severe', str(4))
    resume = resume.replace('5 - Critical', str(5))
    T = resume['BLADE TYPE'].unique()
    if len(T) == 1:
        if 'AW68.7' in T:
            resume = resume[
                ['BLADE ID', 'WF', 'WTG', 'BLADE POS', 'BLADE TYPE', 'MANUFACTURER', 'SRN', 'INSPECTION DATE',
                 'SEVERITY(Z8)', 'SEVERITY(Z10)', 'SEVERITY(Z11)', 'SEVERITY(Z14)', 'SEVERITY(Z15,5)',
                 'SEVERITY(Z17)', 'SEVERITY(Z20)', 'SEVERITY(Z20,6)', 'SEVERITY(OTHER01)', 'SEVERITY(OTHER02)',
                 'BLADE SEVERITY (WORST)', 'BLADE SEVERITY (ACUMULATED)', 'NEXT INSPECTION', 'REPAIRED']]

    st.markdown(get_table_download_link(resume, filename_res), unsafe_allow_html=True)

    st.subheader('Resume table by windfarm')

    index = ['installed blades', '     inspected blades', '     inspected blades [% of installed]',
             '          NOT affected blades', '          Affected blades CUMULATIVE',
             '          Affected blades CUMULATIVE [% of inspected]', '               Repaired blades',
             '               Repaired blades [% of affected]', '               Affected blades NOW',
             '                    1 - Initial', '                    2 - Intermediate',
             '                    3 - Advanced', '                    4 - Severe',
             '                    5 - Critical', 'Blades meeting repair criteria',
             'Blades not affected but AR out of spec']


    reportwf = pd.DataFrame(index=index, columns=parque)
    st.header('by winfarm resume')
    for i in parque:
        df_cliente_info_wf = df_cliente_info[df_cliente_info['WindFarm']==i]
        df_tracasa_wf_manufacturer = df_tracasa_wf[df_tracasa_wf['WindFarm'] == i]
        df_tracasa_actual_manufacturer = df_tracasa_actual[df_tracasa_actual['WindFarm'] == i]
        if len(df_tracasa_wf_manufacturer) == 0:
            installed = 3*(df_cliente_info_wf['No. WTG'].sum())
            aux = [int(installed), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            aux = list(aux)
            reportwf['{}'.format(i)] = aux
        else:
            if len(df_cliente_info) == 1:
                installed = 3*(df_cliente_info_wf['No. WTG'].sum())
                inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                p_installed = round(((inspected / installed) * 100), 2)
                affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                    ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                affected_now = len(affected_now['BLADE ID'].unique())
                affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                    ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                affected_cum = len(affected_cum['BLADE ID'].unique())
                p_inspected = round(((affected_cum / inspected) * 100), 2)
                torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                    ['3 - Advanced', '4 - Severe', '5 - Critical'])]
                torepair = len(torepair['BLADE ID'].unique())
                repaired = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Repair'] == 'YES']
                repaired = len(repaired['BLADE ID'].unique())
                if affected_cum == 0:
                    p_affected = 0
                else:
                    p_affected = round(((repaired / affected_cum) * 100), 2)
                cero = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                cero = len(cero['BLADE ID'].unique())
                uno = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                uno = len(uno['BLADE ID'].unique())
                dos = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                dos = len(dos['BLADE ID'].unique())
                tres = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                tres = len(tres['BLADE ID'].unique())
                cuatro = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                cuatro = len(cuatro['BLADE ID'].unique())
                cinco = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                cinco = len(cinco['BLADE ID'].unique())

                AR_7 = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['AR'] < 8]
                AR_7 = AR_7[AR_7['Blade severity'] == '0 - No defect']
                AR_7 = AR_7['BLADE ID'].unique()
                AR_7 = len(AR_7)
                aux = [int(installed), inspected, p_installed, cero, affected_cum, p_inspected, repaired,
                       p_affected,
                       affected_now, uno, dos, tres, cuatro, cinco, torepair, AR_7]
                aux = list(aux)
                reportwf['{}'.format(i)] = aux
            elif len(df_cliente_info) > 1:

                installed = 3*(df_cliente_info_wf['No. WTG'].sum())
                inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                p_installed = round(((inspected / installed) * 100), 2)
                affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                    ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                affected_cum = len(affected_cum['BLADE ID'].unique())
                affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                    ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                affected_now = len(affected_now['BLADE ID'].unique())
                p_inspected = round(((affected_cum / inspected) * 100), 2)
                torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                    ['3 - Advanced', '4 - Severe', '5 - Critical'])]
                torepair = len(torepair['BLADE ID'].unique())
                repaired = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Repair'] == 'YES']
                repaired = len(repaired['BLADE ID'].unique())
                if affected_cum == 0:
                    p_affected = 0
                else:
                    p_affected = round(((repaired / affected_cum) * 100), 2)
                cero = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                cero = len(cero['BLADE ID'].unique())
                uno = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                uno = len(uno['BLADE ID'].unique())
                dos = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                dos = len(dos['BLADE ID'].unique())
                tres = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                tres = len(tres['BLADE ID'].unique())
                cuatro = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                cuatro = len(cuatro['BLADE ID'].unique())
                cinco = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                cinco = len(cinco['BLADE ID'].unique())
                AR_7 = df_tracasa_actual_manufacturer[
                    df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']

                AR_7 = AR_7[AR_7['AR'] < 8]
                AR_7 = AR_7['BLADE ID'].unique()
                AR_7 = len(AR_7)
                aux = [int(installed), inspected, p_installed, cero, affected_cum, p_inspected, repaired,
                       p_affected,
                       affected_now, uno, dos, tres, cuatro, cinco, torepair, AR_7]
                aux = list(aux)
                reportwf['{}'.format(i)] = aux

    installed_total = (df_cliente_info['TOTAL INSTALLED'].sum())
    inspected_total = reportwf.iloc[[1]].sum()
    inspected_total = inspected_total.sum()
    por_isnpected = round((inspected_total / installed_total) * 100, 2)
    not_affected_total = reportwf.iloc[[3]].sum()
    not_affected_total = not_affected_total.sum()
    affected_cumu = reportwf.iloc[[4]].sum()
    affected_cumu = affected_cumu.sum()
    por_effected = round((affected_cumu / inspected_total) * 100, 2)
    repaired_total = reportwf.iloc[[6]].sum()
    repaired_total = repaired_total.sum()
    por_reppaired = round((repaired_total / affected_cumu) * 100,2)
    afected_now_total = reportwf.iloc[[8]].sum()
    afected_now_total = afected_now_total.sum()
    uno_total = reportwf.iloc[[9]].sum()
    uno_total = uno_total.sum()
    dos_total = reportwf.iloc[[10]].sum()
    dos_total = dos_total.sum()
    tres_total = reportwf.iloc[[11]].sum()
    tres_total = tres_total.sum()
    cuatro_total = reportwf.iloc[[12]].sum()
    cuatro_total = cuatro_total.sum()
    cinco_total = reportwf.iloc[[13]].sum()
    cinco_total = cinco_total.sum()
    AR_7_total = reportwf.iloc[[15]].sum()
    AR_7_total = AR_7_total.sum()
    torepair_tot = reportwf.iloc[[14]].sum()
    torepair_tot = torepair_tot.sum()
    total = [installed_total, inspected_total, por_isnpected, not_affected_total, affected_cumu, por_effected,
             repaired_total, por_reppaired, afected_now_total, uno_total, dos_total, tres_total, cuatro_total,
             cinco_total, torepair_tot, AR_7_total]
    fig = go.Figure(data=[go.Pie(
        labels=['0 - No defect', '1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'],
        values=[not_affected_total, uno_total, dos_total, tres_total, cuatro_total, cinco_total], hole=.4,
        marker_colors=['#2ca02c', 'blue', 'yellow', 'orange', 'red', 'black'], sort=False)])
    fig.update_layout(
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="RebeccaPurple"
        )
    )
    reportwf['TOTAL'] = total

    color_discrete_map = {'IND': 'rgb(42,9,4)', 'NBM': 'rgb(111,203,209)', 'NBS': 'rgb(55,165,172),',
                          'TMT': 'rgb(29,127,136)', 'TPI': 'rgb(2,84,92)'}
    fig22 = ff.create_table(reportwf, index=index, index_title=today_str)
    fig22.update_layout(width=2200, height=800)
    st.plotly_chart(fig22)
    filename_reportwf = 'WF_REPORT_CLIENT'
    st.markdown(get_table_download_link(reportwf, filename_reportwf), unsafe_allow_html=True)





    st.dataframe(resume)
    with st.expander('view last state data'):
        st.markdown(get_table_download_link(df_tracasa_actual, filename_r), unsafe_allow_html=True)
        st.dataframe(df_tracasa_actual)

def REPORT_GENERATION(parque,df_tracasa,df_cliente_info,df_tracasa_actual,palas_quitar):
    df_tracasa = df_tracasa.replace('IND (INDUTCH)', 'IND')
    df_tracasa = df_tracasa.replace('NBM (CONCAMEX)', 'NBM')
    df_tracasa = df_tracasa.replace('NBS (ACCIONA BLADES)', 'NBS')
    df_tracasa = df_tracasa.replace('AER (AERIS)', 'AER')
    df_tracasa_actual = df_tracasa_actual.replace('IND (INDUTCH)', 'IND')
    df_tracasa_actual = df_tracasa_actual.replace('NBM (CONCAMEX)', 'NBM')
    df_tracasa_actual = df_tracasa_actual.replace('NBS (ACCIONA BLADES)', 'NBS')
    df_tracasa_actual = df_tracasa_actual.replace('AER (AERIS)', 'AER')
    today = date.today()
    today_str = str(today)

    if parque != '<all>' and parque != 'HIGH LONESOME':

        if parque == 'PRAIRIE HILL ':
            parque = parque[:12]
        palas_quitar = palas_quitar[palas_quitar['Windfarm']==parque]
        palas_quitar = list(palas_quitar['Old blade'].unique())
        df_cliente_info = df_cliente_info[df_cliente_info['WindFarm'] == parque]

        df_tracasa_actual = df_tracasa_actual[df_tracasa_actual['WindFarm']==parque]
        df_tracasa_actual = df_tracasa_actual.reset_index(drop=True)
        df_cliente_info = df_cliente_info.reset_index(drop=True)
        df_tracasa_wf = df_tracasa
        df_tracasa_wf = df_tracasa_wf[df_tracasa_wf['WindFarm']==parque]
        df_tracasa_wf = df_tracasa_wf.reset_index(drop=True)
        ANOTACION = df_tracasa_wf['ANNOTATION_WF'][0]
        index = ['installed blades','     inspected blades','     inspected blades [% of installed]','          NOT affected blades','          Affected blades CUMULATIVE','          Affected blades CUMULATIVE [% of inspected]','               Repaired blades','               Repaired blades [% of affected]','               Affected blades NOW','                    1 - Initial','                    2 - Intermediate','                    3 - Advanced','                    4 - Severe','                    5 - Critical','Blades meeting repair criteria','Blades not affected but AR out of spec']
        MANUFACTURER_LIST = sorted(list(df_tracasa['Manufacturer'].unique()))

        report = pd.DataFrame(index=index,columns=MANUFACTURER_LIST)
        st.header('{}'.format(parque))
        for i in MANUFACTURER_LIST:

            df_tracasa_wf_manufacturer = df_tracasa_wf[df_tracasa_wf['Manufacturer'] == i]
            df_tracasa_actual_manufacturer = df_tracasa_actual[df_tracasa_actual['Manufacturer'] == i]
            if len(df_tracasa_wf_manufacturer) == 0:
                installed = df_cliente_info['{}'.format(i)][0]
                aux = [int(installed), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                aux = list(aux)
                report['{}'.format(i)] = aux
            else:
                if len(df_cliente_info) == 1:
                    installed = df_cliente_info['{}'.format(i)][0]
                    inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                    p_installed = round(((inspected / installed) * 100),2)
                    affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(['1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe', '5 - Critical'])]
                    affected_now = len(affected_now['BLADE ID'].unique())
                    affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                        ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                    affected_cum = len(affected_cum['BLADE ID'].unique())
                    p_inspected = round(((affected_cum / inspected) * 100),2)
                    torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(['3 - Advanced','4 - Severe', '5 - Critical'])]
                    torepair = len(torepair['BLADE ID'].unique())
                    repaired = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Repair'] == 'YES']
                    repaired = len(repaired['BLADE ID'].unique())
                    if affected_cum == 0:
                        p_affected = 0
                    else:
                        p_affected = round(((repaired / affected_cum) * 100),2)
                    cero = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                    cero = len(cero['BLADE ID'].unique())
                    uno = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                    uno = len(uno['BLADE ID'].unique())
                    dos = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                    dos = len(dos['BLADE ID'].unique())
                    tres = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                    tres = len(tres['BLADE ID'].unique())
                    cuatro = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                    cuatro = len(cuatro['BLADE ID'].unique())
                    cinco = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                    cinco = len(cinco['BLADE ID'].unique())

                    AR_7 = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['AR']<8]
                    AR_7 = AR_7[AR_7['Blade severity'] == '0 - No defect']
                    AR_7 = AR_7['BLADE ID'].unique()
                    AR_7 = len(AR_7)
                    aux = [int(installed), inspected, p_installed,cero, affected_cum, p_inspected, repaired, p_affected,
                           affected_now, uno, dos, tres, cuatro,cinco,torepair,AR_7]
                    aux = list(aux)
                    report['{}'.format(i)] = aux
                elif len(df_cliente_info) > 1:

                    installed = df_cliente_info['{}'.format(i)].sum()
                    inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                    p_installed = round(((inspected / installed) * 100),2)
                    affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                        ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                    affected_cum = len(affected_cum['BLADE ID'].unique())
                    affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                        ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                    affected_now = len(affected_now['BLADE ID'].unique())
                    p_inspected = round(((affected_cum / inspected) * 100),2)
                    torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                        ['3 - Advanced', '4 - Severe', '5 - Critical'])]
                    torepair = len(torepair['BLADE ID'].unique())
                    repaired = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Repair'] == 'Yes']
                    repaired = len(repaired['BLADE ID'].unique())
                    if affected_cum == 0:
                        p_affected = 0
                    else:
                        p_affected = round(((repaired / affected_cum) * 100), 2)
                    cero = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                    cero = len(cero['BLADE ID'].unique())
                    uno = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                    uno = len(uno['BLADE ID'].unique())
                    dos = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                    dos = len(dos['BLADE ID'].unique())
                    tres = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                    tres = len(tres['BLADE ID'].unique())
                    cuatro = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                    cuatro = len(cuatro['BLADE ID'].unique())
                    cinco = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                    cinco = len(cinco['BLADE ID'].unique())
                    AR_7 = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']

                    AR_7 = AR_7[AR_7['AR'] < 8]
                    AR_7 = AR_7['BLADE ID'].unique()
                    AR_7 = len(AR_7)
                    aux = [int(installed), inspected, p_installed, cero, affected_cum, p_inspected, repaired,
                           p_affected,
                           affected_now, uno, dos, tres, cuatro, cinco, torepair, AR_7]
                    aux = list(aux)
                    report['{}'.format(i)] = aux

        installed_total = (df_cliente_info['TOTAL INSTALLED'].sum())
        inspected_total = report.iloc[[1]].sum()
        inspected_total = inspected_total.sum()
        por_isnpected = round((inspected_total/installed_total)*100,2)
        not_affected_total = report.iloc[[3]].sum()
        not_affected_total = not_affected_total.sum()
        affected_cumu = report.iloc[[4]].sum()
        affected_cumu = affected_cumu.sum()
        por_effected = round((affected_cumu/inspected_total)*100,2)
        repaired_total = report.iloc[[6]].sum()
        repaired_total = repaired_total.sum()
        por_reppaired = round((repaired_total / affected_cumu) * 100,2)
        afected_now_total = report.iloc[[8]].sum()
        afected_now_total = afected_now_total.sum()
        uno_total = report.iloc[[9]].sum()
        uno_total = uno_total.sum()
        dos_total = report.iloc[[10]].sum()
        dos_total = dos_total.sum()
        tres_total = report.iloc[[11]].sum()
        tres_total = tres_total.sum()
        cuatro_total = report.iloc[[12]].sum()
        cuatro_total = cuatro_total.sum()
        cinco_total = report.iloc[[13]].sum()
        cinco_total = cinco_total.sum()
        AR_7_total = report.iloc[[15]].sum()
        AR_7_total = AR_7_total.sum()
        torepair_tot = report.iloc[[14]].sum()
        torepair_tot = torepair_tot.sum()
        total = [installed_total,inspected_total,por_isnpected,not_affected_total,affected_cumu,por_effected,repaired_total,por_reppaired,afected_now_total,uno_total,dos_total,tres_total,cuatro_total,cinco_total,torepair_tot,AR_7_total]
        fig = go.Figure(data=[go.Pie(labels=['0 - No defect','1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe','5 - Critical'], values=[not_affected_total,uno_total,dos_total,tres_total,cuatro_total,cinco_total], hole=.4, marker_colors=['#2ca02c','blue','yellow','orange','red','black'], sort=False)])
        fig.update_layout(
            title=parque+'_'+today_str,
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
            )
        )
        report['TOTAL'] = total


        color_discrete_map = {'IND': 'rgb(42,9,4)','NBM': 'rgb(111,203,209)','NBS': 'rgb(55,165,172),','TMT': 'rgb(29,127,136)','TPI': 'rgb(2,84,92)'}
        fig2 = ff.create_table(report,index=index,index_title=parque+'_'+today_str)
        report2 = report
        today = date.today()
        today_str = str(today)
        filename_r = 'REPORT_{}'.format(today_str)
        if not pd.isnull(ANOTACION):
            txt = st.text_area('WindFarm annotation','''{}'''.format(ANOTACION),max_chars=999999999)
            with st.expander('Read Me'):
                st.write(txt)
        report['index'] = report.index
        cols = report.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        report = report[cols]
        st.markdown(get_table_download_link(report, filename_r), unsafe_allow_html=True)
        del report['index']
        fig.update_layout(width=1000, height=800)
        fig2.update_layout(width=2200, height=800)
        st.plotly_chart(fig2)

        st.plotly_chart(fig)
        report_ratios = report
        report = report.drop(['     inspected blades [% of installed]',
                 '          NOT affected blades',
                 '          Affected blades CUMULATIVE [% of inspected]',
                 '               Repaired blades [% of affected]', '               Affected blades NOW',
                 '                    1 - Initial', '                    2 - Intermediate',
                 '                    3 - Advanced', '                    4 - Severe', '                    5 - Critical',
                 'Blades meeting repair criteria', 'Blades not affected but AR out of spec'])

        del report['TOTAL']
        report_ratios = report_ratios.drop(['installed blades', '     inspected blades [% of installed]',
                 '          NOT affected blades', '          Affected blades CUMULATIVE',
                 '          Affected blades CUMULATIVE [% of inspected]', '               Repaired blades',
                 '               Repaired blades [% of affected]', '               Affected blades NOW',
                 'Blades meeting repair criteria', 'Blades not affected but AR out of spec'])

        del report_ratios['TOTAL']

        inspected_list = report_ratios.iloc[0]
        uno_list = report_ratios.iloc[1]
        dos_list = report_ratios.iloc[2]
        tres_list = report_ratios.iloc[3]
        cuatro_list = report_ratios.iloc[4]
        cinco_list = report_ratios.iloc[5]

        uno_list_ratio = 100*(uno_list/inspected_list)
        dos_list_ratio = 100 * (dos_list / inspected_list)
        tres_list_ratio = 100 * (tres_list / inspected_list)
        cuatro_list_ratio = 100 * (cuatro_list / inspected_list)
        cinco_list_ratio = 100 * (cinco_list / inspected_list)

        report_ratios = report_ratios.drop(['     inspected blades','                    1 - Initial', '                    2 - Intermediate',
                 '                    3 - Advanced', '                    4 - Severe', '                    5 - Critical'])

        report_ratios = report_ratios.append(uno_list_ratio,ignore_index=True)
        report_ratios = report_ratios.append(dos_list_ratio, ignore_index=True)
        report_ratios = report_ratios.append(tres_list_ratio, ignore_index=True)
        report_ratios = report_ratios.append(cuatro_list_ratio, ignore_index=True)
        report_ratios = report_ratios.append(cinco_list_ratio, ignore_index=True)

        report_ratios.set_axis(['1 - %', '2 - %', '3 - %','4 - %','5 - %'])


        report_ratios = report_ratios.T
        report_ratios.columns = ['1 - %', '2 - %', '3 - %','4 - %','5 - %']
        color_discrete_map_sev = {'1 - %':'blue', '2 - %':'yellow', '3 - %':'orange','4 - %':'red','5 - %':'black'}

        fig4 = px.bar(report_ratios, title=parque + '_' + today_str,color_discrete_map=color_discrete_map_sev)

        fig4.update_layout(width=1000, height=800)
        fig3 = px.bar(report,title=parque+'_'+today_str)


        fig3.update_layout(width=1000, height=800)
        st.plotly_chart(fig3)
        st.plotly_chart(fig4)

        df_rep = df_tracasa[df_tracasa['WindFarm']==parque]
        df_tracasa_actual_wf = df_tracasa_actual[df_tracasa_actual['WindFarm']==parque]
        blades_id = sorted(list(df_tracasa_actual_wf['BLADE ID'].unique()))
        resume = pd.DataFrame(columns=['BLADE ID','WF','WTG','BLADE POS','BLADE TYPE','MANUFACTURER','SRN','INSPECTION DATE','SEVERITY(Z4)','SEVERITY(Z8)','SEVERITY(Z7)','SEVERITY(Z10)','SEVERITY(Z13)','SEVERITY(Z6)','SEVERITY(Z11,5)','SEVERITY(Z16,6)','SEVERITY(Z15,5)','SEVERITY(Z19)','SEVERITY(Z14)','SEVERITY(Z16)','SEVERITY(Z11)','SEVERITY(Z20)','SEVERITY(Z20,6)','SEVERITY(Z17)','SEVERITY(Z18)','SEVERITY(OTHER01)','SEVERITY(OTHER02)','BLADE SEVERITY (WORST)','BLADE SEVERITY (ACUMULATED)','NEXT INSPECTION','REPAIRED'])
        for i in blades_id:
            df_blade = df_tracasa_actual_wf[df_tracasa_actual_wf['BLADE ID']==i]
            df_blade = df_blade.reset_index(drop=True)
            wf= df_blade['WindFarm'][0]
            wtg = df_blade['WTG'][0]
            pos = df_blade['Blade position'][0]
            type_b = df_blade['Blade type'][0]
            manufact = df_blade['Manufacturer'][0]
            srn = df_blade['SRN'][0]
            ins = df_blade['inspection_date'][0]
            sev_w = df_blade['Blade severity'][0]
            sev_g = df_blade['GLOBAL Blade severity'][0]
            next = df_blade['PROXIMA_INSP'][0]

            df_blade_rep = df_rep[df_rep['BLADE ID'] == i]
            df_blade_rep = df_blade_rep.reset_index(drop=True)
            df_blade_rep = df_blade_rep[df_blade_rep['Repair'] == 'YES']
            if len(df_blade_rep)==0:
                rep ='NO'
            else:
                rep = 'YES'

            bladeid_z19 = df_blade[df_blade['Blade radious'] == 'Z19']
            if len(bladeid_z19) > 0:
                bladeid_z19 = bladeid_z19.reset_index(drop=0)
                z19 = bladeid_z19['Severity'][0]
            else:
                z19 = 'NA'

            bladeid_z18 = df_blade[df_blade['Blade radious'] == 'Z18']
            if len(bladeid_z18) > 0:
                bladeid_z18 = bladeid_z18.reset_index(drop=0)
                z18 = bladeid_z18['Severity'][0]
            else:
                z18 = 'NA'

            bladeid_z16 = df_blade[df_blade['Blade radious'] == 'Z16']
            if len(bladeid_z16) > 0:
                bladeid_z16 = bladeid_z16.reset_index(drop=0)
                z16 = bladeid_z16['Severity'][0]
            else:
                z16 = 'NA'

            bladeid_z14 = df_blade[df_blade['Blade radious'] == 'Z14']
            if len(bladeid_z14) > 0:
                bladeid_z14 = bladeid_z14.reset_index(drop=0)
                z14 = bladeid_z14['Severity'][0]
            else:
                z14 = 'NA'

            bladeid_z6 = df_blade[df_blade['Blade radious'] == 'Z6']
            if len(bladeid_z6) > 0:
                bladeid_z6 = bladeid_z6.reset_index(drop=0)
                z6 = bladeid_z6['Severity'][0]
            else:
                z6 = 'NA'

            bladeid_z13 = df_blade[df_blade['Blade radious'] == 'Z13']
            if len(bladeid_z13) > 0:
                bladeid_z13 = bladeid_z13.reset_index(drop=0)
                z13 = bladeid_z13['Severity'][0]
            else:
                z13 = 'NA'

            bladeid_z4 = df_blade[df_blade['Blade radious'] == 'Z4']
            if len(bladeid_z4) > 0:
                bladeid_z4 = bladeid_z4.reset_index(drop=0)

                z4 = bladeid_z4['Severity'][0]
            else:
                z4 = 'NA'

            bladeid_z16_6 = df_blade[df_blade['Blade radious'] == 'Z16.6']
            if len(bladeid_z16_6) > 0:
                bladeid_z16_6 = bladeid_z16_6.reset_index(drop=0)
                z16_6 = bladeid_z16_6['Severity'][0]
            else:
                z16_6 = 'NA'

            bladeid_z7 = df_blade[df_blade['Blade radious'] == 'Z7']
            if len(bladeid_z7) > 0:
                bladeid_z7 = bladeid_z7.reset_index(drop=0)
                z7 = bladeid_z7['Severity'][0]
            else:
                z7 = 'NA'

            bladeid_zother2 = df_blade[df_blade['Blade radious'] == 'Others (2)']
            if len(bladeid_zother2) > 0:
                bladeid_zother2 = bladeid_zother2.reset_index(drop=0)
                zother2 = bladeid_zother2['Severity'][0]
            else:
                zother2 = 'NA'

            bladeid_zother1 = df_blade[df_blade['Blade radious'] == 'Others (1)']
            if len(bladeid_zother1) > 0:
                bladeid_zother1 = bladeid_zother1.reset_index(drop=0)
                zother1 = bladeid_zother1['Severity'][0]
            else:
                zother1 = 'NA'

            bladeid_z10 = df_blade[df_blade['Blade radious'] == 'Z10']
            if len(bladeid_z10) > 0:
                bladeid_z10 = bladeid_z10.reset_index(drop=0)
                z10 = bladeid_z10['Severity'][0]
            else:
                z10 = 'NA'

            bladeid_z11_5 = df_blade[df_blade['Blade radious'] == 'Z11.5']
            if len(bladeid_z11_5) > 0:
                bladeid_z11_5 = bladeid_z11_5.reset_index(drop=0)
                z11_5 = bladeid_z11_5['Severity'][0]
            else:
                z11_5 = 'NA'

            bladeid_z11 = df_blade[df_blade['Blade radious'] == 'Z11']
            if len(bladeid_z11) > 0:
                bladeid_z11 = bladeid_z11.reset_index(drop=0)
                z11 = bladeid_z11['Severity'][0]
            else:
                z11 = 'NA'

            bladeid_z17 = df_blade[df_blade['Blade radious'] == 'Z17']
            if len(bladeid_z17) > 0:
                bladeid_z17 = bladeid_z17.reset_index(drop=0)
                z17 = bladeid_z17['Severity'][0]
            else:
                z17 = 'NA'

            bladeid_z20 = df_blade[df_blade['Blade radious'] == 'Z20']
            if len(bladeid_z20) > 0:
                bladeid_z20 = bladeid_z20.reset_index(drop=0)
                z20 = bladeid_z20['Severity'][0]
            else:
                z20 = 'NA'

            bladeid_z20_6 = df_blade[df_blade['Blade radious'] == 'Z20.6']
            if len(bladeid_z20_6) > 0:
                bladeid_z20_6 = bladeid_z20_6.reset_index(drop=0)
                z20_6 = bladeid_z20_6['Severity'][0]
            else:
                z20_6 = 'NA'
            bladeid_z15_5 = df_blade[df_blade['Blade radious'] == 'Z15.5']
            if len(bladeid_z15_5) > 0:
                bladeid_z15_5 = bladeid_z15_5.reset_index(drop=0)
                z15_5 = bladeid_z15_5['Severity'][0]
            else:
                z15_5 = 'NA'

            bladeid_z8 = df_blade[df_blade['Blade radious'] == 'Z8']
            if len(bladeid_z8) > 0:
                bladeid_z8 = bladeid_z8.reset_index(drop=0)
                z8 = bladeid_z8['Severity'][0]
            else:
                z8 = 'NA'
            resume = resume.append({'BLADE ID':i,'WF':wf,'WTG':wtg,'BLADE POS':pos,'BLADE TYPE':type_b,'MANUFACTURER':manufact,'SRN':srn,'INSPECTION DATE':ins,'SEVERITY(Z4)':z4,'SEVERITY(Z8)':z8,'SEVERITY(Z7)':z7,'SEVERITY(Z10)':z10,'SEVERITY(Z13)':z13,'SEVERITY(Z6)':z6,'SEVERITY(Z11,5)':z11_5,'SEVERITY(Z15,5)':z15_5,'SEVERITY(Z16,6)':z16_6,'SEVERITY(Z19)':z19,'SEVERITY(Z14)':z14,'SEVERITY(Z16)':z16,'SEVERITY(Z11)':z11,'SEVERITY(Z20)':z20,'SEVERITY(Z20,6)':z20_6,'SEVERITY(Z17)':z17,'SEVERITY(Z18)':z18,'SEVERITY(OTHER01)':zother1,'SEVERITY(OTHER02)':zother2,'BLADE SEVERITY (WORST)':sev_w,'BLADE SEVERITY (ACUMULATED)':sev_g,'NEXT INSPECTION':next,'REPAIRED':rep},ignore_index=True)


        resume = resume.sort_values(by='WTG')
        filename_res = '{}_{}'.format(parque,today_str)
        resume = resume.replace('0 - No defect', str(0))
        resume = resume.replace('1 - Initial', str(1))
        resume = resume.replace('2 - Intermediate', str(2))
        resume = resume.replace('3 - Advanced', str(3))
        resume = resume.replace('4 - Severe', str(4))
        resume = resume.replace('5 - Critical', str(5))
        T = resume['BLADE TYPE'].unique()
        if len(T) == 1:
            if 'AW68.7' in T:
                resume = resume[['BLADE ID','WF','WTG','BLADE POS','BLADE TYPE','MANUFACTURER','SRN','INSPECTION DATE','SEVERITY(Z8)','SEVERITY(Z10)','SEVERITY(Z11)','SEVERITY(Z14)','SEVERITY(Z15,5)','SEVERITY(Z17)','SEVERITY(Z20)','SEVERITY(Z20,6)','SEVERITY(OTHER01)','SEVERITY(OTHER02)','BLADE SEVERITY (WORST)','BLADE SEVERITY (ACUMULATED)','NEXT INSPECTION','REPAIRED']]
        else:
            resume = resume[['BLADE ID', 'WF', 'WTG', 'BLADE POS', 'BLADE TYPE', 'MANUFACTURER',
                 'SRN', 'INSPECTION DATE', 'SEVERITY(Z4)', 'SEVERITY(Z7)', 'SEVERITY(Z10)',
                 'SEVERITY(Z13)', 'SEVERITY(Z16)', 'SEVERITY(Z19)', 'SEVERITY(Z6)',
                 'SEVERITY(Z11,5)', 'SEVERITY(Z16,6)', 'SEVERITY(Z8)', 'SEVERITY(Z11)',
                 'SEVERITY(Z14)', 'SEVERITY(Z17)', 'SEVERITY(Z20)', 'SEVERITY(Z15,5)',
                 'SEVERITY(Z20,6)', 'SEVERITY(OTHER01)', 'SEVERITY(OTHER02)',
                 'BLADE SEVERITY (WORST)', 'BLADE SEVERITY (ACUMULATED)', 'NEXT INSPECTION',
                 'REPAIRED']]
        resume = resume.sort_values(by=['WF','WTG','BLADE POS'])
        total_palas = list(resume['BLADE ID'].unique())

        for pala in palas_quitar:
            if pala not in total_palas:
                palas_quitar.remove(pala)

        for item in palas_quitar:

            id_index = resume.index[resume['BLADE ID'] == item]

            resume = resume.drop(index = id_index)
        resume['ID'] = resume.index
        st.markdown(get_table_download_report(resume, filename_res,ANOTACION,fig,fig3,fig4,report2), unsafe_allow_html=True)

        st.dataframe(resume)


        with st.expander('view last state data'):
            st.markdown(get_table_download_link(df_tracasa_actual, filename_r), unsafe_allow_html=True)
            st.dataframe(df_tracasa_actual)
    elif parque != '<all>' and parque == 'HIGH LONESOME':


        df_cliente_info = df_cliente_info[df_cliente_info['WindFarm'] == parque]

        df_tracasa_actual = df_tracasa_actual[df_tracasa_actual['WindFarm']==parque]
        df_tracasa_actual = df_tracasa_actual.reset_index(drop=True)
        df_cliente_info = df_cliente_info.reset_index(drop=True)
        df_tracasa_wf_aux = df_tracasa
        df_tracasa_wf_aux = df_tracasa_wf_aux[df_tracasa_wf_aux['WindFarm']==parque]
        df_tracasa_wf_aux = df_tracasa_wf_aux.reset_index(drop=True)
        index = ['installed blades','     inspected blades','     inspected blades [% of installed]','          NOT affected blades','          Affected blades CUMULATIVE','          Affected blades CUMULATIVE [% of inspected]','               Repaired blades','               Repaired blades [% of affected]','               Affected blades NOW','                    1 - Initial','                    2 - Intermediate','                    3 - Advanced','                    4 - Severe','                    5 - Critical','Blades meeting repair criteria','Blades not affected but AR out of spec']
        palas = df_tracasa_wf_aux['Blade type'].unique()
        for p in palas:
            st.write(p)
            df_tracasa_wf = df_tracasa_wf_aux[df_tracasa_wf_aux['Blade type']==p]
            df_tracasa_actual_aux = df_tracasa_actual[df_tracasa_actual['Blade type']==p]
            MANUFACTURER_LIST = sorted(list(df_tracasa['Manufacturer'].unique()))

            report = pd.DataFrame(index=index,columns=MANUFACTURER_LIST)
            st.header('{}'.format(parque))
            for i in MANUFACTURER_LIST:

                df_tracasa_wf_manufacturer = df_tracasa_wf[df_tracasa_wf['Manufacturer'] == i]
                df_tracasa_actual_manufacturer = df_tracasa_actual_aux[df_tracasa_actual_aux['Manufacturer'] == i]
                if len(df_tracasa_wf_manufacturer) == 0:
                    installed = df_cliente_info['{}'.format(i)][0]
                    aux = [int(installed), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                    aux = list(aux)
                    report['{}'.format(i)] = aux
                else:
                    if len(df_cliente_info) == 1:
                        installed = df_cliente_info['{}'.format(i)][0]
                        inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                        p_installed = round(((inspected / installed) * 100),2)
                        affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(['1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe', '5 - Critical'])]
                        affected_now = len(affected_now['BLADE ID'].unique())
                        affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                            ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                        affected_cum = len(affected_cum['BLADE ID'].unique())
                        p_inspected = round(((affected_cum / inspected) * 100),2)
                        torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(['3 - Advanced','4 - Severe', '5 - Critical'])]
                        torepair = len(torepair['BLADE ID'].unique())
                        repaired = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Repair'] == 'YES']
                        repaired = len(repaired['BLADE ID'].unique())
                        if affected_cum == 0:
                            p_affected = 0
                        else:
                            p_affected = round(((repaired / affected_cum) * 100),2)
                        cero = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                        cero = len(cero['BLADE ID'].unique())
                        uno = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                        uno = len(uno['BLADE ID'].unique())
                        dos = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                        dos = len(dos['BLADE ID'].unique())
                        tres = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                        tres = len(tres['BLADE ID'].unique())
                        cuatro = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                        cuatro = len(cuatro['BLADE ID'].unique())
                        cinco = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                        cinco = len(cinco['BLADE ID'].unique())

                        AR_7 = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['AR']<8]
                        AR_7 = AR_7[AR_7['Blade severity'] == '0 - No defect']
                        AR_7 = AR_7['BLADE ID'].unique()
                        AR_7 = len(AR_7)
                        aux = [int(installed), inspected, p_installed,cero, affected_cum, p_inspected, repaired, p_affected,
                               affected_now, uno, dos, tres, cuatro,cinco,torepair,AR_7]
                        aux = list(aux)
                        report['{}'.format(i)] = aux
                    elif len(df_cliente_info) > 1:

                        installed = df_cliente_info['{}'.format(i)].sum()
                        inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                        p_installed = round(((inspected / installed) * 100),2)
                        affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                            ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                        affected_cum = len(affected_cum['BLADE ID'].unique())
                        affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                            ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                        affected_now = len(affected_now['BLADE ID'].unique())
                        p_inspected = round(((affected_cum / inspected) * 100),2)
                        torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                            ['3 - Advanced', '4 - Severe', '5 - Critical'])]
                        torepair = len(torepair['BLADE ID'].unique())
                        repaired = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Repair'] == 'Yes']
                        repaired = len(repaired['BLADE ID'].unique())
                        if affected_cum == 0:
                            p_affected = 0
                        else:
                            p_affected = round(((repaired / affected_cum) * 100), 2)
                        cero = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                        cero = len(cero['BLADE ID'].unique())
                        uno = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                        uno = len(uno['BLADE ID'].unique())
                        dos = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                        dos = len(dos['BLADE ID'].unique())
                        tres = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                        tres = len(tres['BLADE ID'].unique())
                        cuatro = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                        cuatro = len(cuatro['BLADE ID'].unique())
                        cinco = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                        cinco = len(cinco['BLADE ID'].unique())
                        AR_7 = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']

                        AR_7 = AR_7[AR_7['AR'] < 8]
                        AR_7 = AR_7['BLADE ID'].unique()
                        AR_7 = len(AR_7)
                        aux = [int(installed), inspected, p_installed, cero, affected_cum, p_inspected, repaired,
                               p_affected,
                               affected_now, uno, dos, tres, cuatro, cinco, torepair, AR_7]
                        aux = list(aux)
                        report['{}'.format(i)] = aux

            installed_total = (df_cliente_info['TOTAL INSTALLED'].sum())
            inspected_total = report.iloc[[1]].sum()
            inspected_total = inspected_total.sum()
            por_isnpected = round((inspected_total/installed_total)*100,2)
            not_affected_total = report.iloc[[3]].sum()
            not_affected_total = not_affected_total.sum()
            affected_cumu = report.iloc[[4]].sum()
            affected_cumu = affected_cumu.sum()
            por_effected = round((affected_cumu/inspected_total)*100,2)
            repaired_total = report.iloc[[6]].sum()
            repaired_total = repaired_total.sum()
            por_reppaired = round((repaired_total / affected_cumu) * 100,2)
            afected_now_total = report.iloc[[8]].sum()
            afected_now_total = afected_now_total.sum()
            uno_total = report.iloc[[9]].sum()
            uno_total = uno_total.sum()
            dos_total = report.iloc[[10]].sum()
            dos_total = dos_total.sum()
            tres_total = report.iloc[[11]].sum()
            tres_total = tres_total.sum()
            cuatro_total = report.iloc[[12]].sum()
            cuatro_total = cuatro_total.sum()
            cinco_total = report.iloc[[13]].sum()
            cinco_total = cinco_total.sum()
            AR_7_total = report.iloc[[15]].sum()
            AR_7_total = AR_7_total.sum()
            torepair_tot = report.iloc[[14]].sum()
            torepair_tot = torepair_tot.sum()
            total = [installed_total,inspected_total,por_isnpected,not_affected_total,affected_cumu,por_effected,repaired_total,por_reppaired,afected_now_total,uno_total,dos_total,tres_total,cuatro_total,cinco_total,torepair_tot,AR_7_total]
            fig = go.Figure(data=[go.Pie(labels=['0 - No defect','1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe','5 - Critical'], values=[not_affected_total,uno_total,dos_total,tres_total,cuatro_total,cinco_total], hole=.4, marker_colors=['#2ca02c','blue','yellow','orange','red','black'], sort=False)])
            fig.update_layout(
                title=parque+'_'+today_str,
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="RebeccaPurple"
                )
            )
            report['TOTAL'] = total


            color_discrete_map = {'IND': 'rgb(42,9,4)','NBM': 'rgb(111,203,209)','NBS': 'rgb(55,165,172),','TMT': 'rgb(29,127,136)','TPI': 'rgb(2,84,92)'}
            fig2 = ff.create_table(report,index=index,index_title=parque+'_'+today_str)
            today = date.today()
            today_str = str(today)
            filename_r = 'REPORT_{}'.format(today_str)
            report['index'] = report.index
            cols = report.columns.tolist()
            cols = cols[-1:] + cols[:-1]
            report = report[cols]
            st.markdown(get_table_download_link(report, filename_r), unsafe_allow_html=True)
            del report['index']
            fig.update_layout(width=1000, height=800)
            fig2.update_layout(width=2200, height=800)
            st.plotly_chart(fig2)

            st.plotly_chart(fig)
            report_ratios = report
            report = report.drop(['     inspected blades [% of installed]',
                     '          NOT affected blades',
                     '          Affected blades CUMULATIVE [% of inspected]',
                     '               Repaired blades [% of affected]', '               Affected blades NOW',
                     '                    1 - Initial', '                    2 - Intermediate',
                     '                    3 - Advanced', '                    4 - Severe', '                    5 - Critical',
                     'Blades meeting repair criteria', 'Blades not affected but AR out of spec'])

            del report['TOTAL']
            report_ratios = report_ratios.drop(['installed blades', '     inspected blades [% of installed]',
                     '          NOT affected blades', '          Affected blades CUMULATIVE',
                     '          Affected blades CUMULATIVE [% of inspected]', '               Repaired blades',
                     '               Repaired blades [% of affected]', '               Affected blades NOW',
                     'Blades meeting repair criteria', 'Blades not affected but AR out of spec'])
            del report_ratios['TOTAL']

            inspected_list = report_ratios.iloc[0]
            uno_list = report_ratios.iloc[1]
            dos_list = report_ratios.iloc[2]
            tres_list = report_ratios.iloc[3]
            cuatro_list = report_ratios.iloc[4]
            cinco_list = report_ratios.iloc[5]

            uno_list_ratio = 100*(uno_list/inspected_list)
            dos_list_ratio = 100 * (dos_list / inspected_list)
            tres_list_ratio = 100 * (tres_list / inspected_list)
            cuatro_list_ratio = 100 * (cuatro_list / inspected_list)
            cinco_list_ratio = 100 * (cinco_list / inspected_list)

            report_ratios = report_ratios.drop(['     inspected blades','                    1 - Initial', '                    2 - Intermediate',
                     '                    3 - Advanced', '                    4 - Severe', '                    5 - Critical'])

            report_ratios = report_ratios.append(uno_list_ratio,ignore_index=True)
            report_ratios = report_ratios.append(dos_list_ratio, ignore_index=True)
            report_ratios = report_ratios.append(tres_list_ratio, ignore_index=True)
            report_ratios = report_ratios.append(cuatro_list_ratio, ignore_index=True)
            report_ratios = report_ratios.append(cinco_list_ratio, ignore_index=True)

            report_ratios.set_axis(['1 - %', '2 - %', '3 - %','4 - %','5 - %'])


            report_ratios = report_ratios.T
            report_ratios.columns = ['1 - %', '2 - %', '3 - %','4 - %','5 - %']
            color_discrete_map_sev = {'1 - %':'blue', '2 - %':'yellow', '3 - %':'orange','4 - %':'red','5 - %':'black'}

            fig4 = px.bar(report_ratios, title=parque + '_' + today_str,color_discrete_map=color_discrete_map_sev)

            fig4.update_layout(width=1000, height=800)
            fig3 = px.bar(report,title=parque+'_'+today_str)


            fig3.update_layout(width=1000, height=800)
            st.plotly_chart(fig3)
            st.plotly_chart(fig4)

            df_rep = df_tracasa[df_tracasa['WindFarm']==parque]
            df_tracasa_actual_wf = df_tracasa_actual[df_tracasa_actual['WindFarm']==parque]
            blades_id = sorted(list(df_tracasa_actual_wf['BLADE ID'].unique()))
            resume = pd.DataFrame(columns=['BLADE ID','WF','WTG','BLADE POS','BLADE TYPE','MANUFACTURER','SRN','INSPECTION DATE','SEVERITY(Z4)','SEVERITY(Z8)','SEVERITY(Z7)','SEVERITY(Z10)','SEVERITY(Z13)','SEVERITY(Z6)','SEVERITY(Z11,5)','SEVERITY(Z16,6)','SEVERITY(Z15,5)','SEVERITY(Z19)','SEVERITY(Z14)','SEVERITY(Z16)','SEVERITY(Z11)','SEVERITY(Z20)','SEVERITY(Z20,6)','SEVERITY(Z17)','SEVERITY(Z18)','SEVERITY(OTHER01)','SEVERITY(OTHER02)','BLADE SEVERITY (WORST)','BLADE SEVERITY (ACUMULATED)','NEXT INSPECTION','REPAIRED'])
            for i in blades_id:
                df_blade = df_tracasa_actual_wf[df_tracasa_actual_wf['BLADE ID']==i]
                df_blade = df_blade.reset_index(drop=True)
                wf= df_blade['WindFarm'][0]
                wtg = df_blade['WTG'][0]
                pos = df_blade['Blade position'][0]
                type_b = df_blade['Blade type'][0]
                manufact = df_blade['Manufacturer'][0]
                srn = df_blade['SRN'][0]
                ins = df_blade['inspection_date'][0]
                sev_w = df_blade['Blade severity'][0]
                sev_g = df_blade['GLOBAL Blade severity'][0]
                next = df_blade['PROXIMA_INSP'][0]

                df_blade_rep = df_rep[df_rep['BLADE ID'] == i]
                df_blade_rep = df_blade_rep.reset_index(drop=True)
                df_blade_rep = df_blade_rep[df_blade_rep['Repair'] == 'YES']
                if len(df_blade_rep)==0:
                    rep ='NO'
                else:
                    rep = 'YES'

                bladeid_z19 = df_blade[df_blade['Blade radious'] == 'Z19']
                if len(bladeid_z19) > 0:
                    bladeid_z19 = bladeid_z19.reset_index(drop=0)
                    z19 = bladeid_z19['Severity'][0]
                else:
                    z19 = 'NA'

                bladeid_z18 = df_blade[df_blade['Blade radious'] == 'Z18']
                if len(bladeid_z18) > 0:
                    bladeid_z18 = bladeid_z18.reset_index(drop=0)
                    z18 = bladeid_z18['Severity'][0]
                else:
                    z18 = 'NA'

                bladeid_z16 = df_blade[df_blade['Blade radious'] == 'Z16']
                if len(bladeid_z16) > 0:
                    bladeid_z16 = bladeid_z16.reset_index(drop=0)
                    z16 = bladeid_z16['Severity'][0]
                else:
                    z16 = 'NA'

                bladeid_z14 = df_blade[df_blade['Blade radious'] == 'Z14']
                if len(bladeid_z14) > 0:
                    bladeid_z14 = bladeid_z14.reset_index(drop=0)
                    z14 = bladeid_z14['Severity'][0]
                else:
                    z14 = 'NA'

                bladeid_z6 = df_blade[df_blade['Blade radious'] == 'Z6']
                if len(bladeid_z6) > 0:
                    bladeid_z6 = bladeid_z6.reset_index(drop=0)
                    z6 = bladeid_z6['Severity'][0]
                else:
                    z6 = 'NA'

                bladeid_z13 = df_blade[df_blade['Blade radious'] == 'Z13']
                if len(bladeid_z13) > 0:
                    bladeid_z13 = bladeid_z13.reset_index(drop=0)
                    z13 = bladeid_z13['Severity'][0]
                else:
                    z13 = 'NA'

                bladeid_z4 = df_blade[df_blade['Blade radious'] == 'Z4']
                if len(bladeid_z4) > 0:
                    bladeid_z4 = bladeid_z4.reset_index(drop=0)

                    z4 = bladeid_z4['Severity'][0]
                else:
                    z4 = 'NA'

                bladeid_z16_6 = df_blade[df_blade['Blade radious'] == 'Z16.6']
                if len(bladeid_z16_6) > 0:
                    bladeid_z16_6 = bladeid_z16_6.reset_index(drop=0)
                    z16_6 = bladeid_z16_6['Severity'][0]
                else:
                    z16_6 = 'NA'

                bladeid_z7 = df_blade[df_blade['Blade radious'] == 'Z7']
                if len(bladeid_z7) > 0:
                    bladeid_z7 = bladeid_z7.reset_index(drop=0)
                    z7 = bladeid_z7['Severity'][0]
                else:
                    z7 = 'NA'

                bladeid_zother2 = df_blade[df_blade['Blade radious'] == 'Others (2)']
                if len(bladeid_zother2) > 0:
                    bladeid_zother2 = bladeid_zother2.reset_index(drop=0)
                    zother2 = bladeid_zother2['Severity'][0]
                else:
                    zother2 = 'NA'

                bladeid_zother1 = df_blade[df_blade['Blade radious'] == 'Others (1)']
                if len(bladeid_zother1) > 0:
                    bladeid_zother1 = bladeid_zother1.reset_index(drop=0)
                    zother1 = bladeid_zother1['Severity'][0]
                else:
                    zother1 = 'NA'

                bladeid_z10 = df_blade[df_blade['Blade radious'] == 'Z10']
                if len(bladeid_z10) > 0:
                    bladeid_z10 = bladeid_z10.reset_index(drop=0)
                    z10 = bladeid_z10['Severity'][0]
                else:
                    z10 = 'NA'

                bladeid_z11_5 = df_blade[df_blade['Blade radious'] == 'Z11.5']
                if len(bladeid_z11_5) > 0:
                    bladeid_z11_5 = bladeid_z11_5.reset_index(drop=0)
                    z11_5 = bladeid_z11_5['Severity'][0]
                else:
                    z11_5 = 'NA'

                bladeid_z11 = df_blade[df_blade['Blade radious'] == 'Z11']
                if len(bladeid_z11) > 0:
                    bladeid_z11 = bladeid_z11.reset_index(drop=0)
                    z11 = bladeid_z11['Severity'][0]
                else:
                    z11 = 'NA'

                bladeid_z17 = df_blade[df_blade['Blade radious'] == 'Z17']
                if len(bladeid_z17) > 0:
                    bladeid_z17 = bladeid_z17.reset_index(drop=0)
                    z17 = bladeid_z17['Severity'][0]
                else:
                    z17 = 'NA'

                bladeid_z20 = df_blade[df_blade['Blade radious'] == 'Z20']
                if len(bladeid_z20) > 0:
                    bladeid_z20 = bladeid_z20.reset_index(drop=0)
                    z20 = bladeid_z20['Severity'][0]
                else:
                    z20 = 'NA'

                bladeid_z20_6 = df_blade[df_blade['Blade radious'] == 'Z20.6']
                if len(bladeid_z20_6) > 0:
                    bladeid_z20_6 = bladeid_z20_6.reset_index(drop=0)
                    z20_6 = bladeid_z20_6['Severity'][0]
                else:
                    z20_6 = 'NA'
                bladeid_z15_5 = df_blade[df_blade['Blade radious'] == 'Z15.5']
                if len(bladeid_z15_5) > 0:
                    bladeid_z15_5 = bladeid_z15_5.reset_index(drop=0)
                    z15_5 = bladeid_z15_5['Severity'][0]
                else:
                    z15_5 = 'NA'

                bladeid_z8 = df_blade[df_blade['Blade radious'] == 'Z8']
                if len(bladeid_z8) > 0:
                    bladeid_z8 = bladeid_z8.reset_index(drop=0)
                    z8 = bladeid_z8['Severity'][0]
                else:
                    z8 = 'NA'
                resume = resume.append({'BLADE ID':i,'WF':wf,'WTG':wtg,'BLADE POS':pos,'BLADE TYPE':type_b,'MANUFACTURER':manufact,'SRN':srn,'INSPECTION DATE':ins,'SEVERITY(Z4)':z4,'SEVERITY(Z8)':z8,'SEVERITY(Z7)':z7,'SEVERITY(Z10)':z10,'SEVERITY(Z13)':z13,'SEVERITY(Z6)':z6,'SEVERITY(Z11,5)':z11_5,'SEVERITY(Z15,5)':z15_5,'SEVERITY(Z16,6)':z16_6,'SEVERITY(Z19)':z19,'SEVERITY(Z14)':z14,'SEVERITY(Z16)':z16,'SEVERITY(Z11)':z11,'SEVERITY(Z20)':z20,'SEVERITY(Z20,6)':z20_6,'SEVERITY(Z17)':z17,'SEVERITY(Z18)':z18,'SEVERITY(OTHER01)':zother1,'SEVERITY(OTHER02)':zother2,'BLADE SEVERITY (WORST)':sev_w,'BLADE SEVERITY (ACUMULATED)':sev_g,'NEXT INSPECTION':next,'REPAIRED':rep},ignore_index=True)


            resume = resume.sort_values(by='WTG')
            filename_res = ' {}_{}'.format(parque,today_str)
            resume = resume.replace('0 - No defect', str(0))
            resume = resume.replace('1 - Initial', str(1))
            resume = resume.replace('2 - Intermediate', str(2))
            resume = resume.replace('3 - Advanced', str(3))
            resume = resume.replace('4 - Severe', str(4))
            resume = resume.replace('5 - Critical', str(5))
            T = resume['BLADE TYPE'].unique()
            if len(T) == 1:
                if 'AW68.7' in T:
                    resume = resume[
                        ['BLADE ID', 'WF', 'WTG', 'BLADE POS', 'BLADE TYPE', 'MANUFACTURER', 'SRN', 'INSPECTION DATE',
                         'SEVERITY(Z8)', 'SEVERITY(Z10)', 'SEVERITY(Z11)', 'SEVERITY(Z14)', 'SEVERITY(Z15,5)',
                         'SEVERITY(Z17)', 'SEVERITY(Z20)', 'SEVERITY(Z20,6)', 'SEVERITY(OTHER01)', 'SEVERITY(OTHER02)',
                         'BLADE SEVERITY (WORST)', 'BLADE SEVERITY (ACUMULATED)', 'NEXT INSPECTION', 'REPAIRED']]
            else:
                resume = resume[['BLADE ID', 'WF', 'WTG', 'BLADE POS', 'BLADE TYPE', 'MANUFACTURER',
                                 'SRN', 'INSPECTION DATE', 'SEVERITY(Z4)', 'SEVERITY(Z7)', 'SEVERITY(Z10)',
                                 'SEVERITY(Z13)', 'SEVERITY(Z16)', 'SEVERITY(Z19)', 'SEVERITY(Z6)',
                                 'SEVERITY(Z11,5)', 'SEVERITY(Z16,6)', 'SEVERITY(Z8)', 'SEVERITY(Z11)',
                                 'SEVERITY(Z14)', 'SEVERITY(Z17)', 'SEVERITY(Z20)', 'SEVERITY(Z15,5)',
                                 'SEVERITY(Z20,6)', 'SEVERITY(OTHER01)', 'SEVERITY(OTHER02)',
                                 'BLADE SEVERITY (WORST)', 'BLADE SEVERITY (ACUMULATED)', 'NEXT INSPECTION',
                                 'REPAIRED']]

            resume = resume.sort_values(by=['WF', 'WTG', 'BLADE POS'])
            st.markdown(get_table_download_link(resume, filename_res), unsafe_allow_html=True)

            st.dataframe(resume)


            with st.expander('view last state data'):
                st.markdown(get_table_download_link(df_tracasa_actual, filename_r), unsafe_allow_html=True)
                st.dataframe(df_tracasa_actual)
    else:
        parques_afectados = df_tracasa['WindFarm'].unique()
        tipos_pala = df_tracasa['Blade type'].unique()
        for x in tipos_pala:

            df_cliente_info_aux = df_cliente_info[df_cliente_info['WindFarm'].isin(parques_afectados)]
            df_cliente_info_aux = df_cliente_info_aux[df_cliente_info_aux['BLADE TYPE']==x]

            df_cliente_info_aux = df_cliente_info_aux.reset_index(drop=True)
            df_tracasa_actual_aux = df_tracasa_actual
            df_tracasa_actual_aux = df_tracasa_actual_aux[df_tracasa_actual_aux['Blade type']==x]
            df_tracasa_wf = df_tracasa
            df_tracasa_wf = df_tracasa_wf[df_tracasa_wf['WindFarm'].isin(parques_afectados)]
            df_tracasa_wf = df_tracasa_wf[df_tracasa_wf['Blade type']==x]
            df_tracasa_wf = df_tracasa_wf.reset_index(drop=True)
            index = ['installed blades', '     inspected blades', '     inspected blades [% of installed]',
                     '          NOT affected blades', '          Affected blades CUMULATIVE',
                     '          Affected blades CUMULATIVE [% of inspected]', '               Repaired blades',
                     '               Repaired blades [% of affected]', '               Affected blades NOW',
                     '                    1 - Initial', '                    2 - Intermediate',
                     '                    3 - Advanced', '                    4 - Severe',
                     '                    5 - Critical', 'Blades meeting repair criteria',
                     'Blades not affected but AR out of spec']

            MANUFACTURER_LIST = sorted(list(df_tracasa['Manufacturer'].unique()))

            report = pd.DataFrame(index=index, columns=MANUFACTURER_LIST)

            st.header('{}_{}'.format(parque,x))

            for i in MANUFACTURER_LIST:
                df_tracasa_wf_manufacturer = df_tracasa_wf[df_tracasa_wf['Manufacturer'] == i]

                df_tracasa_actual_manufacturer = df_tracasa_actual_aux[df_tracasa_actual_aux['Manufacturer'] == i]
                if len(df_tracasa_wf_manufacturer) == 0:
                    installed = df_cliente_info_aux['{}'.format(i)][0]
                    aux = [int(installed), 0, 0, 0, 0, 0, 0,
                           0, 0, 0, 0, 0, 0, 0, 0, 0]
                    aux = list(aux)
                    report['{}'.format(i)] = aux
                else:
                    if len(df_cliente_info_aux) == 1:

                        installed = df_cliente_info_aux['{}'.format(i)][0]
                        inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                        p_installed = round(((inspected / installed) * 100),2)
                        affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] != '0 - No defect']
                        affected_now = len(affected_now['BLADE ID'].unique())

                        affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                            ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                        affected_cum = len(affected_cum['BLADE ID'].unique())
                        p_inspected = round(((affected_cum / inspected) * 100), 2)
                        torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                            ['3 - Advanced', '4 - Severe', '5 - Critical'])]
                        torepair = len(torepair['BLADE ID'].unique())
                        repaired = df_tracasa_wf_manufacturer[
                            df_tracasa_wf_manufacturer['Repair'] == 'YES']
                        repaired = len(repaired['BLADE ID'].unique())
                        if affected_cum == 0:
                            p_affected = 0
                        else:
                            p_affected = round(((repaired / affected_cum) * 100), 2)
                        cero = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                        cero = len(cero['BLADE ID'].unique())
                        uno = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                        uno = len(uno['BLADE ID'].unique())
                        dos = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                        dos = len(dos['BLADE ID'].unique())
                        tres = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                        tres = len(tres['BLADE ID'].unique())
                        cuatro = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                        cuatro = len(cuatro['BLADE ID'].unique())
                        cinco = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                        cinco = len(cinco['BLADE ID'].unique())
                        AR_7 = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['AR'] < 8]
                        AR_7 = AR_7[AR_7['Blade severity'] == '0 - No defect']
                        AR_7 = AR_7['BLADE ID'].unique()
                        AR_7 = len(AR_7)

                        aux = [int(installed), inspected, p_installed, cero, affected_cum, p_inspected, repaired,
                           p_affected,
                           affected_now, uno, dos, tres, cuatro, cinco, torepair, AR_7]


                        report['{}'.format(i)] = aux
                    elif len(df_cliente_info_aux) > 1:
                        installed = df_cliente_info_aux['{}'.format(i)].sum()
                        inspected = len(df_tracasa_wf_manufacturer['BLADE ID'].unique())
                        p_installed = round(((inspected / installed) * 100),2)
                        affected_now = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] != '0 - No defect']
                        affected_now = len(affected_now['BLADE ID'].unique())


                        affected_cum = df_tracasa_wf_manufacturer[df_tracasa_wf_manufacturer['Severity'].isin(
                            ['1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'])]
                        affected_cum = len(affected_cum['BLADE ID'].unique())
                        p_inspected = round(((affected_cum / inspected) * 100), 2)
                        torepair = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'].isin(
                            ['3 - Advanced', '4 - Severe', '5 - Critical'])]
                        torepair = len(torepair['BLADE ID'].unique())
                        repaired = df_tracasa_wf_manufacturer[
                            df_tracasa_wf_manufacturer['Repair'] == 'YES']
                        repaired = len(repaired['BLADE ID'].unique())
                        if affected_cum == 0:
                            p_affected = 0
                        else:
                            p_affected = round(((repaired / affected_cum) * 100), 2)
                        cero = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '0 - No defect']
                        cero = len(cero['BLADE ID'].unique())
                        uno = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '1 - Initial']
                        uno = len(uno['BLADE ID'].unique())
                        dos = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '2 - Intermediate']
                        dos = len(dos['BLADE ID'].unique())
                        tres = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '3 - Advanced']
                        tres = len(tres['BLADE ID'].unique())
                        cuatro = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '4 - Severe']
                        cuatro = len(cuatro['BLADE ID'].unique())
                        cinco = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['Blade severity'] == '5 - Critical']
                        cinco = len(cinco['BLADE ID'].unique())
                        AR_7 = df_tracasa_actual_manufacturer[df_tracasa_actual_manufacturer['AR'] < 8]
                        AR_7 = AR_7[AR_7['Blade severity'] == '0 - No defect']
                        AR_7 = AR_7['BLADE ID'].unique()
                        AR_7 = len(AR_7)
                        aux = [int(installed), inspected, p_installed, cero, affected_cum, p_inspected, repaired,
                               p_affected,
                               affected_now, uno, dos, tres, cuatro, cinco, torepair, AR_7]
                        aux = list(aux)
                        report['{}'.format(i)] = aux

            installed_total = report.iloc[[0]].sum()
            installed_total = installed_total.sum()
            inspected_total = report.iloc[[1]].sum()
            inspected_total = inspected_total.sum()
            por_isnpected = round((inspected_total / installed_total) * 100, 2)
            not_affected_total = report.iloc[[3]].sum()
            not_affected_total = not_affected_total.sum()
            affected_cumu = report.iloc[[4]].sum()
            affected_cumu = affected_cumu.sum()
            por_effected = round((affected_cumu / inspected_total) * 100, 2)
            repaired_total = report.iloc[[6]].sum()
            repaired_total = repaired_total.sum()
            por_reppaired = round((repaired_total / affected_cumu) * 100, 2)
            afected_now_total = report.iloc[[8]].sum()
            afected_now_total = afected_now_total.sum()
            uno_total = report.iloc[[9]].sum()
            uno_total = uno_total.sum()
            dos_total = report.iloc[[10]].sum()
            dos_total = dos_total.sum()
            tres_total = report.iloc[[11]].sum()
            tres_total = tres_total.sum()
            cuatro_total = report.iloc[[12]].sum()
            cuatro_total = cuatro_total.sum()
            cinco_total = report.iloc[[13]].sum()
            cinco_total = cinco_total.sum()
            AR_7_total = report.iloc[[15]].sum()
            AR_7_total = AR_7_total.sum()
            torepair_tot = report.iloc[[14]].sum()
            torepair_tot = torepair_tot.sum()
            total = [installed_total, inspected_total, por_isnpected, not_affected_total, affected_cumu, por_effected,
                     repaired_total, por_reppaired, afected_now_total, uno_total, dos_total, tres_total, cuatro_total,
                     cinco_total, torepair_tot, AR_7_total]
            fig = px.pie(values=[not_affected_total,uno_total,dos_total,tres_total,cuatro_total], names=['0 - No defect','1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe'],title='{}'.format(parque),color=['0 - No defect','1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe'],
                         color_discrete_map={'0 - No defect':'#2ca02c',
                                             '1 - Initial':'greenyellow',
                                             '2 - Intermediate':'yellow',
                                             '3 - Advanced':'orange',
                                             '4 - Severe':'red'})
            fig = go.Figure(data=[go.Pie(labels=['0 - No defect','1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe','5 - Critical'], values=[not_affected_total,uno_total,dos_total,tres_total,cuatro_total,cinco_total], hole=.4, marker_colors=['#2ca02c','blue','yellow','orange','red','black'], sort=False)])
            fig.update_layout(
                title=parque+'_'+today_str+'_'+x,
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="RebeccaPurple"
                )
            )
            report['TOTAL'] = total
            fig2 = ff.create_table(report,index=index, index_title=parque+today_str+x)
            today = date.today()
            today_str = str(today)
            filename_r = 'REPORT_{}'.format(today_str)
            report['index'] = report.index
            cols = report.columns.tolist()
            cols = cols[-1:] + cols[:-1]
            report = report[cols]
            st.markdown(get_table_download_link(report, filename_r), unsafe_allow_html=True)
            del report['index']
            fig.update_layout(width=1000, height=800)
            fig2.update_layout(width=2200, height=800)
            st.plotly_chart(fig2)
            st.plotly_chart(fig)
            report_ratios = report
            report = report.drop(['     inspected blades [% of installed]',
                                 '          NOT affected blades',
                                 '          Affected blades CUMULATIVE [% of inspected]',
                                 '               Repaired blades [% of affected]', '               Affected blades NOW',
                                 '                    1 - Initial', '                    2 - Intermediate',
                                 '                    3 - Advanced', '                    4 - Severe',
                                 '                    5 - Critical',
                                 'Blades meeting repair criteria', 'Blades not affected but AR out of spec'])

            del report['TOTAL']
            report_ratios = report_ratios.drop(['installed blades', '     inspected blades [% of installed]',
                                                '          NOT affected blades', '          Affected blades CUMULATIVE',
                                                '          Affected blades CUMULATIVE [% of inspected]',
                                                '               Repaired blades',
                                                '               Repaired blades [% of affected]',
                                                '               Affected blades NOW',
                                                'Blades meeting repair criteria',
                                                'Blades not affected but AR out of spec'])
            del report_ratios['TOTAL']

            inspected_list = report_ratios.iloc[0]
            uno_list = report_ratios.iloc[1]
            dos_list = report_ratios.iloc[2]
            tres_list = report_ratios.iloc[3]
            cuatro_list = report_ratios.iloc[4]
            cinco_list = report_ratios.iloc[5]

            uno_list_ratio = 100 * (uno_list / inspected_list)
            dos_list_ratio = 100 * (dos_list / inspected_list)
            tres_list_ratio = 100 * (tres_list / inspected_list)
            cuatro_list_ratio = 100 * (cuatro_list / inspected_list)
            cinco_list_ratio = 100 * (cinco_list / inspected_list)

            report_ratios = report_ratios.drop(
                ['     inspected blades', '                    1 - Initial', '                    2 - Intermediate',
                 '                    3 - Advanced', '                    4 - Severe',
                 '                    5 - Critical'])

            report_ratios = report_ratios.append(uno_list_ratio, ignore_index=True)
            report_ratios = report_ratios.append(dos_list_ratio, ignore_index=True)
            report_ratios = report_ratios.append(tres_list_ratio, ignore_index=True)
            report_ratios = report_ratios.append(cuatro_list_ratio, ignore_index=True)
            report_ratios = report_ratios.append(cinco_list_ratio, ignore_index=True)

            report_ratios.set_axis(['1 - %', '2 - %', '3 - %', '4 - %','5 - %'])

            report_ratios = report_ratios.T
            report_ratios.columns = ['1 - %', '2 - %', '3 - %', '4 - %','5 - %']
            color_discrete_map_sev = {'1 - %': 'blue', '2 - %': 'yellow', '3 - %': 'orange', '4 - %': 'red','5 - %':'black'}
            fig4 = px.bar(report_ratios, title=parque + '_' + today_str, color_discrete_map=color_discrete_map_sev)

            fig3 = px.bar(report,title=parque+'_'+today_str+'_'+x)
            fig3.update_layout(width=1000, height=800)
            fig4.update_layout(width=1000, height=800)
            st.plotly_chart(fig3)
            st.plotly_chart(fig4)
            with st.expander('view las state data'):
                st.markdown(get_table_download_link(df_tracasa_actual, filename_r), unsafe_allow_html=True)
                st.dataframe(df_tracasa_actual)


@st.experimental_memo
def KPI(df_tracasa_actual,df_tracasa,df_sem):
    today = date.today()

    hace_una_semana = today + timedelta(weeks=-1)
    hace_una_semana = np.datetime64(hace_una_semana)
    df_tracasa_sem = df_tracasa[df_tracasa['upload_date']<hace_una_semana]
    df_tracasa_actual_sem = df_sem



    ### KPIs actuales  b###
    PALAS_AFECTADAS = df_tracasa_actual[df_tracasa_actual['Blade severity'].isin(['5 - Critical','4 - Severe', '3 - Advanced', '2 - Intermediate','1 - Initial'])]
    PALAS_AFECTADAS = len(PALAS_AFECTADAS['BLADE ID'].unique())
    criticas = df_tracasa_actual[df_tracasa_actual['Blade severity'].isin(['4 - Severe', '3 - Advanced'])]
    CRITICAS = len(criticas['BLADE ID'].unique())
    repair = df_tracasa_actual[df_tracasa_actual['Blade severity'].isin(['5 - Critical','4 - Severe', '3 - Advanced', '2 - Intermediate'])]
    PENDIENTES_REPARAR = len(repair['BLADE ID'].unique())
    PALAS_REPARADAS = df_tracasa[df_tracasa['Repair'] == 'YES']
    PALAS_REPARADAS = len(PALAS_REPARADAS['BLADE ID'].unique())
    PALAS_PARO = df_tracasa_actual[df_tracasa_actual['Blade severity'] == '5 - Critical']
    PALAS_PARO = len(PALAS_PARO['BLADE ID'].unique())
    inspeccionadas_palas = len(df_tracasa['BLADE ID INP'].unique())
    insp = len(df_tracasa_actual['BLADE ID'].unique())
    cero_t = df_tracasa_actual[df_tracasa_actual['Blade severity'] == '0 - No defect']
    cero_t = len(cero_t['BLADE ID'].unique())
    uno_t = df_tracasa_actual[df_tracasa_actual['Blade severity'] == '1 - Initial']
    uno_t = len(uno_t['BLADE ID'].unique())
    dos_t = df_tracasa_actual[df_tracasa_actual['Blade severity'] == '2 - Intermediate']
    dos_t = len(dos_t['BLADE ID'].unique())
    tres_t = df_tracasa_actual[df_tracasa_actual['Blade severity'] == '3 - Advanced']
    lista3 = tres_t['BLADE ID'].unique()
    tres_t = len(tres_t['BLADE ID'].unique())
    cuatro_t = df_tracasa_actual[df_tracasa_actual['Blade severity'] == '4 - Severe']
    lista4 = cuatro_t['BLADE ID'].unique()
    cuatro_t = len(cuatro_t['BLADE ID'].unique())
    cinco_t = df_tracasa_actual[df_tracasa_actual['Blade severity'] == '5 - Critical']
    lista5 = cinco_t['BLADE ID'].unique()
    cinco_t = len(cinco_t['BLADE ID'].unique())


    ### KPIs hace una semana  b###

    PALAS_AFECTADAS_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'].isin(['5 - Critical','4 - Severe', '3 - Advanced', '2 - Intermediate','1 - Initial'])]
    PALAS_AFECTADAS_sem = len(PALAS_AFECTADAS_sem['BLADE ID'].unique())
    criticas_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'].isin(['4 - Severe', '3 - Advanced'])]
    CRITICAS_sem = len(criticas_sem['BLADE ID'].unique())
    repair_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'].isin(['5 - Critical','4 - Severe', '3 - Advanced', '2 - Intermediate'])]
    PENDIENTES_REPARAR_sem = len(repair_sem['BLADE ID'].unique())
    PALAS_REPARADAS_sem = df_sem[df_sem['Repair'] == 'YES']
    PALAS_REPARADAS_sem = len(PALAS_REPARADAS_sem['BLADE ID'].unique())
    PALAS_PARO_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'] == '5 - Critical']
    PALAS_PARO_sem = len(PALAS_PARO_sem['BLADE ID'].unique())
    inspeccionadas_palas_sem = len(df_tracasa_sem['BLADE ID INP'].unique())
    insp_sem = len(df_tracasa_actual_sem['BLADE ID'].unique())
    cero_t_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'] == '0 - No defect']
    cero_t_sem = len(cero_t_sem['BLADE ID'].unique())
    uno_t_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'] == '1 - Initial']
    uno_t_sem = len(uno_t_sem['BLADE ID'].unique())
    dos_t_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'] == '2 - Intermediate']
    dos_t_sem = len(dos_t_sem['BLADE ID'].unique())
    tres_t_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'] == '3 - Advanced']
    lista_3_sem = tres_t_sem['BLADE ID'].unique()
    tres_t_sem = len(tres_t_sem['BLADE ID'].unique())
    cuatro_t_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'] == '4 - Severe']
    lista_4_sem = cuatro_t_sem['BLADE ID'].unique()
    cuatro_t_sem = len(cuatro_t_sem['BLADE ID'].unique())
    cinco_t_sem = df_tracasa_actual_sem[df_tracasa_actual_sem['Blade severity'] == '5 - Critical']
    lista_5_sem = cinco_t_sem['BLADE ID'].unique()
    cinco_t_sem = len(cinco_t_sem['BLADE ID'].unique())

    tresdif = list(set(lista3).difference(lista_3_sem))
    cuatrodif = list(set(lista4).difference(lista_4_sem))
    cincodif = list(set(lista5).difference(lista_5_sem))

    ### DELTAS ###

    PALAS_AFECTADAS_delta = PALAS_AFECTADAS -PALAS_AFECTADAS_sem

    CRITICAS_delta = CRITICAS - CRITICAS_sem

    PENDIENTES_REPARAR_delta = PENDIENTES_REPARAR - PENDIENTES_REPARAR_sem

    PALAS_REPARADAS_delta = PALAS_REPARADAS -PALAS_REPARADAS_sem

    PALAS_PARO_delta = PALAS_PARO - PALAS_PARO_sem

    inspeccionadas_palas_delta = inspeccionadas_palas - inspeccionadas_palas_sem

    insp_delta = insp - insp_sem

    cero_t_delta = cero_t - cero_t_sem

    uno_t_delta = uno_t - uno_t_sem

    dos_t_delta = dos_t - dos_t_sem

    tres_t_delta = tres_t - tres_t_sem

    cuatro_t_delta = cuatro_t - cuatro_t_sem

    cinco_t_delta = cinco_t - cinco_t_sem


    return PALAS_AFECTADAS,CRITICAS,PENDIENTES_REPARAR,inspeccionadas_palas,insp,PALAS_REPARADAS,PALAS_PARO,cero_t,uno_t,dos_t,tres_t,cuatro_t,cinco_t,PALAS_AFECTADAS_delta,CRITICAS_delta,PENDIENTES_REPARAR_delta,PALAS_REPARADAS_delta,PALAS_PARO_delta,inspeccionadas_palas_delta,insp_delta,cero_t_delta,uno_t_delta,dos_t_delta,tres_t_delta,cuatro_t_delta,cinco_t_delta,tresdif,cuatrodif,cincodif

def CALENDAR_NEW(df):
    today = date.today()
    today_str = str(today)
    filename_cal = 'CALENDARIO_INSPECCIONES_{}'.format(today_str)
    calendar = df[['WindFarm','WTG','BLADE ID', 'PROXIMA_INSP', 'Blade severity','inspection_date']]
    calendar = calendar.drop_duplicates()
    calendar = calendar.reset_index(drop=True)
    frec = list()
    for i in range(len(calendar)):
        a = calendar['PROXIMA_INSP'][i]
        b = calendar['inspection_date'][i]
        c = (a-b).days
        frec.append(c)


    calendar['FRECUENCIA INSP'] = frec
    calendar['FIN'] = calendar['PROXIMA_INSP'] + timedelta(days=10)
    fig = px.timeline(calendar, x_start="PROXIMA_INSP", x_end="FIN", y="BLADE ID", color="WindFarm",
                      title='INSPECTIONS CALENDAR')
    fig.update_yaxes(autorange="reversed")  # otherwise tasks are listed from the bottom up
    fig.update_layout(width=1200, height=800)
    calendar['STATUS'] = ''
    calendar = calendar.reset_index(drop=True)
    today = date.today()
    for i in range(len(calendar)):
        if calendar['PROXIMA_INSP'][i] < today:
            delta = (today - calendar['PROXIMA_INSP'][i]).days
            limit = (calendar['FRECUENCIA INSP'][i])
            limit = limit / 2
            if delta > limit:
                calendar['STATUS'][i] = 'VERY DELAYED'
            else:
                calendar['STATUS'][i] = 'DELAYED'
        else:
            calendar['STATUS'][i] = 'ON TIME'

    calendar = calendar.drop_duplicates()


    calendar = calendar.replace('0 - No defect', int(0))
    calendar = calendar.replace('1 - Initial', int(1))
    calendar = calendar.replace('2 - Intermediate', int(2))
    calendar = calendar.replace('3 - Advanced', int(3))
    calendar = calendar.replace('4 - Severe', int(4))
    calendar = calendar.replace('5 - Critical', int(5))
    del calendar['FIN']
    st.plotly_chart(fig)
    st.markdown(get_table_download_link(calendar, filename_cal), unsafe_allow_html=True)
    st.table(calendar)



def CALENDAR(filtros_aplicados,windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df):

    df['FRECUENCIA INSP'] = df['PROXIMA_INSP'] - df['inspection_date']
    calendar = df[['BLADE ID', 'PROXIMA_INSP','Blade severity','FRECUENCIA INSP']]
    calendar['FIN'] = calendar['PROXIMA_INSP'] + timedelta(days=10)
    calendar['WindFarm'] = df['WindFarm']
    calendar['WTG'] = df['WTG']
    calendar = calendar.sort_values(by='BLADE ID')
    fig = px.timeline(calendar, x_start="PROXIMA_INSP", x_end="FIN", y="BLADE ID", color="WindFarm",title='INSPECTIONS CALENDAR {}'.format(filtros_aplicados))
    fig.update_yaxes(autorange="reversed")  # otherwise tasks are listed from the bottom up
    fig.update_layout(width=1200, height=800)
    calendar = calendar[['WindFarm','WTG','BLADE ID','PROXIMA_INSP','Blade severity','FRECUENCIA INSP']]
    calendar['STATUS'] = ''
    calendar = calendar.reset_index(drop=True)
    today = date.today()
    for i in range(len(calendar)):
        if calendar['PROXIMA_INSP'][i]<today:
            delta = (today - calendar['PROXIMA_INSP'][i]).days
            limit = (calendar['FRECUENCIA INSP'][i]).days
            limit = limit/2
            if delta > limit:
                calendar['STATUS'][i] = 'VERY DELAYED'
            else:
                calendar['STATUS'][i] = 'DELAYED'
        else:
            calendar['STATUS'][i] = 'ON TIME'

    calendar = calendar.drop_duplicates()



    st.write('GANTT CHART')
    st.plotly_chart(fig)
    today = date.today()
    today_str = str(today)
    filename_cal = 'CALENDARIO_INSPECCIONES_{}'.format(today_str)

    cal = calendar.replace('0 - No defect', int(0))
    cal = calendar.replace('1 - Initial', int(1))
    cal = calendar.replace('2 - Intermediate', int(2))
    cal = calendar.replace('3 - Advanced', int(3))
    cal = calendar.replace('4 - Severe', int(4))
    cal = calendar.replace('5 - Critical', int(5))

    st.markdown(get_table_download_link(calendar, filename_cal), unsafe_allow_html=True)

    st.table(calendar)


@st.experimental_memo
def ISO(df_tracasa_actual):
    with open(file='iso.txt', mode="r") as f:
        sql_iso = f.read()

    df_iso_t = pd.read_sql(
        sql=sql_iso,
        con=CONEXION_SQL()
    )
    df_iso_t = df_iso_t.append({'Country':'Mexico','WindFarm':'DOLORES'},ignore_index=True)
    df_iso_t = df_iso_t.append({'Country': 'United States', 'WindFarm': 'HIGH LONESOME'}, ignore_index=True)
    df_iso = px.data.gapminder()
    df_iso = df_iso[['country','iso_alpha']]
    df_iso = df_iso.drop_duplicates()
    df_iso.columns = ['Country', 'ISO']
    df_iso = df_iso.merge(df_iso_t,on='Country',how='left')
    lista_parque = df_tracasa_actual['WindFarm'].unique()
    totales_wf = pd.DataFrame(columns=['WindFarm','Total'])
    for i in lista_parque:
        parque_actual = df_tracasa_actual[df_tracasa_actual['WindFarm'] == i]
        palas = parque_actual['BLADE ID'].unique()
        tot = int(len(palas))
        totales_wf = totales_wf.append({'WindFarm':i,'Total':tot},ignore_index=True)
    totales_wf = totales_wf.dropna()
    totales_wf = totales_wf.replace('DEHESA', 'DEHESA DE MALLÉN')
    df_iso['WindFarm'] = df_iso['WindFarm'].str.upper()
    df_iso = df_iso.append({'Country':'United States' , 'ISO':'USA', 'WindFarm':'DIAMOND VISTA'},ignore_index=True)
    df_iso = df_iso.append({'Country': 'United States', 'ISO': 'USA', 'WindFarm': 'RATTLESNAKE CREEK'}, ignore_index=True)
    totales_wf = totales_wf.merge(df_iso,on='WindFarm',how='left')

    mapa = px.scatter_geo(totales_wf, locations="ISO", color="WindFarm",
                         hover_name="WindFarm", size="Total",
                         projection="natural earth")

    return mapa

def MAPA_DEFECTOS(df_tracasa_actual,windfarm, wtg, blade_type, manufacturer, srn,blade_criticidad,filtros_aplicados,MOLDE_CHECK):
    ## FILTRADO ##
    if not MOLDE_CHECK:

        df_tracasa_actual = FILTER(windfarm, wtg, blade_type, manufacturer, srn,blade_criticidad, df_tracasa_actual)
        tipos = df_tracasa_actual['Blade type'].unique()
        for p in tipos:
            df_tipo = df_tracasa_actual[df_tracasa_actual['Blade type']==p]
            BLADES = df_tipo['BLADE ID'].unique()
            Zs = list(df_tipo["Blade radious"].unique())
            col = Zs
            col.append('BLADE ID')
            defects = pd.DataFrame(columns=col)
            Zs = list(df_tipo["Blade radious"].unique())
            for i in BLADES:
                df_blade = df_tipo[df_tipo['BLADE ID'] == i]
                row = list()
                for x in Zs:
                    df_z = df_blade[df_blade["Blade radious"] == x]
                    df_z = df_z.reset_index(drop=True)
                    if len(df_z) == 0:
                        criticidad = None
                        row.append(criticidad)
                    else:
                        criticidad = df_z['Severity'][0]
                        row.append(criticidad)
                row.append(i)
                a_series = pd.Series(row, index=defects.columns)
                defects = defects.append(a_series, ignore_index=True)
                row.clear()
            defects = defects.replace('0 - No defect', int(0))
            defects = defects.replace('1 - Initial', int(1))
            defects = defects.replace('2 - Intermediate', int(2))
            defects = defects.replace('3 - Advanced', int(3))
            defects = defects.replace('4 - Severe', int(4))
            defects = defects.replace('5 - Critical', int(5))
            try:
                Z4 = defects["Z4"].mean()
                Z4_sum = defects["Z4"].sum()
                Z4_incidencias = len(defects[defects['Z4']>0])
                Z4_grav = Z4_sum/Z4_incidencias
            except:
                Z4 = 0
                Z4_incidencias = 0
                Z4_grav = 0
            try:
                Z7 = defects["Z7"].mean()
                Z7_sum = defects["Z7"].sum()
                Z7_incidencias = len(defects[defects['Z7'] > 0])
                Z7_grav = Z7_sum / Z7_incidencias
            except:
                Z7 = 0
                Z7_incidencias = 0
                Z7_grav = 0
            try:
                Z10 = defects["Z10"].mean()
                Z10_sum = defects["Z10"].sum()
                Z10_incidencias = len(defects[defects['Z10'] > 0])
                Z10_grav = Z10_sum / Z10_incidencias
            except:
                Z10 = 0
                Z10_incidencias = 0
                Z10_grav = 0
            try:
                Z13 = defects["Z13"].mean()
                Z13_sum = defects["Z13"].sum()
                Z13_incidencias = len(defects[defects['Z13'] > 0])
                Z13_grav = Z13_sum / Z13_incidencias
            except:
                Z13 = 0
                Z13_incidencias = 0
                Z13_grav = 0
            try:
                Z14 = defects["Z14"].mean()
                Z14_sum = defects["Z14"].sum()
                Z14_incidencias = len(defects[defects['Z14'] > 0])
                Z14_grav = Z14_sum / Z14_incidencias
            except:
                Z14 = 0
                Z14_incidencias = 0
                Z14_grav = 0
            try:
                Z16 = defects["Z16"].mean()
                Z16_sum = defects["Z16"].sum()
                Z16_incidencias = len(defects[defects['Z16'] > 0])
                Z16_grav = Z16_sum / Z16_incidencias
            except:
                Z16 = 0
                Z16_incidencias = 0
                Z16_grav = 0
            try:
                Z17 = defects["Z17"].mean()
                Z17_sum = defects["Z17"].sum()
                Z17_incidencias = len(defects[defects['Z17'] > 0])
                Z17_grav = Z17_sum / Z17_incidencias
            except:
                Z17 = 0
                Z17_incidencias = 0
                Z17_grav = 0
            try:
                Z19 = defects["Z19"].mean()
                Z19_sum = defects["Z19"].sum()
                Z19_incidencias = len(defects[defects['Z19'] > 0])
                Z19_grav = Z19_sum / Z19_incidencias
            except:
                Z19 = 0
                Z19_incidencias = 0
                Z19_grav = 0
            try:
                Z20 = defects["Z20"].mean()
                Z20_sum = defects["Z20"].sum()
                Z20_incidencias = len(defects[defects['Z20'] > 0])
                Z20_grav = Z20_sum / Z20_incidencias
            except:
                Z20 = 0
                Z20_incidencias = 0
                Z20_grav = 0
            try:
                Z6 = defects["Z6"].mean()
                Z6_sum = defects["Z6"].sum()
                Z6_incidencias = len(defects[defects['Z6'] > 0])
                Z6_grav = Z6_sum / Z6_incidencias
            except:
                Z6 = 0
                Z6_incidencias = 0
                Z6_grav = 0
            try:
                Z8 = defects["Z8"].mean()
                Z8_sum = defects["Z8"].sum()
                Z8_incidencias = len(defects[defects['Z8'] > 0])
                Z8_grav = Z8_sum / Z8_incidencias
            except:
                Z8 = 0
                Z8_incidencias = 0
                Z8_grav = 0
            try:
                Z11 = defects["Z11"].mean()
                Z11_sum = defects["Z11"].sum()
                Z11_incidencias = len(defects[defects['Z11'] > 0])
                Z11_grav = Z11_sum / Z11_incidencias
            except:
                Z11 = 0
                Z11_incidencias = 0
                Z11_grav = 0
            try:
                Z11_5 = defects["Z11.5"].mean()
                Z11_5_sum = defects["Z11.5"].sum()
                Z11_5_incidencias = len(defects[defects['Z11.5'] > 0])
                Z11_5_grav = Z11_5_sum / Z11_5_incidencias
            except:
                Z11_5 = 0
                Z11_5_incidencias = 0
                Z11_5_grav = 0
            try:
                Z15_5 = defects["Z15.5"].mean()
                Z15_5_sum = defects["Z15.5"].sum()
                Z15_5_incidencias = len(defects[defects['Z15.5'] > 0])
                Z15_5_grav = Z15_5_sum / Z15_5_incidencias
            except:
                Z15_5 = 0
                Z15_5_incidencias = 0
                Z15_5_grav = 0
            try:
                Z16_6 = defects["Z16.6"].mean()
                Z16_6_sum = defects["Z16.6"].sum()
                Z16_6_incidencias = len(defects[defects['Z16.6'] > 0])
                Z16_6_grav = Z16_6_sum / Z16_6_incidencias
            except:
                Z16_6 = 0
                Z16_6_incidencias = 0
                Z16_6_grav = 0
            try:
                Z20_6 = defects["Z20.6"].mean()
                Z20_6_sum = defects["Z20.6"].sum()
                Z20_6_incidencias = len(defects[defects['Z20.6'] > 0])
                Z20_6_grav = Z20_6_sum / Z20_6_incidencias
            except:
                Z20_6 = 0
                Z20_6_incidencias = 0
                Z20_6_grav = 0
            st.header('SEVERITY RATIOS_{}'.format(p))
            col1,col2,col3,col4,col5 = st.columns(5)

            col1.metric('Z4', '{}'.format(str(Z4)[:4]))
            col2.metric("Z7", '{}'.format(str(Z7)[:4]))
            col3.metric("Z10", '{}'.format(str(Z10)[:4]))
            col4.metric("Z11", '{}'.format(str(Z11)[:4]))
            col5.metric("Z13", '{}'.format(str(Z13)[:4]))

            col1.metric('Z14', '{}'.format(str(Z14)[:4]))
            col2.metric("Z16", '{}'.format(str(Z16)[:4]))
            col3.metric("Z17", '{}'.format(str(Z17)[:4]))
            col4.metric("Z19", '{}'.format(str(Z19)[:4]))
            col5.metric("Z20", '{}'.format(str(Z20)[:4]))

            col1.metric('Z6', '{}'.format(str(Z6)[:4]))
            col2.metric("Z11_5", '{}'.format(str(Z11_5)[:4]))
            col3.metric("Z16_6", '{}'.format(str(Z16_6)[:4]))
            col4.metric("Z15_5", '{}'.format(str(Z15_5)[:4]))
            col5.metric("Z20_6", '{}'.format(str(Z20_6)[:4]))




            fig = px.line(x=['Z4', "Z7","Z8","Z10","Z11","Z13","Z14","Z16","Z17","Z19","Z20"], y=[Z4,Z7,Z8,Z10,Z11,Z13,Z14,Z16,Z17,Z19,Z20],title='AERIS;NBI;NBS;TPI,TMT')
            fig2 = px.line(x=["Z6","Z10", "Z11_5","Z15,5", "Z16_6","Z20,6"], y = [Z6,Z10, Z11_5,Z15_5, Z16_6,Z20_6],title='CONCAMEX')
            fig3 = px.bar(x=['Z4', "Z7","Z8","Z10","Z11","Z13","Z14","Z16","Z17","Z19","Z20"], y=[Z4_incidencias,Z7_incidencias,Z8_incidencias,Z10_incidencias,Z11_incidencias, Z13_incidencias,Z14_incidencias, Z16_incidencias,Z17_incidencias, Z19_incidencias, Z20_incidencias],title='AERIS;NBI;NBS;TPI,TMT [Numero de incidencias]' )
            fig4 = px.bar(x=["Z6","Z10", "Z11_5","Z15,5", "Z16_6","Z20,6"], y = [Z6_incidencias,Z10_incidencias, Z11_5_incidencias,Z15_5_incidencias, Z16_6_incidencias,Z20_6_incidencias],title='CONCAMEX [numero de incidencias]')
            fig5 = px.bar(x=['Z4', "Z7","Z8","Z10","Z11","Z13","Z14","Z16","Z17","Z19","Z20"], y=[Z4_grav,Z7_grav,Z8_grav, Z10_grav,Z11_grav, Z13_grav,Z14_grav, Z16_grav,Z17_grav, Z19_grav, Z20_grav],title='AERIS;NBI;NBS;TPI,TMT [Gravedad media de las incidencias]' )
            fig6 = px.bar(x=["Z6","Z10", "Z11_5","Z15,5", "Z16_6","Z20,6"], y = [Z6_grav,Z10_grav, Z11_5_grav,Z15_5_grav, Z16_6_grav,Z20_6_grav],title='CONCAMEX [gravedad media de las incidencias]')

            today = date.today()
            today_str = str(today)
            filename_mapa = 'MAPA_DEFECTOS_{}_{}'.format(today_str,p)
            col1, col2 = st.columns(2)

            col1.plotly_chart(fig3)
            col2.plotly_chart(fig4)
            col1.plotly_chart(fig5)
            col2.plotly_chart(fig6)
            defects = defects.set_index('BLADE ID')
            if manufacturer in ["IND (INDUTCH)","NBS (ACCIONA BLADES)","TMT","NBM (CONCAMEX)","TPI"]:
                defects = defects[["Z4","Z7","Z10","Z13","Z16","Z19"]]
            elif manufacturer in ["NBM (CONCAMEX)"]:
                defects = defects[["Z6","Z11.5","Z16.6"]]
            st.markdown(get_table_download_link(defects, filename_mapa), unsafe_allow_html=True)
            st.dataframe(defects)



    if MOLDE_CHECK:
        df_tracasa_actual_mold = df_tracasa_actual
        df_tracasa_actual_mold_not = df_tracasa_actual

        for i in range(len(df_tracasa_actual_mold)):
            if pd.isnull(df_tracasa_actual_mold['Mold'][i]):
                df_tracasa_actual_mold = df_tracasa_actual_mold.drop([i], axis=0)
        for i in range(len(df_tracasa_actual_mold_not)):
            if not pd.isnull(df_tracasa_actual_mold_not['Mold'][i]):
                df_tracasa_actual_mold_not = df_tracasa_actual_mold_not.drop([i], axis=0)
        df_tracasa_actual_mold = df_tracasa_actual_mold.reset_index(drop=True)
        df_tracasa_actual_mold_not = df_tracasa_actual_mold_not.reset_index(drop=True)
        st.header('Defects mapping without mold info')
        BLADES = df_tracasa_actual_mold_not['BLADE ID'].unique()
        Zs = list(df_tracasa_actual_mold_not["Blade radious"].unique())
        col = Zs
        col.append('BLADE ID')
        defects = pd.DataFrame(columns=col)
        Zs = list(df_tracasa_actual_mold_not["Blade radious"].unique())
        for i in BLADES:
            df_blade = df_tracasa_actual_mold_not[df_tracasa_actual_mold_not['BLADE ID'] == i]
            row = list()
            for x in Zs:
                df_z = df_blade[df_blade["Blade radious"] == x]
                df_z = df_z.reset_index(drop=True)
                if len(df_z) == 0:
                    criticidad = None
                    row.append(criticidad)
                else:
                    criticidad = df_z['Severity'][0]
                    row.append(criticidad)
            row.append(i)
            a_series = pd.Series(row, index=defects.columns)
            defects = defects.append(a_series, ignore_index=True)
            row.clear()
        defects = defects.replace('0 - No defect', int(0))
        defects = defects.replace('1 - Initial', int(1))
        defects = defects.replace('2 - Intermediate', int(2))
        defects = defects.replace('3 - Advanced', int(3))
        defects = defects.replace('4 - Severe', int(4))
        defects = defects.replace('5 - Critical', int(5))
        try:
            Z4 = defects["Z4"].mean()
            Z4_sum = defects["Z4"].sum()
            Z4_incidencias = len(defects[defects['Z4'] > 0])
            Z4_grav = Z4_sum / Z4_incidencias
        except:
            Z4 = 0
            Z4_incidencias = 0
            Z4_grav = 0
        try:
            Z7 = defects["Z7"].mean()
            Z7_sum = defects["Z7"].sum()
            Z7_incidencias = len(defects[defects['Z7'] > 0])
            Z7_grav = Z7_sum / Z7_incidencias
        except:
            Z7 = 0
            Z7_incidencias = 0
            Z7_grav = 0
        try:
            Z10 = defects["Z10"].mean()
            Z10_sum = defects["Z10"].sum()
            Z10_incidencias = len(defects[defects['Z10'] > 0])
            Z10_grav = Z10_sum / Z10_incidencias
        except:
            Z10 = 0
            Z10_incidencias = 0
            Z10_grav = 0
        try:
            Z13 = defects["Z13"].mean()
            Z13_sum = defects["Z13"].sum()
            Z13_incidencias = len(defects[defects['Z13'] > 0])
            Z13_grav = Z13_sum / Z13_incidencias
        except:
            Z13 = 0
            Z13_incidencias = 0
            Z13_grav = 0
        try:
            Z16 = defects["Z16"].mean()
            Z16_sum = defects["Z16"].sum()
            Z16_incidencias = len(defects[defects['Z16'] > 0])
            Z16_grav = Z16_sum / Z16_incidencias
        except:
            Z16 = 0
            Z16_incidencias = 0
            Z16_grav = 0
        try:
            Z19 = defects["Z19"].mean()
            Z19_sum = defects["Z19"].sum()
            Z19_incidencias = len(defects[defects['Z19'] > 0])
            Z19_grav = Z19_sum / Z19_incidencias
        except:
            Z19 = 0
            Z19_incidencias = 0
            Z19_grav = 0
        try:
            Z6 = defects["Z6"].mean()
            Z6_sum = defects["Z6"].sum()
            Z6_incidencias = len(defects[defects['Z6'] > 0])
            Z6_grav = Z6_sum / Z6_incidencias
        except:
            Z6 = 0
            Z6_incidencias = 0
            Z6_grav = 0
        try:
            Z11_5 = defects["Z11.5"].mean()
            Z11_5_sum = defects["Z11.5"].sum()
            Z11_5_incidencias = len(defects[defects['Z11.5'] > 0])
            Z11_5_grav = Z11_5_sum / Z11_5_incidencias
        except:
            Z11_5 = 0
            Z11_5_incidencias = 0
            Z11_5_grav = 0
        try:
            Z16_6 = defects["Z16.6"].mean()
            Z16_6_sum = defects["Z16.6"].sum()
            Z16_6_incidencias = len(defects[defects['Z16.6'] > 0])
            Z16_6_grav = Z16_6_sum / Z16_6_incidencias
        except:
            Z16_6 = 0
            Z16_6_incidencias = 0
            Z16_6_grav = 0
        st.header('SEVERITY RATIOS')
        col1, col2, col3, col4, col5 = st.columns(5)

        col1.metric('Z4', '{}'.format(str(Z4)[:4]))
        col2.metric("Z7", '{}'.format(str(Z7)[:4]))
        col3.metric("Z10", '{}'.format(str(Z10)[:4]))
        col4.metric("Z11", '{}'.format(str(Z11)[:4]))
        col5.metric("Z13", '{}'.format(str(Z13)[:4]))

        col1.metric('Z14', '{}'.format(str(Z14)[:4]))
        col2.metric("Z16", '{}'.format(str(Z16)[:4]))
        col3.metric("Z17", '{}'.format(str(Z17)[:4]))
        col4.metric("Z19", '{}'.format(str(Z19)[:4]))
        col5.metric("Z20", '{}'.format(str(Z20)[:4]))

        col1.metric('Z6', '{}'.format(str(Z6)[:4]))
        col2.metric("Z11_5", '{}'.format(str(Z11_5)[:4]))
        col3.metric("Z16_6", '{}'.format(str(Z16_6)[:4]))
        col4.metric("Z15_5", '{}'.format(str(Z15_5)[:4]))
        col5.metric("Z20_6", '{}'.format(str(Z20_6)[:4]))


        fig3 = px.bar(x=['Z4', "Z7", "Z10", "Z13", "Z16", "Z19"],
                      y=[Z4_incidencias, Z7_incidencias, Z10_incidencias, Z13_incidencias, Z16_incidencias,
                         Z19_incidencias], title='AERIS;NBI;NBS;TPI,TMT [Numero de incidencias]')
        fig4 = px.bar(x=["Z6", "Z11_5", "Z16_6"], y=[Z6_incidencias, Z11_5_incidencias, Z16_6_incidencias],
                      title='CONCAMEX [numero de incidencias]')
        fig5 = px.bar(x=['Z4', "Z7", "Z10", "Z13", "Z16", "Z19"],
                      y=[Z4_grav, Z7_grav, Z10_grav, Z13_grav, Z16_grav, Z19_grav],
                      title='AERIS;NBI;NBS;TPI,TMT [Gravedad media de las incidencias]')
        fig6 = px.bar(x=["Z6", "Z11_5", "Z16_6"], y=[Z6_grav, Z11_5_grav, Z16_6_grav],
                      title='CONCAMEX [gravedad media de las incidencias]')

        today = date.today()
        today_str = str(today)
        filename_mapa = 'MAPA_DEFECTOS_{}'.format(today_str)
        col1, col2 = st.columns(2)
        col1.plotly_chart(fig3)
        col2.plotly_chart(fig4)
        col1.plotly_chart(fig5)
        col2.plotly_chart(fig6)
        defects = defects.set_index('BLADE ID')
        if manufacturer in ["IND (INDUTCH)", "NBS (ACCIONA BLADES)", "TMT", "NBM (CONCAMEX)", "TPI"]:
            defects = defects[["Z4", "Z7", "Z10", "Z13", "Z16", "Z19"]]
        elif manufacturer in ["NBM (CONCAMEX)"]:
            defects = defects[["Z6", "Z11.5", "Z16.6"]]
        tabla = ff.create_table(defects, index=True, index_title='BLADE ID')
        tabla.update_layout(width=1500)
        st.markdown(get_table_download_link(defects, filename_mapa), unsafe_allow_html=True)
        st.plotly_chart(tabla)

        st.header('Defects mapping with Mold info')
        mold_list = df_tracasa_actual_mold['Mold'].unique()
        for x in mold_list:
            df_tracasa_actual_mold_x = df_tracasa_actual_mold[df_tracasa_actual_mold['Mold']==x]
            BLADES = df_tracasa_actual_mold_x['BLADE ID'].unique()
            Zs = list(df_tracasa_actual_mold_x["Blade radious"].unique())
            col = Zs
            col.append('BLADE ID')
            defects = pd.DataFrame(columns=col)
            Zs = list(df_tracasa_actual_mold_x["Blade radious"].unique())
            for i in BLADES:
                df_blade = df_tracasa_actual_mold_x[df_tracasa_actual_mold_x['BLADE ID'] == i]
                row = list()
                for x in Zs:
                    df_z = df_blade[df_blade["Blade radious"] == x]
                    df_z = df_z.reset_index(drop=True)
                    if len(df_z) == 0:
                        criticidad = None
                        row.append(criticidad)
                    else:
                        criticidad = df_z['Severity'][0]
                        row.append(criticidad)
                row.append(i)
                a_series = pd.Series(row, index=defects.columns)
                defects = defects.append(a_series, ignore_index=True)
                row.clear()
            defects = defects.replace('0 - No defect', int(0))
            defects = defects.replace('1 - Initial', int(1))
            defects = defects.replace('2 - Intermediate', int(2))
            defects = defects.replace('3 - Advanced', int(3))
            defects = defects.replace('4 - Severe', int(4))
            defects = defects.replace('5 - Critical', int(5))
            try:
                Z4 = defects["Z4"].mean()
                Z4_sum = defects["Z4"].sum()
                Z4_incidencias = len(defects[defects['Z4'] > 0])
                Z4_grav = Z4_sum / Z4_incidencias
            except:
                Z4 = 0
                Z4_incidencias = 0
                Z4_grav = 0
            try:
                Z7 = defects["Z7"].mean()
                Z7_sum = defects["Z7"].sum()
                Z7_incidencias = len(defects[defects['Z7'] > 0])
                Z7_grav = Z7_sum / Z7_incidencias
            except:
                Z7 = 0
                Z7_incidencias = 0
                Z7_grav = 0
            try:
                Z10 = defects["Z10"].mean()
                Z10_sum = defects["Z10"].sum()
                Z10_incidencias = len(defects[defects['Z10'] > 0])
                Z10_grav = Z10_sum / Z10_incidencias
            except:
                Z10 = 0
                Z10_incidencias = 0
                Z10_grav = 0
            try:
                Z13 = defects["Z13"].mean()
                Z13_sum = defects["Z13"].sum()
                Z13_incidencias = len(defects[defects['Z13'] > 0])
                Z13_grav = Z13_sum / Z13_incidencias
            except:
                Z13 = 0
                Z13_incidencias = 0
                Z13_grav = 0
            try:
                Z16 = defects["Z16"].mean()
                Z16_sum = defects["Z16"].sum()
                Z16_incidencias = len(defects[defects['Z16'] > 0])
                Z16_grav = Z16_sum / Z16_incidencias
            except:
                Z16 = 0
                Z16_incidencias = 0
                Z16_grav = 0
            try:
                Z19 = defects["Z19"].mean()
                Z19_sum = defects["Z19"].sum()
                Z19_incidencias = len(defects[defects['Z19'] > 0])
                Z19_grav = Z19_sum / Z19_incidencias
            except:
                Z19 = 0
                Z19_incidencias = 0
                Z19_grav = 0
            try:
                Z6 = defects["Z6"].mean()
                Z6_sum = defects["Z6"].sum()
                Z6_incidencias = len(defects[defects['Z6'] > 0])
                Z6_grav = Z6_sum / Z6_incidencias
            except:
                Z6 = 0
                Z6_incidencias = 0
                Z6_grav = 0
            try:
                Z11_5 = defects["Z11.5"].mean()
                Z11_5_sum = defects["Z11.5"].sum()
                Z11_5_incidencias = len(defects[defects['Z11.5'] > 0])
                Z11_5_grav = Z11_5_sum / Z11_5_incidencias
            except:
                Z11_5 = 0
                Z11_5_incidencias = 0
                Z11_5_grav = 0
            try:
                Z16_6 = defects["Z16.6"].mean()
                Z16_6_sum = defects["Z16.6"].sum()
                Z16_6_incidencias = len(defects[defects['Z16.6'] > 0])
                Z16_6_grav = Z16_6_sum / Z16_6_incidencias
            except:
                Z16_6 = 0
                Z16_6_incidencias = 0
                Z16_6_grav = 0
            st.header('SEVERITY RATIOS {}'.format(x))
            col1, col2, col3, col4, col5 = st.columns(5)

            col1.metric('Z4', '{}'.format(str(Z4)[:4]))
            col2.metric("Z7", '{}'.format(str(Z7)[:4]))
            col3.metric("Z10", '{}'.format(str(Z10)[:4]))
            col4.metric("Z11", '{}'.format(str(Z11)[:4]))
            col5.metric("Z13", '{}'.format(str(Z13)[:4]))

            col1.metric('Z14', '{}'.format(str(Z14)[:4]))
            col2.metric("Z16", '{}'.format(str(Z16)[:4]))
            col3.metric("Z17", '{}'.format(str(Z17)[:4]))
            col4.metric("Z19", '{}'.format(str(Z19)[:4]))
            col5.metric("Z20", '{}'.format(str(Z20)[:4]))

            col1.metric('Z6', '{}'.format(str(Z6)[:4]))
            col2.metric("Z11_5", '{}'.format(str(Z11_5)[:4]))
            col3.metric("Z16_6", '{}'.format(str(Z16_6)[:4]))
            col4.metric("Z15_5", '{}'.format(str(Z15_5)[:4]))
            col5.metric("Z20_6", '{}'.format(str(Z20_6)[:4]))

            fig = px.line(x=['Z4', "Z7", "Z10", "Z13", "Z16", "Z19"], y=[Z4, Z7, Z10, Z13, Z16, Z19],
                          title='AERIS;NBI;NBS;TPI,TMT')
            fig2 = px.line(x=["Z6", "Z11_5", "Z16_6"], y=[Z6, Z11_5, Z16_6], title='CONCAMEX')
            fig3 = px.bar(x=['Z4', "Z7", "Z10", "Z13", "Z16", "Z19"],
                          y=[Z4_incidencias, Z7_incidencias, Z10_incidencias, Z13_incidencias, Z16_incidencias,
                             Z19_incidencias], title='AERIS;NBI;NBS;TPI,TMT [Number of findings]')
            fig4 = px.bar(x=["Z6", "Z11_5", "Z16_6"], y=[Z6_incidencias, Z11_5_incidencias, Z16_6_incidencias],
                          title='CONCAMEX [Number of findings]')
            fig5 = px.bar(x=['Z4', "Z7", "Z10", "Z13", "Z16", "Z19"],
                          y=[Z4_grav, Z7_grav, Z10_grav, Z13_grav, Z16_grav, Z19_grav],
                          title='AERIS;NBI;NBS;TPI,TMT [Average severity per finding]')
            fig6 = px.bar(x=["Z6", "Z11_5", "Z16_6"], y=[Z6_grav, Z11_5_grav, Z16_6_grav],
                          title='CONCAMEX [Average severity per finding]')

            today = date.today()
            today_str = str(today)
            filename_mapa = 'MAPA_DEFECTOS_{}'.format(today_str)
            col1, col2 = st.columns(2)
            col1.plotly_chart(fig)
            col2.plotly_chart(fig2)
            col1.plotly_chart(fig3)
            col2.plotly_chart(fig4)
            col1.plotly_chart(fig5)
            col2.plotly_chart(fig6)
            defects = defects.set_index('BLADE ID')
            if manufacturer in ["IND (INDUTCH)", "NBS (ACCIONA BLADES)", "TMT", "NBM (CONCAMEX)", "TPI"]:
                defects = defects[["Z4", "Z7", "Z10", "Z13", "Z16", "Z19"]]
            elif manufacturer in ["NBM (CONCAMEX)"]:
                defects = defects[["Z6", "Z11.5", "Z16.6"]]
            tabla = ff.create_table(defects, index=True, index_title='BLADE ID')
            tabla.update_layout(width=1500)
            st.markdown(get_table_download_link(defects, filename_mapa), unsafe_allow_html=True)
            st.plotly_chart(tabla)

def ANALISIS_TEMPORAL(df):

    today = date.today()
    today_str = str(today)
    filename_tiempo = 'ANALISIS_TEMPORAL_1_{}'.format(today_str)
    filename_tiempo2 = 'ANALISIS_TEMPORAL_2_{}'.format(today_str)
    analisis = pd.DataFrame(columns=['Blade radious','WindFarm', 'WTG', 'Blade position','Blade type', 'Manufacturer', 'SRN', 'inspection_date','PeM','Delta Pem-Primer daño','Severity'])
    analisis2 = pd.DataFrame(columns=['Blade radious','WindFarm', 'WTG', 'Blade position', 'Blade type', 'Manufacturer', 'SRN', 'inspection_date_daño', 'inspection_date_sano','Delta Inspeccion sin daño - Inspeccion con daño','Severity'])
    with open(file='windfarms_turbines.txt', mode="r") as f:
        sql_wf = f.read()

    PeM = pd.read_sql(
        sql=sql_wf,
        con=CONEXION_SQL()
    )

    PeM = PeM[['Wind_farm','Turbine','Commissioning_Date']]
    PeM['Wind_farm'] = PeM['Wind_farm'].str.upper()
    PeM.columns = ['WindFarm','WTG','Commissioning_Date']
    wf = ['DIAMOND VISTA','RATTLESNAKE CREEK']
    PwM = PeM[PeM['WindFarm'].isin(wf)]
    df = df[df['WindFarm'].isin(wf)]

    df = df.merge(PwM, how='left', on=['WindFarm','WTG'])
    df['inspection_date'] = pd.to_datetime(df['inspection_date'], format='%Y-%m-%d')
    df['Commissioning_Date'] = pd.to_datetime(df['Commissioning_Date'], format='%Y-%m-%d')
    blades = df[ 'BLADE ID Z'].unique()
    daño = ['1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe','5 - Critical']
    for i in blades:
        df_blade = df[df['BLADE ID Z'] == i]
        df_blade = df_blade[df_blade['Severity'].isin(daño)]
        if len(df_blade) > 0:
            df_blade = df_blade.sort_values(by='inspection_date',ascending=True).reset_index(drop=True)
            df_blade = df_blade.reset_index(drop=True)
            sev = df_blade['Severity'][0]
            d1 = df_blade['inspection_date'][0]
            d2 = df_blade['Commissioning_Date'][0]
            delta = abs((d2 - d1).days)
            analisis = analisis.append({'Blade radious':df_blade['Blade radious'][0],'WindFarm':df_blade['WindFarm'][0], 'WTG':df_blade['WTG'][0], 'Blade position':df_blade['Blade position'][0],'Blade type':df_blade['Blade type'][0], 'Manufacturer':df_blade['Manufacturer'][0], 'SRN':df_blade['SRN'][0], 'inspection_date':df_blade['inspection_date'][0],'PeM':df_blade['Commissioning_Date'][0],'Delta Pem-Primer daño':delta,'Severity':sev},ignore_index=True)

    for i in blades:
        df_blade = df[df['BLADE ID Z'] == i]
        df_blade = df_blade.reset_index(drop=True)
        df_blade_sano = df_blade[df_blade['Severity'] =='0 - No defect']
        df_blade_sano = df_blade_sano.reset_index(drop=True)
        if len(df_blade_sano)>0:
            df_blade = df_blade[df_blade['Severity'].isin(daño)]
            df_blade = df_blade.reset_index(drop=True)
            if len(df_blade) > 0:
                sev = df_blade['Severity'][0]
                df_blade_sano = df_blade_sano.sort_values(by='inspection_date', ascending=False).reset_index(drop=True)
                df_blade = df_blade.sort_values(by='inspection_date', ascending=True).reset_index(drop=True)
                d1 = df_blade['inspection_date'][0]
                d2 = df_blade_sano['inspection_date'][0]
                delta = abs((d2 - d1).days)
                analisis2 = analisis2.append({'Blade radious':df_blade['Blade radious'][0],'WindFarm': df_blade['WindFarm'][0], 'WTG': df_blade['WTG'][0], 'Blade position': df_blade['Blade position'][0], 'Blade type': df_blade['Blade type'][0],'Manufacturer': df_blade['Manufacturer'][0], 'SRN': df_blade['SRN'][0],'inspection_date_daño':d1, 'inspection_date_sano':d2, 'Delta Inspeccion sin daño - Inspeccion con daño': delta,'Severity':sev},ignore_index=True)

    today = date.today()
    today = np.datetime64(today)
    analisis = analisis[analisis['inspection_date']<today]
    analisis2 = analisis2[analisis2['inspection_date_daño']< today]
    ###fig = ff.create_distplot(hist_data, group_labels)
    tabla = ff.create_table(analisis, height_constant=60)
    ###fig2 = ff.create_distplot(hist_data, group_labels)
    tabla2 = ff.create_table(analisis2, height_constant=60)


    analisis_ind = analisis[analisis['Manufacturer'] == 'IND']
    analisis_nbs = analisis[analisis['Manufacturer'] == 'NBS']

    x1 = list(analisis_ind['Delta Pem-Primer daño'])
    x2 = list(analisis_nbs['Delta Pem-Primer daño'])

    # Group data together
    hist_data = [x1, x2]

    group_labels = ['IND (INDUTCH)', 'NBS (ACCIONA BLADES)']

    # Create distplot with custom bin_size
    fig = ff.create_distplot(hist_data, group_labels, bin_size=.2)
    fig.update_layout(width=1500, height=600)

    analisis_ind2 = analisis2[analisis2['Manufacturer'] == 'IND']
    analisis_nbs2 = analisis2[analisis2['Manufacturer'] == 'NBS']
    x12 = list(analisis_ind2['Delta Inspeccion sin daño - Inspeccion con daño'])
    x22 = list(analisis_nbs2['Delta Inspeccion sin daño - Inspeccion con daño'])

    # Group data together
    hist_data2 = [x12, x22]

    group_labels2 = ['IND', 'NBS']

    # Create distplot with custom bin_size
    fig2 = ff.create_distplot(hist_data2, group_labels2, bin_size=.2)
    fig2.update_layout(width=1500, height=600)

    st.title('Analisis temporal PeM - Primer daño')
    st.title('Distplot by manufacturer')
    st.plotly_chart(fig)
    st.markdown(get_table_download_link(analisis, filename_tiempo), unsafe_allow_html=True)

    st.table(analisis)

    st.title('Analisis temporal Sin daño - Primer daño')


    st.title('Distplot by manufacturer')
    st.markdown(get_table_download_link(analisis2, filename_tiempo2), unsafe_allow_html=True)
    st.plotly_chart(fig2)
    st.table(analisis2)

    # Add histogram data


def DELTA_COMMISSIONING(df):
    today = date.today()
    today_str = str(today)


    analisis = pd.DataFrame(columns=['Blade radious','WindFarm', 'WTG', 'Blade position','Blade type', 'Manufacturer', 'SRN', 'inspection_date','PeM','Delta Pem-Primer daño','Severity'])

    with open(file='windfarms_turbines.txt', mode="r") as f:
        sql_wf = f.read()

    PeM = pd.read_sql(
        sql=sql_wf,
        con=CONEXION_SQL()
    )
    PeM = PeM[['Wind_farm','Turbine','Commissioning_Date']]
    PeM['Wind_farm'] = PeM['Wind_farm'].str.upper()
    PeM.columns = ['WindFarm','WTG','Commissioning_Date']
    df = df.merge(PeM, how='left', on=['WindFarm','WTG'])
    df['inspection_date'] = pd.to_datetime(df['inspection_date'], format='%Y-%m-%d')
    df['Commissioning_Date'] = pd.to_datetime(df['Commissioning_Date'], format='%Y-%m-%d')
    blades = df[ 'BLADE ID Z'].unique()
    daño = ['1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe','5 - Critical']
    for i in blades:
        df_blade = df[df[ 'BLADE ID Z'] == i]
        df_blade = df_blade[df_blade['Severity'].isin(daño)]
        if len(df_blade) > 0:
            df_blade = df_blade.sort_values(by='inspection_date',ascending=True).reset_index(drop=True)
            df_blade = df_blade.reset_index(drop=True)
            sev = df_blade['Severity'][0]
            d1 = df_blade['inspection_date'][0]
            d2 = df_blade['Commissioning_Date'][0]
            delta = abs((d2 - d1).days)
            analisis = analisis.append({'Blade radious':df_blade['Blade radious'][0],'WindFarm':df_blade['WindFarm'][0], 'WTG':df_blade['WTG'][0], 'Blade position':df_blade['Blade position'][0],'Blade type':df_blade['Blade type'][0], 'Manufacturer':df_blade['Manufacturer'][0], 'SRN':df_blade['SRN'][0], 'inspection_date':df_blade['inspection_date'][0],'PeM':df_blade['Commissioning_Date'][0],'Delta Pem-Primer daño':delta,'Severity':sev},ignore_index=True)

    today = np.datetime64(today)
    analisis = analisis[analisis['inspection_date']<today]
    bladetipo = analisis["Blade type"].unique()
    for item in bladetipo:
        analisis_aux = analisis[analisis["Blade type"]==item]
        analisis_aux = analisis_aux.sort_values(by="Blade radious")
        fig = px.box(analisis_aux, x="Blade radious", y="Delta Pem-Primer daño", color="Manufacturer",points="all")
        fig.update_layout(width=1200)
        st.header('PeM - 1st damage box plot - {}'.format(item))
        st.plotly_chart(fig)
        st.dataframe(analisis_aux, width=1200)
        filename_pem = 'PEM_first_damage'
        st.markdown(get_table_download_link(analisis_aux, filename_pem), unsafe_allow_html=True)


def DELTA_SEV(df_tracasa):

    df_tracasa = df_tracasa.replace('0 - No defect', int(0))
    df_tracasa = df_tracasa.replace('1 - Initial', int(1))
    df_tracasa = df_tracasa.replace('2 - Intermediate', int(2))
    df_tracasa = df_tracasa.replace('3 - Advanced', int(3))
    df_tracasa = df_tracasa.replace('4 - Severe', int(4))
    df_tracasa = df_tracasa.replace('5 - Critical', int(5))
    type_list = df_tracasa['Blade type'].unique()
    for i in type_list:
        df_tracasa_type = df_tracasa[df_tracasa['Blade type']==i]
        df_tracasa_type = df_tracasa_type.reset_index(drop=True)
        Z_list = df_tracasa_type['Blade radious'].unique()
        for x in Z_list:
            df = pd.DataFrame(columns=['Sev origen','Delta Sev','Delta dias','id'])
            df_tracasa_type_Z = df_tracasa_type[df_tracasa_type['Blade radious'] == x]
            blades = df_tracasa_type_Z['BLADE ID'].unique()
            for p in blades:
                df_tracasa_type_Z_blade = df_tracasa_type_Z[df_tracasa_type_Z['BLADE ID']==p]
                if len(df_tracasa_type_Z) > 1:
                    df_tracasa_type_Z_blade = df_tracasa_type_Z_blade.sort_values(by = 'inspection_date',ascending=True)
                    df_tracasa_type_Z_blade = df_tracasa_type_Z_blade.reset_index(drop=True)
                    for item in range(1,len(df_tracasa_type_Z_blade)):
                            sev_origem = df_tracasa_type_Z_blade['Severity'][(item-1)]
                            primera_inp = df_tracasa_type_Z_blade['inspection_date'][item-1]
                            sev = df_tracasa_type_Z_blade['Severity'][item]
                            insp = df_tracasa_type_Z_blade['inspection_date'][item]
                            delta_days = (insp-primera_inp).days
                            delta_sev = sev-sev_origem
                            if delta_sev>0:
                                df = df.append({'Sev origen':sev_origem,'Delta Sev':delta_sev,'Delta dias':delta_days,'id':p},ignore_index=True)
            if len(df)>0:

                fig = px.scatter(df, x="Delta dias", y="Delta Sev", color="Sev origen",hover_name='id')
                fig.update_layout(width=1200)
                st.header('Delta days vs Delta Severity | {} - {}'.format(i,x))
                st.plotly_chart(fig)

                today = date.today()
                today_str = str(today)
                filename = 'DELTA_SEVERIDAD_{}'.format(today_str)
                st.markdown(get_table_download_link(df, filename), unsafe_allow_html=True)
                st.dataframe(df,width=1200)




def EDA_P(df_tracasa):
    df_p = df_tracasa
    for i in range(len(df_p)):
        if pd.isnull(df_p['observations'][i]):
            df_p = df_p.drop([i], axis=0)
    df_p = df_p.reset_index(drop = True)

    fig = px.bar(df_p, x="WindFarm", y="observations", color="observations")
    fig2 = px.bar(df_p, x="Blade type", y="observations", color="observations")
    st.header('MAPPING BY WINDFARM')
    fig.update_layout(width=1200)
    st.plotly_chart(fig)
    st.header('MAPPING BY BLADE TYPE')
    fig2.update_layout(width=1200)
    st.plotly_chart(fig2)


def RCA(windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df,filtros_aplicados):
    df = FILTER(windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df)
    df = df.reset_index(drop=True)
    df['ARRUGAS'] = 'unknown'
    for i in range(len(df)):
        if df['Wrinkle or step?'][i] == 'No':
            df['ARRUGAS'][i] = 'No Wrinkle or step?'
        elif df['Wrinkle or step?'][i] == 'Yes':
            if df["H"][i]<2:
                df['ARRUGAS'][i] = 'Wrincle H<2'
            elif 2<df['H'][i]<5:
                if df['AR'][i] >10:
                    df['ARRUGAS'][i] = 'Wrincle 2<H<5 AR OK'
                elif df['AR'][i] <=19:
                    df['ARRUGAS'][i] = 'Wrincle 2<H<5 AR NO OK'
            elif df['H'][i]>5:
                df['ARRUGAS'][i] = 'Wrincle H>5'
        if pd.isnull(df['Wrinkle or step?'][i]):
            df['ARRUGAS'][i] = 'No Wrinkle or step?'
    df1 = df.groupby(['Blade radious',"Red coloured area?",'Severity']).size().sort_values(ascending=False).reset_index(name='count')
    Zs = ['Z4','Z7','Z10','Z13','Z16','Z19']
    sano = ['0 - No defect']
    enfermo = ['1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe',]
    df1 = df1[df1['Blade radious'].isin(Zs)]
    df1_sano = df1[df1['Severity'].isin(sano)]
    df1_enfermo = df1[df1['Severity'].isin(enfermo)]
    fig = px.bar(df1, x="Red coloured area?", y="count", color="Red coloured area?", barmode="group", facet_col='Blade radious',title=filtros_aplicados)
    fig.update_layout(width=1200, height=800)
    fig2 = px.pie(df1_sano, values='count', names='Red coloured area?',title='Sin daños  {}'.format(filtros_aplicados))
    fig21 = px.pie(df1_enfermo, values='count', names='Red coloured area?',title='Con daños {}'.format(filtros_aplicados))
    st.header('ADHESIVO INTERLAMINAR')
    fig.update_layout(width=1500)
    st.plotly_chart(fig)
    fig2.update_layout(width=1500)
    st.plotly_chart(fig2)
    fig21.update_layout(width=1500)
    st.plotly_chart(fig21)
    df1 = df1.replace('0 - No defect', int(0))
    df1 = df1.replace('1 - Initial', int(1))
    df1 = df1.replace('2 - Intermediate', int(2))
    df1 = df1.replace('3 - Advanced', int(3))
    df1 = df1.replace('4 - Severe', int(4))
    df1 = df1.replace('5 - Critical', int(4))
    st.table(df1)


    df2 = df.groupby(['Blade radious',"Excessive adhesive?",'Severity']).size().sort_values(ascending=False).reset_index(name='count')
    Zs = ['Z4','Z7','Z10','Z13','Z16','Z19']
    df2 = df2[df2['Blade radious'].isin(Zs)]
    df2_sano = df2[df2['Severity'].isin(sano)]
    df2_enfermo = df2[df2['Severity'].isin(enfermo)]
    fig3 = px.bar(df2, x="Excessive adhesive?", y="count", color="Excessive adhesive?", barmode="group", facet_col='Blade radious',title=filtros_aplicados)
    fig3.update_layout(width=1200, height=800)
    fig4 = px.pie(df2_sano, values='count', names="Excessive adhesive?",title='Sin daños  {}'.format(filtros_aplicados))
    fig41 = px.pie(df2_enfermo, values='count', names="Excessive adhesive?",title='Con daños  {}'.format(filtros_aplicados))
    st.header('ESPESOR ADHESIVO')
    fig3.update_layout(width=1500)
    st.plotly_chart(fig3)
    fig4.update_layout(width=1500)
    st.plotly_chart(fig4)
    fig41.update_layout(width=1500)
    st.plotly_chart(fig41)
    df2 = df2.replace('0 - No defect', int(0))
    df2 = df2.replace('1 - Initial', int(1))
    df2 = df2.replace('2 - Intermediate', int(2))
    df2 = df2.replace('3 - Advanced', int(3))
    df2 = df2.replace('4 - Severe', int(4))
    df2 = df2.replace('5 - Critical', int(5))
    st.table(df2)
    df3 = df[df['ARRUGAS']!='unknown']
    df3a = df3
    df3a['ARRUGAS_GROUP'] = 'unknown'
    df3a = df3a.reset_index(drop=True)

    for i in range(len(df3a)):
        if df3a['ARRUGAS'][i] == 'No Wrinkle or step?':
            df3a['ARRUGAS_GROUP'][i] = 'OK'
        elif df3a['ARRUGAS'][i] == 'Wrincle H<2':
            df3a['ARRUGAS_GROUP'][i] = 'OK'
        elif df3a['ARRUGAS'][i] == 'Wrincle 2<H<5 AR OK':
            df3a['ARRUGAS_GROUP'][i] = 'OK'
        elif df3a['ARRUGAS'][i] == 'Wrincle 2<H<5 AR NO OK':
            df3a['ARRUGAS_GROUP'][i] = 'NO OK'
        elif df3a['ARRUGAS'][i] == 'Wrincle H>5':
            df3a['ARRUGAS_GROUP'][i] = 'NO OK'



    df3 = df3.groupby(['Blade radious',"ARRUGAS",'Severity']).size().sort_values(ascending=False).reset_index(name='count')
    Zs = ['Z4','Z7','Z10','Z13','Z16','Z19']
    df3 = df3[df3['Blade radious'].isin(Zs)]
    df3_sano = df3[df3['Severity'].isin(sano)]
    df3_enfermo = df3[df3['Severity'].isin(enfermo)]
    fig5 = px.bar(df3, x="ARRUGAS", y="count", color="ARRUGAS", barmode="group", facet_col='Blade radious',title=filtros_aplicados)
    fig5.update_layout(width=1200, height=800)
    fig6 = px.pie(df3_sano, values='count', names="ARRUGAS",title='Sin daños  {}'.format(filtros_aplicados))
    fig61 = px.pie(df3_enfermo, values='count', names="ARRUGAS",title='Con daños  {}'.format(filtros_aplicados))

    df3a = df3a.groupby(['Blade radious', "ARRUGAS_GROUP", 'Severity']).size().sort_values(ascending=False).reset_index(
        name='count')

    Zs = ['Z4', 'Z7', 'Z10', 'Z13', 'Z16', 'Z19']
    df3a = df3a[df3a['Blade radious'].isin(Zs)]
    df3a_sano = df3a[df3a['Severity'].isin(sano)]
    df3a_enfermo = df3a[df3a['Severity'].isin(enfermo)]
    fig7 = px.pie(df3a_sano, values='count', names="ARRUGAS_GROUP", title='Sin daños  {}'.format(filtros_aplicados))
    fig71 = px.pie(df3a_enfermo, values='count', names="ARRUGAS_GROUP", title='Con daños  {}'.format(filtros_aplicados))
    st.header('ARRUGAS/ ESCALONES')
    fig5.update_layout(width=1500)
    st.plotly_chart(fig5)
    fig6.update_layout(width=1500)
    st.plotly_chart(fig6)
    fig61.update_layout(width=1500)
    st.plotly_chart(fig61)
    fig7.update_layout(width=1500)
    st.plotly_chart(fig7)
    fig71.update_layout(width=1500)
    st.plotly_chart(fig71)


    df3 = df3.replace('0 - No defect', int(0))
    df3 = df3.replace('1 - Initial', int(1))
    df3 = df3.replace('2 - Intermediate', int(2))
    df3 = df3.replace('3 - Advanced', int(3))
    df3 = df3.replace('4 - Severe', int(4))
    df3 = df3.replace('5 - Critical', int(5))
    st.table(df3)





###    PAGE SET UP   ###
def MOLDE_RCA(df_tracasa_actual):
    df = df_tracasa_actual.reset_index(drop=True)

    for i in range(len(df)):
        if pd.isnull(df['Mold'][i]):
            df = df.drop([i], axis=0)

    df = df.reset_index(drop=True)


    df['SRN'] = df['SRN'].astype(float)
    df['SRN'] = df['SRN'].astype(int)
    df = df.replace('0 - No defect',int(0))
    df = df.replace('1 - Initial', int(1))
    df = df.replace('2 - Intermediate', int(2))
    df = df.replace('3 - Advanced', int(3))
    df = df.replace('4 - Severe', int(4))
    df = df.replace('5 - Critical', int(5))
    df['Severity'] = df['Severity'].astype(int)
    with st.expander('View raw data'):
        st.dataframe(df)


    man = df['Manufacturer'].unique()
    for i in man:
        aux = df[df['Manufacturer'] == i]
        m = list(aux['Mold'].unique())
        for x in m:
            aux2 = aux[aux['Mold']==x]
            color_discrete_map_sev = {'1': 'blue', '2': 'yellow', '3': 'orange', '4': 'red',
                                      '5': 'black'}
            fig = px.scatter(aux2, x="SRN", y="Severity", color="Blade radious")
            fig.update_layout(width=1000, height=400)
            st.title('{} - {}'.format(i,x))
            st.plotly_chart(fig)



    df1 = df.groupby(['Blade radious', "Mold", 'Severity']).size().sort_values(
        ascending=False).reset_index(name='count')
    Zs = ['Z4','Z7', 'Z10', 'Z13','Z16','Z19']

    df1 = df1[df1['Blade radious'].isin(Zs)]

    fig3 = px.bar(df1, x="Mold", y="count", color="Severity", barmode="group",facet_col='Blade radious')
    fig3.update_layout(width=1000, height=400)
    st.plotly_chart(fig3)

def MOLDE_MAX_SEV(df):

    molde_sev = pd.DataFrame(columns=['MOLD','MAX SEV','MANUFACTURER','BLADE TYPE'])
    df = df.replace('0 - No defect',int(0))
    df = df.replace('1 - Initial', int(1))
    df = df.replace('2 - Intermediate', int(2))
    df = df.replace('3 - Advanced', int(3))
    df = df.replace('4 - Severe', int(4))
    df = df.replace('5 - Critical', int(5))
    blades = df['BLADE ID'].unique()
    for i in blades:
        df_blade = df[df['BLADE ID']==i]
        df_blade = df_blade.reset_index(drop=True)
        sev = max(df_blade['Severity'])
        molde = df_blade['Mold'][0]
        man = df_blade['Manufacturer'][0]
        type_pala = df_blade['Blade type'][0]
        molde_sev = molde_sev.append({'MOLD':molde,'MAX SEV':sev,'MANUFACTURER':man,'BLADE TYPE':type_pala}, ignore_index=True)
    molde_sev = molde_sev.fillna('NO INFO')
    molde_sev = molde_sev[molde_sev['MAX SEV']!=0]
    molde_sev = molde_sev.replace(int(1),'1 - Initial')
    molde_sev = molde_sev.replace(int(2),'2 - Intermediate')
    molde_sev = molde_sev.replace(int(3),'3 - Advanced')
    molde_sev = molde_sev.replace(int(4),'4 - Severe')
    molde_sev = molde_sev.replace(int(5),'5 - Critical')
    manu = molde_sev['MANUFACTURER'].unique()
    for x in manu:
        st.title('{}'.format(x))
        molde_sev_man = molde_sev[molde_sev['MANUFACTURER']==x]
        tipo = molde_sev_man['BLADE TYPE'].unique()
        for t in tipo:
            st.subheader('{}'.format(t))
            molde_sev_man_tipo = molde_sev_man[molde_sev_man['BLADE TYPE']==t]
            molde_sev_man_tipo = molde_sev_man_tipo.groupby(['MOLD', 'MAX SEV']).size().reset_index(name='counts')
            color_discrete_map_sev = {'1 - Initial': 'blue', '2 - Intermediate': 'yellow', '3 - Advanced': 'orange', '4 - Severe': 'red','5 - Critical': 'black'}
            fig = px.bar(molde_sev_man_tipo, x="MOLD", y="counts", color="MAX SEV",color_discrete_map=color_discrete_map_sev,color_continuous_scale=None,height=800)
            fig.update_layout(width=1200)
            st.table(molde_sev_man_tipo)
            st.plotly_chart(fig)


@st.experimental_memo
def TRAZABILIDAD_CAMBIO_PALA(df):
    sustituciones = pd.DataFrame(columns=['Windfarm','WTG','Old blade','New blade'])
    df['Blade position str'] = df['Blade position'].astype(str)
    df['BLADE ID REPLACEMENT'] = df['WindFarm'] + '_' + df['WTG']  + '_' + df['Blade position str']
    listado_palas = list(df['BLADE ID REPLACEMENT'].unique())
    for i in listado_palas:
        busqueda = df[df['BLADE ID REPLACEMENT']==i]
        n_srn = list(busqueda['BLADE ID'].unique())
        if len(n_srn)>1:
            busqueda = busqueda.sort_values(by='inspection_date',ascending=False)
            busqueda = busqueda.reset_index(drop=True)
            pala_nueva = busqueda['BLADE ID'][0]
            pala_vieja = busqueda['BLADE ID'][len(busqueda)-1]
            wf = busqueda['WindFarm'][0]
            wtg = busqueda['WTG'][0]
            sustituciones = sustituciones.append({'Windfarm':wf,'WTG':wtg ,'Old blade':pala_vieja,'New blade':pala_nueva}, ignore_index=True)

    return sustituciones





def main():

    st.sidebar.title('Technical Issue 22 | PRO')
    df_tracasa = LECTURA_BD()
    df_tracasa['SRN'] = df_tracasa['SRN'].astype(int)
    df_tracasa = df_tracasa.replace('NO        ', 'NO')
    df_tracasa = df_tracasa.replace('YES       ', 'YES')
    df_tracasa = df_tracasa.replace('IND (INDUTCH)', 'IND')
    df_tracasa = df_tracasa.replace('NBM (CONCAMEX)', 'NBM')
    df_tracasa = df_tracasa.replace('NBS (ACCIONA BLADES)', 'NBS')



    df_cliente, df_mold,df_cliente_wtg = ETL_REGION_CLIENTE()

    df_tracasa = df_tracasa.merge(df_cliente, how='left', on='WindFarm')
    df_raw_bbdd = df_tracasa
    df_raw_bbdd['SRN'] = df_raw_bbdd['SRN'].astype(int)
    df_tracasa = df_tracasa.merge(df_mold, how='left', on=['Blade type', 'Manufacturer', 'SRN'])

    df_tracasa['SRN'] = df_tracasa['SRN'].astype(str)
    df_tracasa['BLADE ID'] = df_tracasa['Blade type'] + '_' + df_tracasa['Manufacturer'] + '_' + df_tracasa['SRN']
    df_tracasa['BLADE ID Z'] = df_tracasa['Blade type'] + '_' + df_tracasa['Manufacturer'] + '_' + df_tracasa['SRN'] + '_' + df_tracasa['Blade radious']
    df_tracasa['inspection_date_str'] = df_tracasa['inspection_date'].astype(str)
    df_tracasa['BLADE ID INP'] = df_tracasa['Blade type'] + '_' + df_tracasa['Manufacturer'] + '_' + df_tracasa['SRN'] + '_' + (df_tracasa['inspection_date_str'])
    today = date.today()

    hace_una_semana = today + timedelta(weeks=-1)
    hace_una_semana = np.datetime64(hace_una_semana)
    df_tracasa_actual_semana = df_tracasa[df_tracasa['upload_date']<hace_una_semana]


    df_tracasa_actual = ETL_ESTADO_ACTUAL(df_tracasa)
    df_tracasa_actual = CRITICIDAD_PALA(df_tracasa_actual)
    df_tracasa_actual = ARRUGAS(df_tracasa_actual)
    df_tracasa_actual = FECHAS_NEXT_INSP(df_tracasa_actual)
    df_tracasa_actual = NEXT_INSP(df_tracasa_actual)
    df_tracasa_actual = SEGUNDA_CRITICIDAD(df_tracasa_actual)

    df_tracasa_actual_semana = ETL_ESTADO_ACTUAL_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = CRITICIDAD_PALA_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = ARRUGAS_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = FECHAS_NEXT_INSP_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = NEXT_INSP_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = SEGUNDA_CRITICIDAD_SEM(df_tracasa_actual_semana)

    today = date.today()
    today_str = str(today)
    AFECTADAS,CRITICAS,REPARAR,INSPECCIONADAS,insp,REPARADAS,PARO,cero_KPI,uno_KPI,dos_KPI,tres_KPI,cuatro_KPI,cinco_KPI,AFECTADAS_DELTA,CRITICAS_DELTA,REPARAR_DELTA,REPARADAS_DELTA,PARO_DELTA,INSPECCIONADAS_DELTA,insp_DELTA,cero_KPI_DELTA,uno_KPI_DELTA,dos_KPI_DELTA,tres_KPI_DELTA,cuatro_KPI_DELTA,cinco_KPI_DELTA,dif3,dif4,dif5 = KPI(df_tracasa_actual,df_tracasa,df_tracasa_actual_semana)

    replacement = TRAZABILIDAD_CAMBIO_PALA(df_tracasa_actual)
    palas_viejas = list(replacement['Old blade'].unique())
    ###   BUTTONS    ###


    notificaciones_pendientes = 0

    if tres_KPI_DELTA !=0:
        notificaciones_pendientes = notificaciones_pendientes + 1
    if cuatro_KPI_DELTA !=0:
        notificaciones_pendientes = notificaciones_pendientes + 1
    if cinco_KPI_DELTA !=0:
        notificaciones_pendientes = notificaciones_pendientes + 1


    ###   BUTTONS    ###
    while notificaciones_pendientes>0:
        with st.sidebar.expander("Notifications ({})".format(notificaciones_pendientes)):
            if tres_KPI_DELTA != 0:
                st.write('Weekly delta of advanced blades ({})'.format(tres_KPI_DELTA))
                st.write('Debutant blades with advanced damages')
                st.write(dif3)
            if cuatro_KPI_DELTA != 0:
                st.write('Weekly delta of severe bladeS ({})'.format(cuatro_KPI_DELTA))
                st.write('Debutant blades with severe damages')
                st.write(dif4)

            if cinco_KPI_DELTA != 0:
                st.write('Weekly delta of critical blades  ({})'.format(cinco_KPI_DELTA))
                st.write('Debutant blades with critical damages')
                st.write(dif5)


            notificaciones_pendientes = 0
    st.sidebar.title('FILTERS:')
    wf_list = sorted(list(df_tracasa['WindFarm'].unique()))
    wf_list.append('<all>')
    windfarm = st.sidebar.multiselect('WINDFARM', wf_list,default=['<all>'])
    df_tracasa_wf = df_tracasa[df_tracasa['WindFarm'].isin(windfarm)]
    wtg_list = sorted(list(df_tracasa_wf['WTG'].unique()))
    wtg_list.append('<all>')
    wtg = st.sidebar.multiselect('WTG', wtg_list,default=['<all>'])
    df_tracasa_wf_wtg = df_tracasa_wf[df_tracasa_wf['WTG'].isin(wtg)]
    blade_type_list = sorted(list(df_tracasa['Blade type'].unique()))
    blade_type_list.append('<all>')
    blade_type = st.sidebar.multiselect('BLADE TYPE', blade_type_list,default=['<all>'])
    manufacturer_list = sorted(list(df_tracasa['Manufacturer'].unique()))
    manufacturer_list.append('<all>')
    manufacturer = st.sidebar.multiselect('MANUFACTURER', manufacturer_list,default=['<all>'])
    srn_list = sorted(list(df_tracasa['SRN'].unique()))
    srn_list.append('<all>')
    srn = st.sidebar.multiselect('SRN', srn_list,default=['<all>'])
    blade_criticidad = st.sidebar.multiselect('BLADE CRITICITY', ['0 - No defect','1 - Initial','2 - Intermediate','3 - Advanced','4 - Severe','5 - Critical','<all>'],default=['<all>'])
    client = sorted(list(df_cliente_wtg['CUSTOMER'].unique()))
    client.append('<all>')
    CLIENTE = st.sidebar.multiselect('CUSTOMER', client,default=['<all>'])

    ###    MAIN   ###


    last = st.sidebar.radio('DDBB Settings',['Raw','Last'])

    windfarm_selected = ''
    for i in windfarm:
        windfarm_selected = windfarm_selected + str(i) +'_'

    wtg_selected = ''
    for i in wtg:
        wtg_selected = wtg_selected + str(i) +'_'

    blade_selected = ''
    for i in blade_type:
        blade_selected = blade_selected + str(i) +'_'

    manufacturer_selected = ''
    for i in manufacturer:
        manufacturer_selected = manufacturer_selected + str(i) + '_'

    srn_selected = ''
    for i in srn:
        srn_selected = srn_selected + str(i) + '_'

    blade_criticidad_selected = ''
    for i in blade_criticidad:
        blade_criticidad_selected = blade_criticidad_selected + str(i) + '_'



    filtros_aplicados = windfarm_selected +'_'+blade_selected+'_'+manufacturer_selected +'_'+today_str
    SEARCH = st.sidebar.button('SEARCH')


    if last == 'Last':
        if SEARCH:
            try:
                df_db = FILTER(windfarm, wtg, blade_type, manufacturer, srn,blade_criticidad, df_tracasa_actual)
                df_db = df_db[["WindFarm","WTG","Blade position","Blade type","Manufacturer","SRN","inspection_date","Blade radious","Severity","Blade severity",'GLOBAL Blade severity',"Red coloured area?","Excessive adhesive?","Wrinkle or step?","H","L","AR","Criterio CS","Criterio Circular","Damage description","Z ini (m)","Z end (m)","Defect max spanwise dimension","Defect max chordwise","service_provider","observations","CUSTOMER","COUNTRY","REGION","Mold","BLADE ID","BLADE ID Z","inspection_folder",'PROXIMA_INSP']]

                df_db = df_db.replace('0 - No defect', int(0))
                df_db = df_db.replace('1 - Initial', int(1))
                df_db = df_db.replace('2 - Intermediate', int(2))
                df_db = df_db.replace('3 - Advanced', int(3))
                df_db = df_db.replace('4 - Severe', int(4))
                df_db = df_db.replace('5 - Critical', int(5))
                df_db = df_db.drop_duplicates()
                filename = 'QUERY_{}'.format(today_str)
                st.markdown(get_table_download_link(df_db, filename), unsafe_allow_html=True)
                st.dataframe(df_db,height=2000)
                with st.expander("See explanation"):
                    st.write("""""")

            except:
                st.warning('Error...')
    if last == 'Raw':
        if SEARCH:
            a = '<all>'
            df_db = FILTER(windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df_tracasa)
            df_db = df_db.replace('0 - No defect', int(0))
            df_db = df_db.replace('1 - Initial', int(1))
            df_db = df_db.replace('2 - Intermediate', int(2))
            df_db = df_db.replace('3 - Advanced', int(3))
            df_db = df_db.replace('4 - Severe', int(4))
            df_db = df_db.replace('5 - Critical', int(5))
            df_db = df_db.drop_duplicates()
            df_db = df_db.reset_index(drop = True)
            filename = 'QUERY_{}'.format(today_str)
            st.markdown(get_table_download_link(df_db, filename), unsafe_allow_html=True)
            st.dataframe(df_db,height=2000)
            with st.expander("See explanation"):
                st.write("""""")
    df_tracasa = df_tracasa.drop_duplicates()

    KPI_b = st.sidebar.button('KPI')
    if KPI_b:
        st.title('PROJECT KPIs')


        col1, col2, col3,col33 = st.columns(4)
        col1.metric('TOTAL NUMBER OF INSPECTIONS (CUMULATIVE)', '{}'.format(INSPECCIONADAS),'{}'.format(INSPECCIONADAS_DELTA))
        col2.metric('INSPECTED BLADES', '{}'.format(insp),'{}'.format(insp_DELTA))
        col3.metric("AFFECTED BLADES NOW", '{}'.format(AFECTADAS),'{}'.format(AFECTADAS_DELTA),delta_color="inverse")
        col33.metric("REPAIRED BLADES (MIGHT BE RE-AFFECTED)", '{}'.format(REPARADAS),'{}'.format(REPARADAS_DELTA))



        col4, col5, col6, col7, col8 = st.columns(5)
        col4.metric('1 - Initial', '{}'.format(uno_KPI), '{}'.format(uno_KPI_DELTA),delta_color="inverse")
        col5.metric('2 - Intermediate', '{}'.format(dos_KPI), '{}'.format(dos_KPI_DELTA),delta_color="inverse")
        col6.metric('3 - Advanced', '{}'.format(tres_KPI), '{}'.format(tres_KPI_DELTA), delta_color="inverse")
        col7.metric('4 - Severe', '{}'.format(cuatro_KPI), '{}'.format(cuatro_KPI_DELTA),delta_color="inverse")
        col8.metric('5 - Critical', '{}'.format(cinco_KPI), '{}'.format(cinco_KPI_DELTA),delta_color="inverse")


        figpie = go.Figure(data=[go.Pie(
            labels=['0 - No defect', '1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'],
            values=[cero_KPI, uno_KPI, dos_KPI, tres_KPI, cuatro_KPI, cinco_KPI], hole=.4,
            marker_colors=['#2ca02c', 'blue', 'yellow', 'orange', 'red', 'black'], sort=False)])
        figpie.update_layout(
            title='PROJECT STATUS' + '_' + today_str,
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
            )
        )
        figpie.update_layout(width=1200)
        st.plotly_chart(figpie)
        mapa = ISO(df_tracasa_actual)
        mapa.update_layout(width=1200)
        st.plotly_chart(mapa)
        with st.expander("See explanation"):
            st.subheader('Figures show actual data (not historical one).')
            st.write('Ex: “KPI for critical blades” = 0 means that there are currently no reported blades with a critical crack pending repair. Nevertheless, there might have been several blades in the past with critical cracks but already repaired.')

            st.subheader('Figures categorized per severity show blades, not issues; “worst blade damage severity” is shown.')
            st.write('Ex: If one blade has several findings (Z4 severity 0, Z7 severity 2, Z10 severity 3, Z13 severity 3, Z16 severity 1, Z19 severity 0), the blade is categorized with the worst damage. In this case, severity 3.')

            st.subheader('Deltas show absolute changes in the data base for the last seven days.')
            st.write('Ex: “Delta KPI for advanced blades” = ñ 5 means that seven days before there were 5 less blades categorized as “3” in the data base. There might be more than 5 blades becoming Advanced, but the delta shows absolute changes. For instance, it might be possible that 3 blades have changed from category 3 to category 4; and additionally 8 blades with lighter or unexisting damages have progressed to category 3. In total: -3+8 = +5 blades in category 3.')

            st.subheader('Map.')
            st.write('It represents the number of affected blades per country, not site. It just gives a rough idea of where the problem is.')
            st.write('For reporting, it must be taken into account that all internal blade inspections performed by GDES in US and Canada during the last years due to TI-11/12 should be also considered. There are almost no findings in all AW61.2-2 old blades from NBS and any in those from TPI."')

    REPORT = st.sidebar.button('GENERATE REPORT ')
    if REPORT:
        REPORT_GENERATION(windfarm[0],df_tracasa,df_cliente_wtg,df_tracasa_actual,replacement)
        with st.expander("See explanation"):
            st.write("""""")
    REPORT_CLIENT = st.sidebar.button('CUSTOMER REPORT')
    if REPORT_CLIENT:
        REPORT_GENERATION_CLIENT(df_tracasa, df_cliente_wtg, df_tracasa_actual,CLIENTE[0])
        with st.expander("See explanation"):
            st.write("""""")
    LISTA_CRITICAS = st.sidebar.button('LIST OF BLADES TO REPAIR')
    if LISTA_CRITICAS:
        df_criticas = df_tracasa_actual
        c = ['3 - Advanced','4 - Severe','5 - Critical']
        df_criticas = df_criticas[df_criticas['Severity'].isin(c)]
        df_criticas = df_criticas[['WindFarm','WTG','Blade position','Blade type','Manufacturer','SRN','Blade severity','GLOBAL Blade severity']]
        df_criticas = df_criticas.replace('0 - No defect', int(0))
        df_criticas = df_criticas.replace('1 - Initial', int(1))
        df_criticas = df_criticas.replace('2 - Intermediate', int(2))
        df_criticas = df_criticas.replace('3 - Advanced', int(3))
        df_criticas = df_criticas.replace('4 - Severe', int(4))
        df_criticas = df_criticas.replace('5 - Critical', int(5))
        df_criticas = df_criticas.drop_duplicates()
        today = date.today()
        today_str = str(today)
        filename_r = 'PALAS A REPARAR LISTA {}'.format(today_str)
        st.markdown(get_table_download_link(df_criticas, filename_r), unsafe_allow_html=True)
        st.dataframe(df_criticas,1100,20000)
        with st.expander("See explanation"):
            st.write("""As of November 23, the criteria for including one blade in the REPAIR LIST is as simple as having at least one TI-22 finding categorized as "3-Advanced" or greater in any blade radius. Nevertheless, once a blade is included in the repair list, not only damages categorized as "3-Advanced" but all findings in the LE flange must be repaired. If resources are limited, it must be guaranteed that at least findings in Z10 and Z13 are properly addressed (+4m when talking about AW68.7 blade type).

                        Cumulative severity (severities in all blade radius summed) can be used for prioritization.""")

    DEFECTS_MAP = st.sidebar.button('DEFECT MAPPING')
    MOLDE_CHECK = st.sidebar.checkbox('Discriminate by Mold')
    if DEFECTS_MAP:
        MAPA_DEFECTOS(df_tracasa_actual,windfarm, wtg, blade_type, manufacturer,srn,blade_criticidad,filtros_aplicados,MOLDE_CHECK)
        with st.expander("See explanation"):
            st.write("""""")

    GANTT = st.sidebar.button('INSPECTION SCHEDULE')

    if GANTT:
        df_cal = FILTER(windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df_tracasa_actual)
        st.write('GANTT CHART')
        CALENDAR_NEW(df_cal)
        with st.expander("See explanation"):
            st.write("""""")

    TIEMPO = st.sidebar.button('TIME EVOLUTION ANALYSIS')
    if TIEMPO:
        ANALISIS_TEMPORAL(df_tracasa)
        with st.expander("See explanation"):
            st.write("""""")
    DELTA = st.sidebar.button('DELTA SEVERITY')
    if DELTA:
        DELTA_SEV(df_tracasa)
        with st.expander("See explanation"):
            st.write("""""")
    COMMISIONING = st.sidebar.button('Pem - 1st DAMAGE')
    if COMMISIONING:
        DELTA_COMMISSIONING(df_tracasa)
        with st.expander("See explanation"):
            st.write("""""")
    RCA_a = st.sidebar.button('RCA')
    if RCA_a:
        RCA(windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df_tracasa_actual,filtros_aplicados)
        with st.expander("See explanation"):
            st.write("""""")
    RCA_MOLDE = st.sidebar.button('MOLD RCA')
    if RCA_MOLDE:
        MOLDE_RCA(df_tracasa_actual)
        with st.expander("See explanation"):
            st.write("""""")
    REP = st.sidebar.button('EDA - REPAIR')
    if REP:
        df_rep = df_tracasa[df_tracasa['observations']=='Existing repair (good status)']
        manu = df_rep['Manufacturer'].unique()
        for x in manu:
            df_rep_manu = df_rep[df_rep['Manufacturer']==x]
            serial_number = df_rep_manu['SRN'].unique()
            srn_rep_df = pd.DataFrame(columns=['SRN','Count','Blade type'])
            for i in serial_number:
                df_rep_srn = df_rep_manu[df_rep_manu['SRN']==i]
                df_rep_srn = df_rep_srn.reset_index(drop=True)
                count = len(df_rep_srn['Blade radious'].unique())
                bt = df_rep_srn['Blade type'][0]
                srn_rep_df = srn_rep_df.append({'SRN':i,'Count':count,'Blade type':bt},ignore_index=True)
            srn_rep_df['SRN'] = srn_rep_df['SRN'].astype(int)
            fig = px.scatter(srn_rep_df, x="SRN", y="Count", color="Blade type")
            fig2 = ff.create_table(srn_rep_df)
            fig.update_layout(width=1200, height=800)
            fig2.update_layout(width=1000)
            st.header('EXPLORATORY DATA ANALYSIS - REPAIR | {}'.format(x))
            st.plotly_chart(fig)
            st.plotly_chart(fig2)
        with st.expander("See explanation"):
            st.write("""""")

    OTHERS = st.sidebar.button('EDA - OTHERS')
    if OTHERS:
        other_list = ['Others (2)','Others (1)']
        df_other = df_tracasa[df_tracasa['Blade radious'].isin(other_list)]

        manu = df_other['Manufacturer'].unique()
        for i in manu:
            df_other_manu = df_other[df_other['Manufacturer']==i]
            fig = px.histogram(df_other_manu, x="Z ini (m)", color="WindFarm",nbins=30)
            fig.update_layout(width=1200, height=800)
            fig2 = ff.create_table(df_other_manu)
            fig2.update_layout(width=1200)
            st.header('EDA - OTHERS | {}'.format(i))
            st.plotly_chart(fig)
            st.dataframe(df_tracasa_actual,width=1200)
        with st.expander("See explanation"):
            st.write("""""")
    EDA_P_P = st.sidebar.button('EDA - OBSERVATIONS')
    if EDA_P_P:
        EDA_P(df_tracasa)
        with st.expander("See explanation"):
            st.write("""""")
    MOLDE_SEV = st.sidebar.button('MOLD RCA MAX SEVERITY')
    if MOLDE_SEV:
        MOLDE_MAX_SEV(df_tracasa)
        with st.expander("See explanation"):
            st.write("""""")
    TRAZABILIDAD = st.sidebar.button(('BLADE REPLACEMENT TRACEABILITY'))
    if TRAZABILIDAD:
        st.title('List of blade replacements')
        today = date.today()
        today_str = str(today)
        filename_t = 'TRAZABILIDAD_PALAS_SUSTITUIDAS_{}'.format((today_str))
        st.markdown(get_table_download_link(replacement, filename_t), unsafe_allow_html=True)
        st.table(replacement)
        with st.expander("See explanation"):
            st.write("""""")
    st.sidebar.markdown(get_table_download_link_raw(df_tracasa, 'FULL_DATA'), unsafe_allow_html=True)


def main_light():
    st.sidebar.title('Technical Issue 22 | Lite ')
    df_tracasa = LECTURA_BD()
    df_tracasa['SRN'] = df_tracasa['SRN'].astype(int)
    df_tracasa = df_tracasa.replace('NO        ', 'NO')
    df_tracasa = df_tracasa.replace('YES       ', 'YES')
    df_tracasa = df_tracasa.replace('IND (INDUTCH)', 'IND')
    df_tracasa = df_tracasa.replace('NBM (CONCAMEX)', 'NBM')
    df_tracasa = df_tracasa.replace('NBS (ACCIONA BLADES)', 'NBS')

    df_cliente, df_mold, df_cliente_wtg = ETL_REGION_CLIENTE()

    df_tracasa = df_tracasa.merge(df_cliente, how='left', on='WindFarm')
    df_raw_bbdd = df_tracasa
    df_raw_bbdd['SRN'] = df_raw_bbdd['SRN'].astype(int)
    df_tracasa = df_tracasa.merge(df_mold, how='left', on=['Blade type', 'Manufacturer', 'SRN'])

    df_tracasa['SRN'] = df_tracasa['SRN'].astype(str)
    df_tracasa['BLADE ID'] = df_tracasa['Blade type'] + '_' + df_tracasa['Manufacturer'] + '_' + df_tracasa['SRN']
    df_tracasa['BLADE ID Z'] = df_tracasa['Blade type'] + '_' + df_tracasa['Manufacturer'] + '_' + df_tracasa[
        'SRN'] + '_' + df_tracasa['Blade radious']
    df_tracasa['inspection_date_str'] = df_tracasa['inspection_date'].astype(str)
    df_tracasa['BLADE ID INP'] = df_tracasa['Blade type'] + '_' + df_tracasa['Manufacturer'] + '_' + df_tracasa[
        'SRN'] + '_' + (df_tracasa['inspection_date_str'])
    today = date.today()

    hace_una_semana = today + timedelta(weeks=-1)
    hace_una_semana = np.datetime64(hace_una_semana)
    df_tracasa_actual_semana = df_tracasa[df_tracasa['upload_date'] < hace_una_semana]

    df_tracasa_actual = ETL_ESTADO_ACTUAL(df_tracasa)
    df_tracasa_actual = CRITICIDAD_PALA(df_tracasa_actual)
    df_tracasa_actual = ARRUGAS(df_tracasa_actual)
    df_tracasa_actual = FECHAS_NEXT_INSP(df_tracasa_actual)
    df_tracasa_actual = NEXT_INSP(df_tracasa_actual)
    df_tracasa_actual = SEGUNDA_CRITICIDAD(df_tracasa_actual)

    df_tracasa_actual_semana = ETL_ESTADO_ACTUAL_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = CRITICIDAD_PALA_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = ARRUGAS_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = FECHAS_NEXT_INSP_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = NEXT_INSP_SEM(df_tracasa_actual_semana)
    df_tracasa_actual_semana = SEGUNDA_CRITICIDAD_SEM(df_tracasa_actual_semana)

    today = date.today()
    today_str = str(today)
    AFECTADAS, CRITICAS, REPARAR, INSPECCIONADAS, insp, REPARADAS, PARO, cero_KPI, uno_KPI, dos_KPI, tres_KPI, cuatro_KPI, cinco_KPI, AFECTADAS_DELTA, CRITICAS_DELTA, REPARAR_DELTA, REPARADAS_DELTA, PARO_DELTA, INSPECCIONADAS_DELTA, insp_DELTA, cero_KPI_DELTA, uno_KPI_DELTA, dos_KPI_DELTA, tres_KPI_DELTA, cuatro_KPI_DELTA, cinco_KPI_DELTA, dif3, dif4, dif5 = KPI(
        df_tracasa_actual, df_tracasa, df_tracasa_actual_semana)

    replacement = TRAZABILIDAD_CAMBIO_PALA(df_tracasa_actual)
    palas_viejas = list(replacement['Old blade'].unique())
    ###   BUTTONS    ###

    notificaciones_pendientes = 0

    if tres_KPI_DELTA != 0:
        notificaciones_pendientes = notificaciones_pendientes + 1
    if cuatro_KPI_DELTA != 0:
        notificaciones_pendientes = notificaciones_pendientes + 1
    if cinco_KPI_DELTA != 0:
        notificaciones_pendientes = notificaciones_pendientes + 1

    ###   BUTTONS    ###
    while notificaciones_pendientes > 0:
        with st.sidebar.expander("Notifications ({})".format(notificaciones_pendientes)):
            if tres_KPI_DELTA > 0:
                st.write('Weekly delta of advanced blades ({})'.format(tres_KPI_DELTA))
                st.write('Debutant blades with advanced damages')
                st.write(dif3)
            if cuatro_KPI_DELTA > 0:
                st.write('Weekly delta of severe bladeS ({})'.format(cuatro_KPI_DELTA))
                st.write('Debutant blades with severe damages')
                st.write(dif4)
            if cinco_KPI_DELTA > 0:
                st.write('Weekly delta of critical blades  ({})'.format(cinco_KPI_DELTA))
                st.write('Debutant blades with critical damages')
                st.write(dif5)

            notificaciones_pendientes = 0
    st.sidebar.title('FILTERS:')
    wf_list = sorted(list(df_tracasa['WindFarm'].unique()))
    wf_list.append('<all>')
    windfarm = st.sidebar.multiselect('WINDFARM', wf_list, default=['<all>'])
    df_tracasa_wf = df_tracasa[df_tracasa['WindFarm'].isin(windfarm)]
    wtg_list = sorted(list(df_tracasa_wf['WTG'].unique()))
    wtg_list.append('<all>')
    wtg = st.sidebar.multiselect('WTG', wtg_list, default=['<all>'])
    df_tracasa_wf_wtg = df_tracasa_wf[df_tracasa_wf['WTG'].isin(wtg)]
    blade_type_list = sorted(list(df_tracasa['Blade type'].unique()))
    blade_type_list.append('<all>')
    blade_type = st.sidebar.multiselect('BLADE TYPE', blade_type_list, default=['<all>'])
    manufacturer_list = sorted(list(df_tracasa['Manufacturer'].unique()))
    manufacturer_list.append('<all>')
    manufacturer = st.sidebar.multiselect('MANUFACTURER', manufacturer_list, default=['<all>'])
    srn_list = sorted(list(df_tracasa['SRN'].unique()))
    srn_list.append('<all>')
    srn = st.sidebar.multiselect('SRN', srn_list, default=['<all>'])
    blade_criticidad = st.sidebar.multiselect('BLADE CRITICITY',
                                              ['0 - No defect', '1 - Initial', '2 - Intermediate', '3 - Advanced',
                                               '4 - Severe', '5 - Critical', '<all>'], default=['<all>'])
    client = sorted(list(df_cliente_wtg['CUSTOMER'].unique()))
    client.append('<all>')
    CLIENTE = st.sidebar.multiselect('CUSTOMER', client, default=['<all>'])

    ###    MAIN   ###

    last = st.sidebar.radio('DDBB Settings', ['Raw', 'Last'])

    windfarm_selected = ''
    for i in windfarm:
        windfarm_selected = windfarm_selected + str(i) + '_'

    wtg_selected = ''
    for i in wtg:
        wtg_selected = wtg_selected + str(i) + '_'

    blade_selected = ''
    for i in blade_type:
        blade_selected = blade_selected + str(i) + '_'

    manufacturer_selected = ''
    for i in manufacturer:
        manufacturer_selected = manufacturer_selected + str(i) + '_'

    srn_selected = ''
    for i in srn:
        srn_selected = srn_selected + str(i) + '_'

    blade_criticidad_selected = ''
    for i in blade_criticidad:
        blade_criticidad_selected = blade_criticidad_selected + str(i) + '_'

    filtros_aplicados = windfarm_selected + '_' + blade_selected + '_' + manufacturer_selected + '_' + today_str
    SEARCH = st.sidebar.button('SEARCH')

    if last == 'Last':
        if SEARCH:
            try:
                df_db = FILTER(windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df_tracasa_actual)
                df_db = df_db[
                    ["WindFarm", "WTG", "Blade position", "Blade type", "Manufacturer", "SRN", "inspection_date",
                     "Blade radious", "Severity", "Blade severity", 'GLOBAL Blade severity', "Red coloured area?",
                     "Excessive adhesive?", "Wrinkle or step?", "H", "L", "AR", "Criterio CS", "Criterio Circular",
                     "Damage description", "Z ini (m)", "Z end (m)", "Defect max spanwise dimension",
                     "Defect max chordwise", "service_provider", "observations", "CUSTOMER", "COUNTRY", "REGION",
                     "Mold", "BLADE ID", "BLADE ID Z", "inspection_folder", 'PROXIMA_INSP']]

                df_db = df_db.replace('0 - No defect', int(0))
                df_db = df_db.replace('1 - Initial', int(1))
                df_db = df_db.replace('2 - Intermediate', int(2))
                df_db = df_db.replace('3 - Advanced', int(3))
                df_db = df_db.replace('4 - Severe', int(4))
                df_db = df_db.replace('5 - Critical', int(5))
                df_db = df_db.drop_duplicates()
                filename = 'QUERY_{}'.format(today_str)
                st.markdown(get_table_download_link(df_db, filename), unsafe_allow_html=True)
                st.dataframe(df_db, height=2000)
                with st.expander("See explanation"):
                    st.write("""""")

            except:
                st.warning('Error...')
    if last == 'Raw':
        if SEARCH:
            a = '<all>'
            df_db = FILTER(windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df_tracasa)
            df_db = df_db.replace('0 - No defect', int(0))
            df_db = df_db.replace('1 - Initial', int(1))
            df_db = df_db.replace('2 - Intermediate', int(2))
            df_db = df_db.replace('3 - Advanced', int(3))
            df_db = df_db.replace('4 - Severe', int(4))
            df_db = df_db.replace('5 - Critical', int(5))
            df_db = df_db.drop_duplicates()
            df_db = df_db.reset_index(drop=True)
            filename = 'QUERY_{}'.format(today_str)
            st.markdown(get_table_download_link(df_db, filename), unsafe_allow_html=True)
            st.dataframe(df_db, height=2000)
            with st.expander("See explanation"):
                st.write("""""")
    df_tracasa = df_tracasa.drop_duplicates()

    KPI_b = st.sidebar.button('KPI')
    if KPI_b:
        st.title('PROJECT KPIs')

        col1, col2, col3, col33 = st.columns(4)
        col1.metric('TOTAL NUMBER OF INSPECTIONS (CUMULATIVE)', '{}'.format(INSPECCIONADAS),
                    '{}'.format(INSPECCIONADAS_DELTA))
        col2.metric('INSPECTED BLADES', '{}'.format(insp), '{}'.format(insp_DELTA))
        col3.metric("AFFECTED BLADES NOW", '{}'.format(AFECTADAS), '{}'.format(AFECTADAS_DELTA), delta_color="inverse")
        col33.metric("REPAIRED BLADES (MIGHT BE RE-AFFECTED)", '{}'.format(REPARADAS), '{}'.format(REPARADAS_DELTA))

        col4, col5, col6, col7, col8 = st.columns(5)
        col4.metric('1 - Initial', '{}'.format(uno_KPI), '{}'.format(uno_KPI_DELTA), delta_color="inverse")
        col5.metric('2 - Intermediate', '{}'.format(dos_KPI), '{}'.format(dos_KPI_DELTA), delta_color="inverse")
        col6.metric('3 - Advanced', '{}'.format(tres_KPI), '{}'.format(tres_KPI_DELTA), delta_color="inverse")
        col7.metric('4 - Severe', '{}'.format(cuatro_KPI), '{}'.format(cuatro_KPI_DELTA), delta_color="inverse")
        col8.metric('5 - Critical', '{}'.format(cinco_KPI), '{}'.format(cinco_KPI_DELTA), delta_color="inverse")

        figpie = go.Figure(data=[go.Pie(
            labels=['0 - No defect', '1 - Initial', '2 - Intermediate', '3 - Advanced', '4 - Severe', '5 - Critical'],
            values=[cero_KPI, uno_KPI, dos_KPI, tres_KPI, cuatro_KPI, cinco_KPI], hole=.4,
            marker_colors=['#2ca02c', 'blue', 'yellow', 'orange', 'red', 'black'], sort=False)])
        figpie.update_layout(
            title='PROJECT STATUS' + '_' + today_str,
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
            )
        )
        figpie.update_layout(width=1200)
        st.plotly_chart(figpie)
        mapa = ISO(df_tracasa_actual)
        mapa.update_layout(width=1200)
        st.plotly_chart(mapa)
        with st.expander("See explanation"):
            st.subheader('Figures show actual data (not historical one).')
            st.write(
                'Ex: “KPI for critical blades” = 0 means that there are currently no reported blades with a critical crack pending repair. Nevertheless, there might have been several blades in the past with critical cracks but already repaired.')

            st.subheader(
                'Figures categorized per severity show blades, not issues; “worst blade damage severity” is shown.')
            st.write(
                'Ex: If one blade has several findings (Z4 severity 0, Z7 severity 2, Z10 severity 3, Z13 severity 3, Z16 severity 1, Z19 severity 0), the blade is categorized with the worst damage. In this case, severity 3.')

            st.subheader('Deltas show absolute changes in the data base for the last seven days.')
            st.write(
                'Ex: “Delta KPI for advanced blades” = ñ 5 means that seven days before there were 5 less blades categorized as “3” in the data base. There might be more than 5 blades becoming Advanced, but the delta shows absolute changes. For instance, it might be possible that 3 blades have changed from category 3 to category 4; and additionally 8 blades with lighter or unexisting damages have progressed to category 3. In total: -3+8 = +5 blades in category 3.')

            st.subheader('Map.')
            st.write(
                'It represents the number of affected blades per country, not site. It just gives a rough idea of where the problem is.')

    REPORT = st.sidebar.button('GENERATE REPORT ')
    if REPORT:
        REPORT_GENERATION(windfarm[0], df_tracasa, df_cliente_wtg, df_tracasa_actual, replacement)
        with st.expander("See explanation"):
            st.write("""""")

    LISTA_CRITICAS = st.sidebar.button('LIST OF BLADES TO REPAIR')
    if LISTA_CRITICAS:
        df_criticas = df_tracasa_actual
        c = ['3 - Advanced', '4 - Severe', '5 - Critical']
        df_criticas = df_criticas[df_criticas['Severity'].isin(c)]
        df_criticas = df_criticas[
            ['WindFarm', 'WTG', 'Blade position', 'Blade type', 'Manufacturer', 'SRN', 'Blade severity',
             'GLOBAL Blade severity']]
        df_criticas = df_criticas.replace('0 - No defect', int(0))
        df_criticas = df_criticas.replace('1 - Initial', int(1))
        df_criticas = df_criticas.replace('2 - Intermediate', int(2))
        df_criticas = df_criticas.replace('3 - Advanced', int(3))
        df_criticas = df_criticas.replace('4 - Severe', int(4))
        df_criticas = df_criticas.replace('5 - Critical', int(5))
        df_criticas = df_criticas.drop_duplicates()
        today = date.today()
        today_str = str(today)
        filename_r = 'PALAS A REPARAR LISTA {}'.format(today_str)
        st.markdown(get_table_download_link(df_criticas, filename_r), unsafe_allow_html=True)
        st.dataframe(df_criticas, 1100, 20000)
        with st.expander("See explanation"):
            st.write("""""")


    GANTT = st.sidebar.button('INSPECTION SCHEDULE')

    if GANTT:
        df_cal = FILTER(windfarm, wtg, blade_type, manufacturer, srn, blade_criticidad, df_tracasa_actual)
        st.write('GANTT CHART')
        CALENDAR_NEW(df_cal)
        with st.expander("See explanation"):
            st.write("""""")

    TRAZABILIDAD = st.sidebar.button(('BLADE REPLACEMENT TRACEABILITY'))
    if TRAZABILIDAD:
        st.title('List of blade replacements')
        today = date.today()
        today_str = str(today)
        filename_t = 'TRAZABILIDAD_PALAS_SUSTITUIDAS_{}'.format((today_str))
        st.markdown(get_table_download_link(replacement, filename_t), unsafe_allow_html=True)
        st.table(replacement)
        with st.expander("See explanation"):
            st.write("""""")
    st.sidebar.markdown(get_table_download_link_raw(df_tracasa, 'FULL_DATA'), unsafe_allow_html=True)


st.set_page_config(
    page_title="TI-22 Web App",
    page_icon='icon.png',
    layout="wide",
    initial_sidebar_state="expanded")



session_state = get(password='')

if session_state.password != 'ti22#@' and session_state.password != 'ti22':
    col1111, mid, col2111 = st.columns([2, 1, 2])
    logo = mid.empty()
    logo.image('logo.jpg',use_column_width=True)
    pwd_placeholder = mid.empty()
    pwd = pwd_placeholder.text_input("Password:", value="", type="password")
    session_state.password = pwd
    if session_state.password == 'ti22#@':
        pwd_placeholder.empty()
        logo.empty()
        main()
    elif session_state.password == 'ti22':
        pwd_placeholder.empty()
        logo.empty()
        main_light()
    elif session_state.password != '':
        st.error("the password you entered is incorrect")
        with st.expander("Olvide mi contraseña"):
            st.write('Please contact Vicente Tanco  :  VTanco@nordex-online.com')
elif session_state.password == 'ti22#@':
    main()
elif session_state.password == 'ti22':
    main_light()

hide_streamlit_style = """ 
            <style> 
            #MainMenu {visibility: hidden;} 
            footer {visibility: hidden;} 
            </style> 
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)


