#!/usr/bin/env python
# coding: utf-8

# #### 1. IMPORTAÇÃO DAS BIBLIOTECAS

# In[ ]:


# leitura das lib
import pyodbc
import pandas as pd
import math
from pandas.tseries.offsets import MonthEnd
import numpy as np
import future_fstrings
import plotly.express as px
import plotly.offline as pyo


# #### 2. GET - CONEXÃO COM O BANCO DE DADOS

# In[ ]:


# cria a conexao com o banco de dados
conn = pyodbc.connect(
    server="192.168.168.28\IMGDATABASE",
    database="Int_IMGSys",
    user='digital.unico1',
    password="!Digit@al20211",
    driver='{SQL Server}'
)


# In[ ]:


# puxa os dados de todas as vendas feitas no ecommerce junto com a identificação do cliente
# Lembrar sempre de alterar a data limite na Query de Franquias e de Ecommerce 

query = """
SELECT [CODIGO_FILIAL]
      ,[TICKET] AS PEDIDO
      ,CANAL = 'LOJA'
      ,OMNI = ''
      ,[DATA_VENDA] AS EMISSAO
      ,[CODIGO_CLIENTE] AS CPF
      ,[VALOR_PAGO] AS VALOR
FROM [ERP_Franquias].[dbo].[LOJA_VENDA] (NOLOCK)
WHERE DATA_VENDA >= '2018-06-01'
UNION ALL
SELECT CODIGO_FILIAL = '999999',
       CAST(T1.PEDIDO AS varchar) AS PEDIDO,
       CANAL = 'ECM',
       T1.OMNI,
       T1.EMISSAO,
       T2.CPF,
       T1.VALOR_TOTAL AS VALOR
FROM [Int_IMGSys].[dbo].[IMG_ECM_LOJA_PEDIDO] AS T1
INNER JOIN [Int_IMGSys].[dbo].[IMG_ECM_CLIENTES_VAREJO] AS T2
    ON T1.PEDIDO = T2.PEDIDO
WHERE T1.EMISSAO >= '2018-06-01' AND T1.ORIGEM_PEDIDO = 'IMAGINARIUM'
"""

pedidos = pd.read_sql(query, conn)


# In[ ]:


conn.close


# #### 3. MERGE - CONEXÃO COM O BANCO DE DADOS

# In[ ]:


# cria a conexao com o banco de dados
conn = pyodbc.connect(
    server="192.168.168.28\IMGDATABASE",
    database="Int_IMGSys",
    user='digital.unico1',
    password="!Digit@al20211",
    driver='{SQL Server}'
)


# In[ ]:


cursor = conn.cursor()


# In[ ]:


cols_list = base_cli.columns.tolist()
cols_list_query = f'({(", ".join(cols_list))})'
sr_cols_list = [f'Source.{i}' for i in cols_list]
sr_cols_list_query = f'({(", ".join(sr_cols_list))})'
up_cols_list = [f'{i}=Source.{i}' for i in cols_list]
up_cols_list_query = f'{", ".join(up_cols_list)}'


# In[ ]:


def fill_null(vals: list) -> list:
    def bad(val):
        if isinstance(val, type(pd.NA)):
            return True
        return val in ['NULL', np.nan, 'nan', '', '', '-', '?']
    return tuple(i if not bad(i) else None for i in vals)


# In[ ]:


params = [fill_null(row.tolist()) for _, row in base_cli.iterrows()]
param_slots = '('+', '.join(['?']*len(base_cli.columns))+')'


# In[ ]:


cmd =  f'''
       MERGE INTO [Int_IMGSys].[dbo].[DASHCOMITE_BASECLIENTES_MARCAS] as Target 
       USING (SELECT * FROM 
       (VALUES {param_slots}) 
       AS s {cols_list_query} 
       ) AS Source 
       ON Target.MES_REF=Source.MES_REF
       AND Target.CANAL=Source.CANAL
       AND Target.MARCA=Source.MARCA
       AND Target.TIPO_CLIENTE=Source.TIPO_CLIENTE
       WHEN NOT MATCHED THEN 
       INSERT {cols_list_query} VALUES {sr_cols_list_query} 
       WHEN MATCHED THEN 
       UPDATE SET {up_cols_list_query};
       '''


# In[ ]:


for index, row in base_cli.iterrows():
     cursor.execute(cmd, row.MES_REF, row.CANAL, row.MARCA, row.TIPO_CLIENTE, row.QTDE_CPF, row.QTDE_PEDIDO, row.RECEITA)


# In[ ]:


conn.commit()


# In[ ]:


conn.close

