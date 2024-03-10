#!/usr/bin/env python
# coding: utf-8

#!/usr/bin/env python
# coding: utf-8

# Importação das bibliotecas necessárias
import pyodbc
import pandas as pd

# Conexão com o banco de dados utilizando ODBC
# As credenciais e detalhes do servidor devem ser preenchidos abaixo
conn = pyodbc.connect(
    server="",
    database="",
    user='',
    password="",
    driver='{SQL Server}'
)

# Insira a query SQL desejada
query = """
"""
# Executa a query SQL e armazena o resultado em um DataFrame
df = pd.read_sql(query, conn)
conn.close

# Segunda conexão com o banco de dados utilizando ODBC
# As credenciais e detalhes do servidor devem ser preenchidos abaixo
conn = pyodbc.connect(
    server="",
    database="",
    user='',
    password="",
    driver='{SQL Server}'
)

cursor = conn.cursor()

# Listas de colunas do DataFrame
cols_list = df.columns.tolist()
# Cria strings com os nomes de colunas para serem usados na query SQL
cols_list_query = f'({(", ".join(cols_list))})'
sr_cols_list = [f'Source.{i}' for i in cols_list]
sr_cols_list_query = f'({(", ".join(sr_cols_list))})'
up_cols_list = [f'{i}=Source.{i}' for i in cols_list]
up_cols_list_query = f'{", ".join(up_cols_list)}'

# Função para preencher valores nulos ou 'NULL' com None
def fill_null(vals: list) -> list:
    def bad(val):
        if isinstance(val, type(pd.NA)):
            return True
        return val in ['NULL', np.nan, 'nan', '', '', '-', '?']
    return tuple(i if not bad(i) else None for i in vals)

# Lista de parâmetros para a query SQL
params = [fill_null(row.tolist()) for _, row in df.iterrows()]
param_slots = '('+', '.join(['?']*len(df.columns))+')'

# Comando SQL para realizar o merge dos dados
cmd =  f'''
       MERGE INTO [database].[schema].[table] as Target 
       USING (SELECT * FROM 
       (VALUES {param_slots}) 
       AS s {cols_list_query} 
       ) AS Source 
       ON Target.{column}=Source.{column}
       WHEN NOT MATCHED THEN 
       INSERT {cols_list_query} VALUES {sr_cols_list_query} 
       WHEN MATCHED THEN 
       UPDATE SET {up_cols_list_query};
       '''

# Execução do comando SQL para cada linha do DataFrame
for index, row in base_cli.iterrows():
     cursor.execute(cmd, row.{column}, row.{column}, row.{column})

# Commit das alterações no banco de dados e fechamento da conexão
conn.commit()
conn.close
