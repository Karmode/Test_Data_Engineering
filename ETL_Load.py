# -*- coding: utf-8 -*-
# PUNTO 2
## Crear una tabla en base de datos postgres que almacene la información automáticamente desde el script ETL de las transformaciones.
import psycopg2
import os
import pandas as pd
# %%
data_transform = pd.read_parquet('data_transform.parquet', engine='fastparquet')
conection = psycopg2.connect(database="DataTest", user='postgres', password='561214')
cur = conection.cursor()

# %%
def copy_from_file(con, df, table):
    """
    Se guarda el dataframe en memoria para luego cargarlo
    a la tabla de la base de datos postgeSQL con el metodo:
    copy_from()
    """
    tmp_df = "C:/Program Files/PostgreSQL/13/data/tmp_dataframe.csv"
    df.to_csv(tmp_df, index_label='id', header=False)
    f = open(tmp_df, 'r')
    cursor = con.cursor()
    try:
        cursor.copy_from(f, table, sep=",")
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        #os.remove(tmp_df)
        print("Error: %s" % error)
        con.rollback()
        cursor.close()
        return 1
    print("copy_from_file() done")
    cursor.close()
    os.remove(tmp_df)

# %%
copy_from_file(conection, data_transform, 'tratadosintern')
conection.close()