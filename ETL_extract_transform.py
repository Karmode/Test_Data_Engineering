# %%
import pandas as pd
import numpy as np

# %% [markdown]
# # PUNTO 1
# ## Construir una tabla uniendo las dos fuentes de datos que permita conocer la información de cada país incluido en el tratado.

# %% [markdown]
# ## Cargar Dataset 1

# %%
url = "https://raw.githubusercontent.com/sebas317/testDataEnginner/main/Tratados_internacionales_de_Colombia.csv"
data1 = pd.read_csv(url)

# %% [markdown]
# ### Convertir (NO REGISTRA) en nulos

# %%
data1 = data1.replace({'(NO REGISTRA)': np.nan})

# %% [markdown]
# ### Convertir 'Bilateral' y 'Vigente' en Bool

# %%
data1 = data1.replace({'Bilateral': {'SI': True, 'NO' : False}})
data1 = data1.replace({'Vigente': {'SI': True, 'NO' : False}})

# %% [markdown]
# ## Cargar Dataset 2

# %%
import requests

# %%
url2 = "https://restcountries.com/v2/all"

resp = requests.get(url2)
resp = resp.json()

# %% [markdown]
# for item in resp:
#     print(item['name'])

# %%
data2 = pd.DataFrame(resp)

# %%
data2 = data2[['name', 'alpha2Code', 'callingCodes', 'capital', 'region', 'subregion', 'population', 'area', 'timezones', 'currencies', 'languages', 'borders']]

# %% [markdown]
# ## Agregar código ISO de paises

# %%
import difflib

# %%
paises = pd.read_csv("https://gist.githubusercontent.com/brenes/1095110/raw/4422fd7ba3a388f31a9a017757e21e5df23c5916/paises.csv")

# %%
### Convertir en minusculas
data1['Estados-Organismos'] = data1['Estados-Organismos'].str.lower()

# %%
def p_match(x):
    try:
        return difflib.get_close_matches(x, paises['nombre'].astype(str))[0]
    except:
        return x

# %%
data1['Estados-Organismos'] = data1['Estados-Organismos'].apply(p_match)

# %%
data1.insert(16, 'ISO', data1['Estados-Organismos'].map(paises.set_index('nombre')['iso']))

# %% [markdown]
# ## Union de tablas

# %%
data2 = data2.rename(columns={'alpha2Code':'ISO'})

# %%
data_full = pd.merge(data1, data2, on='ISO')

# %% [markdown]
# ### Monedas como texto separado por comas

# %%
data_full['currencies'].to_dict()[0][0]['code']

# %%
def code_money(x):
    #print(x)
    list_aux = []
    for a in x:
        list_aux.append(a['code'])
    return list_aux

data_full['currencies'] = data_full['currencies'].apply(code_money)

# %% [markdown]
# ### Idiomas como texto separado por comas

# %%
def languages_name(x):
    #print(x)
    list_aux = []
    for a in x:
        list_aux.append(a['name'])
    return list_aux

data_full['languages'] = data_full['languages'].apply(languages_name)

# %% [markdown]
# ### Cantidad de fronteras

# %%
def longitud(valor):
    # Validar si es lista o cadena
    if type(valor) is not list and type(valor) is not str:
        return -1
    contador = 0
    for elemento in valor:
        contador += 1
    return contador
data_full['Cantidad fronteras'] = data_full['borders'].apply(longitud)

# %% [markdown]
# ### Diferencia horaria

# %%
def dif_zonetime(x):
    h = x[0][3:6]
    try:
        h = int(h)
    except:
        h = 0
    return np.diff([-5,h])[0] # Colombia tiene un UTC-05:00

# %%
data_full['Diferencia zona horaria'] = data_full['timezones'].apply(dif_zonetime)

# %% [markdown]
# ## Seleccionar columnas que compondran la tabla resultante

# %%
data_transform = data_full[['Nombre del Tratado', 
                            'Bilateral', 
                            'Lugar de Adopcion',
                            'Fecha de Adopcion', 
                            'Estados-Organismos', 
                            'Temas',
                            'Naturaleza del Tratado', 
                            'Depositario', 
                            'Suscribio Por Colombia',
                            'Vigente', 
                            'Fecha Ley Aprobatoria', 
                            'Numero Ley Aprobatoria',
                            'Sentencia Fecha Ley', 
                            'Sentencia Numero',
                            'Decreto Fecha Diario Oficial', 
                            'Decreto Numero Diario Oficial', 
                            'name', 
                            'callingCodes', 
                            'capital', 
                            'region', 
                            'subregion', 
                            'population',
                            'area', 
                            'timezones', 
                            'currencies', 
                            'languages',
                            'Cantidad fronteras', 
                            'Diferencia zona horaria']]

data_transform.head(2)

# %% [markdown]
# ### Ajustar formato de area

# %%
data_transform['area'] = data_transform['area'].astype(int)

# %%
#cambiar el formato de las fechas
from datetime import datetime
def convert_date(x):
    if type(x) == str:
        try:
            return datetime.strptime(str(x), '%d/%m/%Y')
        except:
            return np.nan
    else:
        return x
col_fechas = ['Fecha de Adopcion', 'Fecha Ley Aprobatoria', 'Sentencia Fecha Ley', 'Decreto Fecha Diario Oficial']
for n in col_fechas:
    data_transform[n] = data_transform[n].apply(convert_date)

# %% [markdown]
# ## Exportar archivo parquet

# %%
data_transform.to_parquet('data_transform.parquet')

