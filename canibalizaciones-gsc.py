
import searchconsole
import pandas as pd
import sys
import requests
from bs4 import BeautifulSoup
import os
import re

def getNombreFichero(url_propiedad, ini, fin, conTitle):
    if url_propiedad.startswith('sc-domain:'):
        dominio=url_propiedad.replace('sc-domain:','')
    else:
        m = re.search('https?://([A-Za-z_0-9.-]+).*',url_propiedad)
        dominio=m.group(1)
    formato_salida='%Y%m%d-%H%M%S'
    if conTitle:
      nombre_fichero="ficheros_salida/canibalizaciones_"+dominio+"_"+ini+"_"+fin+".csv"
    else:
      nombre_fichero="ficheros_salida/canibalizaciones_sin_title_"+dominio+"_"+ini+"_"+fin+".csv"
    return nombre_fichero

def get_title(url):
    data_headers ={"User-Agent":"Mozilla/5.0"}
    page = requests.get(url,headers=data_headers)
    soup = BeautifulSoup(page.content,'html.parser')
    try:
      title = soup.find('title').get_text()
    except IndexError:
      title=''
    except Exception as e:
      title=''
    return title         

def get_meta(url):
    data_headers ={"User-Agent":"Mozilla/5.0"}
    page = requests.get(url,headers=data_headers)
    soup = BeautifulSoup(page.content,'html.parser')
    try:
      title = soup.find('title').get_text()
    except IndexError:
      title='' 
    except Exception as e:
      title=''        
    try:
      meta = soup.select('meta[name="description"]')[0].attrs["content"] 
    except IndexError:
      meta=''            
    except Exception as e:
      meta=''                  
    return title, meta
 
filtro=None
dimensiones=["page","query"]
if len(sys.argv) >= 5:
    cuenta=sys.argv[1]
    propiedad=sys.argv[2]
    inicio=sys.argv[3]
    fin=sys.argv[4]
    branded_queries=sys.argv[5]
    if len(sys.argv)==7:
        fil=sys.argv[6]
        aux=fil.split(",")
        filtro={'parametro':aux[0], 'operador':aux[1], 'valor':aux[2]}

if os.path.isfile('../credentials/credentials_'+cuenta+'.json'):
    account=searchconsole.authenticate(client_config='../credentials/client_secrets_'+cuenta+'.json', credentials='../credentials/credentials_'+cuenta+'.json')
else:
    account = searchconsole.authenticate(client_config='../credentials/client_secrets_'+cuenta+'.json',
                                     serialize='../credentials/credentials_'+cuenta+'.json')


# Connect to the GSC property
webproperty= account[propiedad]

# Set your dates and dimensions

exampleGSC =webproperty.query.range(inicio, fin).dimension(dimensiones[0], dimensiones[1])

if filtro is not None:
    exampleGSC=exampleGSC.filter(filtro.get('parametro'),filtro.get('valor'),filtro.get('operador')).get()
else:
    exampleGSC=exampleGSC.get()

# Make it a Data Frame
df = pd.DataFrame(data=exampleGSC) 
df['clicks'] = df['clicks'].astype('int')
df['ctr'] = df['ctr']*100
df['impressions'] = df['impressions'].astype('int')
df['position'] = df['position'].round(2)
df.sort_values('clicks',inplace=True,ascending=False)

SERP_results = 1 #insert here your prefered value for SERP results
 
df_canibalized = df[df['position'] > SERP_results] 
df_canibalized = df_canibalized[~df_canibalized['query'].str.contains(branded_queries, regex=True)]
#df_canibalized =df[~df['query'].str.contains(branded_queries, regex=True)]
df_canibalized = df_canibalized[df_canibalized.duplicated(subset=['query'], keep=False)]
df_canibalized.set_index(['query'],inplace=True)
df_canibalized.sort_index(inplace=True)
df_canibalized.reset_index(inplace=True)
df_canibalized.to_csv(getNombreFichero(propiedad, inicio, fin,False), index=False)

#Seleccionamos URL únicas para obtener el title
columns = ['page']
df1 = pd.DataFrame(df_canibalized, columns=columns)
df1.drop_duplicates(subset=['page'],inplace=True)
df1['title'] = df1['page'].apply(get_title)


#obtenemos las canibalizaciones añadiendo el title
inner_join = pd.merge(df_canibalized, df1, on ='page', how ='inner')
inner_join.set_index(['query'],inplace=True)
inner_join.sort_index(inplace=True)
inner_join.reset_index(inplace=True)

# Export to *.csv
inner_join.to_csv(getNombreFichero(propiedad, inicio, fin,True), index=False)

#df_canibalized.to_csv('canibalizaciones.csv', index=False)
