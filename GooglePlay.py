import pandas as pd
import psycopg2 as psy

#Conectamos con la base de datos
coneccion = psy.connect("host=localhost dbname=GooglePlay user=postgres password=postgres")
cursor = coneccion.cursor()

#Generamos las tablas
cursor.execute('DROP TABLE facts_Application')
coneccion.commit()

cursor.execute('DROP TABLE developer')
coneccion.commit()

cursor.execute('DROP TABLE category')
coneccion.commit()

cursor.execute('DROP TABLE details')
coneccion.commit()

cursor.execute('DROP TABLE contentRating')
coneccion.commit()

cursor.execute("""CREATE TABLE developer(
id serial,
name text,
website text,
email text,
Primary key(id))""")
coneccion.commit()

cursor.execute("""CREATE TABLE category(
id serial,
name text,
Primary key(id))""")
coneccion.commit()

cursor.execute("""CREATE TABLE details(
id serial,
price float,
currency text,
inAppPurchases boolean,
addSupport boolean,
Primary key(id))""")
coneccion.commit()

cursor.execute("""CREATE TABLE contentRating(
id serial,
name text,
Primary key(id))""")
coneccion.commit()

cursor.execute("""CREATE TABLE facts_Application(
id serial,
idDeveloper integer,
idCategory integer,
idContentRating integer,
idDetails integer,
name text,
rating float,
downloads integer,
editorsChoise boolean,
releasedYear integer,
releasedMonth text,
releasedDay integer,
CONSTRAINT fk_idDeveloper FOREIGN KEY(idDeveloper) REFERENCES developer(id),
CONSTRAINT fk_idCategory FOREIGN KEY(idCategory) REFERENCES category(id),
CONSTRAINT fk_idDetails FOREIGN KEY(idDetails) REFERENCES details(id),
CONSTRAINT fk_idContentRating FOREIGN KEY(idContentRating) REFERENCES contentRating(id),
Primary key(id))""")
coneccion.commit()


#string = "Hey! What's up bro?"
#new_string = re.sub(r"[^a-zA-Z0-9]","",string)
#print(new_string)
#dt['App Name'] = dt['App Name'].str.replace('[#,@,&]', '') 

dt = pd.read_csv('Google-Playstore.csv',encoding='utf-8')

# dt[dt.duplicated()] no existen datos duplicados.
dt = dt.dropna() #Elimina datos nulos

#Limpieza de datos:
#-Eliminar todos las apps con rating count 0 o con menos de 1000 descargas
filtro1 = dt['Rating Count'] > 0
dt = dt[filtro1]

dt['Installs'] = dt['Installs'].str.replace('+','')
dt['Installs'] = dt['Installs'].str.replace(',','')
dt['Installs'] = pd.to_numeric(dt['Installs'])
filtro2 = dt['Installs'] > 1000
dt = dt[filtro2]

#-Eliminar aplicaciones que no esten en USD
filtro3 = dt['Currency'] == "USD"
dt = dt[filtro3]

#-Separamos 'release' en [dia, mes,año], limpiamos los datos y eliminanamos los registros de antes del año 2017
releaseDates = dt['Released'].str.split(expand=True)
releaseDates.columns = ['releaseMonth', 'releaseDay', 'releaseYear']
dt = pd.concat([dt, releaseDates], axis=1)
dt['releaseDay'] = dt['releaseDay'].str.replace(',','')
dt['releaseDay'] = pd.to_numeric(dt['releaseDay'])
dt['releaseYear'] = pd.to_numeric(dt['releaseYear'])
filtro4 = dt['releaseYear'] > 2017
dt = dt[filtro4]

#--
print("")
for i in range(50):
    print(dt.iloc[i]['App Name'], dt.iloc[i]['Currency'], dt.iloc[i]['releaseYear'])



