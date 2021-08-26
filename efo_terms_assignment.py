from urllib.request import urlopen, Request
import json
import datetime as dt
import pandas as pd  
import urllib.request, json 
import psycopg2
from sqlalchemy import create_engine
import logging
import argparse
import sys

logger = logging.getLogger(__name__)


######## Establish arguments for credentials ########

parser = argparse.ArgumentParser()
parser.add_argument("--user", type=str, help="Postgres user name, e.g. postgres", required=True)
parser.add_argument("--post_pass", type=str, help="Postgres password, e.g. postgres", required=True)

args = parser.parse_args()
user_nm = str(args.user)
post_password = str(args.post_pass)

print(user_nm)
print(post_password)


##################### SQL PART #####################
#establishing the connection
conn = psycopg2.connect(
   database="postgres", user=user_nm, password=post_password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Create DB if not exists
#In postgres there is no 'IF NOT EXISTS' statement
#so, we proceed checking if we are getting back response from the selecting
cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'efo_db'")
exists = cursor.fetchone()
if not exists:
    cursor.execute('CREATE DATABASE efo_db')

###################################################


##### Load data using Python JSON module
with urllib.request.urlopen("https://www.ebi.ac.uk/ols/api/ontologies/efo/terms") as url:
    data = json.loads(url.read().decode())
    print(data)

### ~~~~~~~~~~~~~~ Transformation process starts for global dataframe ~~~~~~~~~~~~~~ ###

# getting 'terms'
terms_keys = data["_embedded"]["terms"]
# create a pandas dataframe
df = pd.DataFrame(terms_keys[1:],columns=terms_keys[0])

#Print the work as far
with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    print(df)

#Keep useful columns for the next steps
df = df[['label','annotation','synonyms','_links','iri','ontology_prefix']]

#Flatten annotation column in order to get eventually the id 
df = pd.concat([df.drop(['annotation'], axis=1), df['annotation'].apply(pd.Series)], axis=1)
df = pd.concat([df.drop(['_links'], axis=1), df['_links'].apply(pd.Series)], axis=1)
df = df[['label','id','synonyms','parents','children','iri','ontology_prefix']] #select desired column

#Filter out the NaN lines as id cannot be NaN
#df['id'] = df['id'].astype(str)
df = df.dropna(subset=['id'])
#df['ontology_prefix'] = df['ontology_prefix'].astype(str)

#Asked dataset was "EFO terms"
#so, i tried to filter them since i observed and other types exist in ontology_prefix field
df = df.loc[df['ontology_prefix'].str.contains("EFO")]

#Create the id column for the EFO terms
df['id'] = df['id'].astype(str)
df['efo_id'] = df.id.str.extract('(\d+)').astype(int)

### ~~~~~~~~~~~~~~ Transformation process ends ~~~~~~~~~~~~~~ ###

#engine = create_engine('postgresql://postgres:postgres@localhost:5432/efo_db')
engine = create_engine('postgresql://%s:%s@localhost:5432/efo_db' % (user_nm, post_password))

######## STARTING CREATING FIRST TABLE ########
#Starting creating first table which includes label and id, as id seems to be the unique identifier
df_1 = df[['efo_id','label','iri']] #select desired column
#Add timestamp column
df_1.insert(0, 'TimeStamp', pd.to_datetime('now').replace(microsecond=0))
df_1 = df_1.drop_duplicates()

#Check if table exists
exists1 = pd.read_sql_query("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public'	AND table_name = 'efo_terms';",con=engine)

if exists1.empty:
    df_1.to_sql('efo_terms', engine, index=False)
    logger.info('Table created')
else:
    df_old = pd.read_sql_query('select * from "efo_terms"',con=engine)
    data1=df_old[['efo_id', 'label', 'iri']]
    data2=df_1[['efo_id', 'label', 'iri']]
    merged = data2.merge(data1, how='left', indicator=True)
    left_only = merged[merged['_merge']=='left_only']
    col_id = left_only['efo_id'].tolist()
    boolean_series = df_1.efo_id.isin(col_id)
    filtered_df = df_1[boolean_series]
    if filtered_df.empty:
        logger.info('No new data to append')
    else:
        filtered_df.to_sql('efo_terms', engine, if_exists='append', index=False)
        logger.info('Data to appended')

######## STARTING CREATING SECOND TABLE ########
#Starting creating first table which includes label and id, as id seems to be the unique identifier
df_2 = df[['efo_id','synonyms']]

#Explode by synonyms in order to create a table which has in each row the id and each one of the synonyms
df_2 = df_2.explode('synonyms')
df_2.insert(0, 'TimeStamp', pd.to_datetime('now').replace(microsecond=0))
df_2 = df_2.drop_duplicates()

exists2 = pd.read_sql_query("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public'	AND table_name = 'efo_synonyms';",con=engine)

if exists2.empty:
    df_2.to_sql('efo_synonyms', engine, index=False)
    logger.info('Table created')
else:
    df_old = pd.read_sql_query('select * from "efo_synonyms"',con=engine)
    data1=df_old[['efo_id','synonyms']]
    data2=df_2[['efo_id','synonyms']]
    merged_sun = data2.merge(data1, how='left', indicator=True)
    left_only_sun = merged_sun[merged_sun['_merge']=='left_only']
    col_id_sun = left_only_sun['efo_id'].tolist()
    boolean_series_sun = df_2.efo_id.isin(col_id_sun)
    filtered_df_sun = df_2[boolean_series_sun]
    if filtered_df_sun.empty:
        logger.info('No new data to append')
    else:
        filtered_df_sun.to_sql('efo_synonyms', engine, if_exists='append', index=False)
        logger.info('Data to appended')


#Check the outcome by print all lines
#pd.set_option('display.max_rows', df_2.shape[0]+1)
#print(df_2)

######## STARTING CREATING THIRD TABLE ########
#Select desirable columns
df_3 = df[['efo_id','parents','children']]

#Get the values from the dictionary column handling the NaN
df_3 = df_3.apply(lambda x: x.apply(lambda y: y['href'] if isinstance(y, dict) else y))
df_3.insert(0, 'TimeStamp', pd.to_datetime('now').replace(microsecond=0))
df_3 = df_3.drop_duplicates()

exists3 = pd.read_sql_query("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public'	AND table_name = 'efo_relations';",con=engine)

if exists3.empty:
    df_3.to_sql('efo_relations', engine, index=False)
    logger.info('Table created')
else:
    df_old = pd.read_sql_query('select * from "efo_relations"',con=engine)
    data1=df_old[['efo_id','parents','children']]
    data2=df_3[['efo_id','parents','children']]
    merged_rel = data2.merge(data1, how='left', indicator=True)
    left_only_rel = merged_rel[merged_rel['_merge']=='left_only']
    col_id_rel = left_only_rel['efo_id'].tolist()
    boolean_series_sun = df_3.efo_id.isin(col_id_rel)
    filtered_df_rel = df_3[boolean_series_sun]
    if filtered_df_rel.empty:
        logger.info('No new data to append')
    else:
        filtered_df_rel.to_sql('efo_relations', engine, if_exists='append', index=False)
        logger.info('Data to appended')
        
conn.close()
logger.info('Inserting/Updating all tables finished')

sys.exit("Run terminated")