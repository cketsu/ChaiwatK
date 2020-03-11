import pandas as pd
import numpy as np
import pyodbc
import math
import logging
import os

from azure.storage.blob import BlockBlobService
from dfply import *
from datetime import date
from azure.common.credentials import ServicePrincipalCredentials
from azure.keyvault import KeyVaultAuthentication, KeyVaultClient

def pdbdata-pgt():

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger(__name__)


logger.info('START: Geting credentials')

app_id = 'e744dc6d-3d62-4879-8f43-e2ca01a49720'
tenant_id = '8e9a02ca-d0ea-4763-900c-ac6ee988b360'
app_secret = 'Y38uym/cZuA]S4.W/Cnnzc9P0645dn4M'
credentials = ServicePrincipalCredentials(client_id=app_id, secret=app_secret, tenant=tenant_id)
secret_client = KeyVaultClient(credentials)
vault_base_url = "https://kv-dse-scccth-prod01.vault.azure.net/"

secret_name = "pctbag-db-host"
secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
current_secret_version_id = current_secret_version.id[-32:] 
secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
db_host = secure_secret.value
#print("db_host = " + str(db_host))

secret_name = "pctbag-db-name"
secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
current_secret_version_id = current_secret_version.id[-32:] 
secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
db_name = secure_secret.value
#print("db_name = " + str(db_name))

secret_name = "pctbag-db-username"
secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
current_secret_version_id = current_secret_version.id[-32:] 
secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
db_user = secure_secret.value
#print("db_username = " + str(db_user))

secret_name = "pctbag-db-password"
secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
current_secret_version_id = current_secret_version.id[-32:] 
secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
db_password = secure_secret.value

logger.info('START: Establish database connection')
driver_name = '{ODBC Driver 17 for SQL Server}'
connection_string = 'DRIVER='+ driver_name + ';SERVER=' + db_host + ';PORT=1433;DATABASE=' + db_name + ';UID=' + db_user + ';PWD=' + db_password
connection  = pyodbc.connect(connection_string)
logger.info('START: Query database data - Bag SAP BI')
data = pd.read_sql("SELECT * FROM dbo.pct_bag_sapbi", connection)
logger.info('START: Query database data - pctbag_tlkpMonthToNum')
monthmap = pd.read_sql("SELECT * FROM pctbag_tlkpMonthToNum", connection) 
logger.info('START: Query database data - pctbag_tlkpSegmentMapping')
SKU_map = pd.read_sql("SELECT * FROM pctbag_tlkSKUMapping", connection)
logger.info('Start reading pctbag_tlkpBagProductCost')

trans_as_of_date = data['Calendar Day'].max()
updated_date = date.today()

logger.info('START: Generating Power BI Data')

data = pd.merge(data,
                monthmap,
                how='inner',
                left_on=['Calendar Year', 'Calendar month'],
                right_on=['CalendarYear', 'CalendarMonth'])

CM = data['MonthNo'].max()
LM = CM - 1

df = data \
        >> mask(X['MonthNo'] >= LM) \
        >> select(
                X['Sold-to Area (SCCC)'], \
                X['Sold-to Province'], \
                X['MonthNo'], \
                X['Product Hierarchy Level 4'], \
                X['Volume Sold'], \
                X['Contribution Margin'] \
                 ) \
        >> mutate(\
                 Volume_last_month = (X['MonthNo']==LM) * X['Volume Sold'], \
                 Volume_current_month = (X['MonthNo']==CM) * X['Volume Sold'], \
                 ) \
        >> group_by(\
                  X['Sold-to Area (SCCC)'], \
                  X['Sold-to Province'], \
                  X['Product Hierarchy Level 4'], \
                  ) \
        >> summarize(\
                Contribution_Margin_last_month = (X['Contribution Margin'] * X['Volume_last_month']).sum()/X['Volume_last_month'].sum(),\
                Contribution_Margin_current_month = (X['Contribution Margin'] * X['Volume_current_month']).sum()/ X['Volume_current_month'].sum(),\
                Volume_last_month = X['Volume_last_month'].sum(),\
                Volume_current_month = X['Volume_current_month'].sum(),\
                    )

SKU_map['Product Hierarchy Level 4'] = SKU_map['SKU']
data_final = pd.merge(df, 
                      SKU_map, 
                      how='left', 
                      on='Product Hierarchy Level 4')

data_final = data_final[[
    'Sold-to Area (SCCC)',
    'Sold-to Province',
    'Product Hierarchy Level 4',
    'SKU1',
    'BagWeight',
    'Contribution_Margin_last_month',
    'Contribution_Margin_current_month',
    'Volume_last_month',
    'Volume_current_month'
]]

data_final[['Contribution_Margin_last_month']] = data_final[['Contribution_Margin_last_month']].replace([np.inf, -np.inf], np.nan)
data_final[['Contribution_Margin_current_month']] = data_final[['Contribution_Margin_current_month']].replace([np.inf, -np.inf], np.nan)
data_final[['Volume_last_month']] = data_final[['Volume_last_month']].replace([np.inf, -np.inf], np.nan)
data_final[['Volume_last_month']] = data_final[['Volume_current_month']].replace([np.inf, -np.inf], np.nan)


data_final.rename({'Sold-to Area (SCCC)': 'Area', 'Sold-to Province': 'SoldToProvince', 'Product Hierarchy Level 4':'ProductHierarchyLevel4', 'Contribution_Margin_last_month':'ContributionMarginLM', 'Contribution_Margin_current_month':'ContributionMarginCM', 'Volume_last_month':'VolumeLM', 'Volume_current_month':'VolumeCM'}, axis=1, inplace=True)

data_final['DataAsOfDate'] = trans_as_of_date
data_final['UpdatedDate'] = updated_date

pgt_all = data_final

logger.info('END: Generating Power BI data (Price Gap Tracker) /' + str(len(pgt_all)) + ' records')

logger.info('START: Writing output csv file')

secret_name = "pctbag-pgt-ofilename"
secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
current_secret_version_id = current_secret_version.id[-32:] 
secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
pgt_ofilename = secure_secret.value
pgt_all.to_csv(pgt_ofilename, index=False)

logger.info('START: Moving result to blob storage')

secret_name = "pctbag-st-name"
secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
current_secret_version_id = current_secret_version.id[-32:] 
secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
account_name = secure_secret.value

secret_name = "pctbag-st-accesskey"
secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
current_secret_version_id = current_secret_version.id[-32:] 
secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
account_key = secure_secret.value

secret_name = "pctbag-st-pbiContainers"
secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
current_secret_version_id = current_secret_version.id[-32:] 
secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
container_name = secure_secret.value

block_blob_service = BlockBlobService(account_name, account_key)

with open(pgt_ofilename, 'r') as myfile:
     file_content = myfile.read()

block_blob_service.create_blob_from_text(container_name, pgt_ofilename, file_content)

logger.info('END: Write result to blob storage')

connection.close()

return "Success!"