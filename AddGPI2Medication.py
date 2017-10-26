import pandas as pd
import numpy as np
import os
from time import ctime
import pyodbc
import sys
from time import sleep
sys.path.append('c:/code/general')
from NCHGeneral import Use, Save, fewSpreadsheets, postAgg, getFN, RenameVars, toMySQL
from NCHGeneral import *

'''
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=BIDATACA2;DATABASE=EDW;UID=ebassin;\
                       PWD=ebassin;Trusted_Connection=yes')
myQuery = 'SELECT GenericName, BillingCode FROM EDW.DIM.Medication as M'
dfm = pd.read_sql(myQuery, conn)
print('Number of medications before dropping duplicates: {:,}'.format(len(dfm.index)))
dfm.drop_duplicates(inplace=True)
print('Number of medications after  dropping duplicates: {:,}'.format(len(dfm.index)))
Save(dfm, 'c:/temp/MedicationTable')
dfmo = dfm[dfm.BillingCode.isin(['J8999', 'J9999'])]
# Logic for drugs with real J codes
dfmj = dfm[~dfm.BillingCode.isin(['J8999', 'J9999'])]
print('Rows in dfmj at beginning: {:,}'.format(len(dfmj.index)))
xw = Use('/AdvAnalytics/Reference/xw_HCPCS2NDC')
xw.rename(columns={'HCPCS': 'BillingCode'}, inplace=True)
xw['NDC9'] = xw.NDC.apply(lambda x: x[:9])
del xw['NDC']
dfmj = pd.merge(dfmj, xw, on='BillingCode', how='left')
dfmj.sort_values('GenericName', inplace=True)
dfmj.drop_duplicates(subset=['NDC9', 'BillingCode'], inplace=True)
dfmj.NDC9.fillna('N/A', inplace=True)
xw = Use('/AdvAnalytics/Reference/ref_NDC')
xw['NDC9'] = xw.NDC.apply(lambda x: x[:9])
xw.GPI.fillna('MISSING', inplace=True)
xw['GPI10']=xw.GPI.apply( lambda x: x[:10])
xw = xw[['NDC9', 'GPI10']]
dfmj = pd.merge(dfmj, xw, on='NDC9', how='left')
dfmj.GPI10.fillna('N/A', inplace=True)
dfmj.sort_values(['GenericName', 'GPI10'], inplace=True)
dfmj = dfmj.groupby('GenericName').first().reset_index()
print('Rows in dfmj at end: {:,}'.format(len(dfmj.index)))
print(dfmj[dfmj.GPI10=='N/A'])
myQuery = 'SELECT * FROM EDW.MSTR.DrugClassification'
dfg = pd.read_sql(myQuery, conn)
dfg['GPI10'] = dfg.GenericProductIdentifier_Code.apply(lambda x: x[:10])
dfg = dfg[['GPI10', 'Drug_Base_Name', 'Drug_Name_Extension']]
dfg.drop_duplicates(inplace=True)
dfg['Drug_Base_Name'] = dfg.Drug_Base_Name.apply(lambda x: x.upper())
dfg['Drug_Name_Extension'] = dfg.Drug_Name_Extension.apply(lambda x: x.upper())
dfg.drop_duplicates(inplace=True)
# Save(dfg, 'c:/temp/GPI10')
dfg.rename(columns={'Drug_Name_Extension': 'GenericName'}, inplace=True)
df = pd.merge(dfm, dfg, on='GenericName', how='left')
df.GPI10.fillna('Missing', inplace=True)
print(df[df.GPI10=='Missing'])
'''

f = open('c:/temp/Med2GPI.txt', 'w')
f.write('Carepro Drug Name' + '\t' + '10 Character GPI' + '\t' + 'Method Used To Match' + '\t' +
        'GPI Drug Name with Extension' + '\t' + 'GPI Base Drug Name' + '\t' + 'HCPCS' + '\n')
# Get data from the Carepro Medication table
'''
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=BIDATACA2;DATABASE=EDW;UID=ebassin;\
                       PWD=ebassin;Trusted_Connection=yes')
myQuery = 'SELECT GenericName, BillingCode FROM EDW.DIM.Medication as M'
dfm = pd.read_sql(myQuery, conn)
print('Number of medications before dropping duplicates: {:,}'.format(len(dfm.index)))
dfm.drop_duplicates(inplace=True)
print('Number of medications after  dropping duplicates: {:,}'.format(len(dfm.index)))
Save(dfm, 'c:/temp/MedicationTable')
# Build the J Code to GPI crosswalk
myQuery = 'SELECT ServiceCode, NdcNumber FROM EDW.MSTR.AvgSalesPrice_NDC'
xw = pd.read_sql(myQuery, conn)
xw.columns = ['BillingCode', 'NDC']
xw['NDC'] = xw.NDC.apply(lambda x: x.replace('-', ''))
xw['NDC9'] = xw.NDC.apply(lambda x: x[:9])
Save(xw, 'c:/temp/JCode2NDC')
'''
xw = Use('c:/temp/JCode2NDC')
xw2 = Use('/AdvAnalytics/Reference/ref_NDC')
xw2['NDC9'] = xw2.NDC.apply(lambda x: x[:9])
xw2 = xw2[pd.notnull(xw2.GPI)]
xw2['GPI10']=xw2.GPI.apply( lambda x: x[:10])
xw2 = xw2[['NDC9', 'GPI10']]
xw = pd.merge(xw, xw2, on='NDC9')
xw = xw[['BillingCode', 'GPI10']]
xw.drop_duplicates(inplace=True)
xw = xw.groupby('BillingCode').first().reset_index()

# Open the Carepro and GPI tables.  These are saved by queries above.
dfm = Use('c:/temp/MedicationTable')
dfm = dfm[dfm.GenericName!='N/A']
dfg = Use('c:/temp/GPI10')
for c in ['Drug_Base_Name', 'Drug_Name_Extension']:
    dfg[c] = dfg[c].apply(lambda x: str(x))
dfg = dfg[pd.notnull(dfg.GPI10)]
# Extract a table of the Carepro medications where the HCPCS code might be used for matching.
dfmj = dfm[~dfm.BillingCode.isin(['J8999', 'J9999'])]
dfg['LenExt'] = dfg.Drug_Name_Extension.apply(lambda x: len(x))
dfg = dfg[dfg.LenExt>1]
listDrugs = dfm.GenericName.tolist()
print(listDrugs)
for drug in listDrugs:
    if drug=='NA FERRIC GLUCONATE':
        drug='SODIUM FERRIC GLUCONATE'
    detectionType='Full Drug Name'
    HCPCS = ''
    print(drug)
    dfw = dfg[dfg.Drug_Name_Extension.apply(lambda x: x in drug)]
    print('  Number of rows in the Drug Name Extension df for ' + drug + ': {:,}'.format(len(dfw.index)))
    # HCPCS code method
    if(len(dfw.index))==0:
        print('Using the HCPCS method for ' + drug)
        dft = dfmj[dfmj.GenericName==drug]
        print('  Number of rows in the Medication table matching the drug: {:,}'.format(len(dft.index)))
        dft = pd.merge(dft, xw, on='BillingCode')
        print('  Number of matches to the crosswalk table: {:,}'.format(len(dft.index)))
        dft = dft[pd.notnull(dft.GPI10)]
        print('  Number of non-null GPI codes: {:,}'.format(len(dft.index)))
        if len(dft.index>0):
            dft['Rows'] = 1
            dfw = dft.groupby('GPI10').count()
            dfw.sort_values('Rows', ascending=False, inplace=True)
            dfw = dfw.groupby('GPI10').first().reset_index()
            dfw = pd.merge(dfw, dfg, on='GPI10', how='left')
            detectionType = 'J Code'
            dfh = dft[dft.GPI10==dfw.GPI10[0]]
            HCPCS = dfh.BillingCode[0]
            # print(dfw.head())
    if(len(dfw.index))==0:
        detectionType='Drug Base Name'
        dfw = dfg[dfg.Drug_Base_Name.apply(lambda x: x in drug)]
    if len(dfw.index)==0:
        detectionType='No Match Found'
        drugName = 'No Match Found'
        shortName = 'No Match Found'
        gpi = 'N/A'
        f.write(drug + '\t' + gpi + '\t' + str(detectionType) + '\t' + drugName + '\t' + shortName + '\t' + HCPCS + '\n')
    else:
        dfw.sort_values('LenExt', ascending=False, inplace=True)
        dfw.reset_index(inplace=True)
        rows = len(dfw.index)
        for r in range(rows):
            drugName = dfw.Drug_Name_Extension[r]
            shortName = dfw.Drug_Base_Name[r]
            gpi = dfw.GPI10[r]
            f.write(drug + '\t' + gpi + '\t' + str(detectionType) + '\t' + drugName + '\t' + shortName + '\t' + HCPCS + '\n')
f.close()

# Write the results to an Excel file for review.
sleep(.2)
df = pd.read_csv('c:/temp/Med2GPI.txt', delimiter='\t')
WSA = []
ColumnNames = {}
ColumnGroups = []
Excel = xlsxwriter.Workbook('c:/temp/CarePro2GPI.xlsx', {'nan_inf_to_errors': True})
fewSpreadsheets(DF=df,
                workbook=Excel,
                sheet='Carepro GPI Match',
                title='Results of Attempt to Assign GPI codes to Carepro Medication Table',
                notes=['This method uses the first 10 characters of the GPI code which provides the drug name and extension.',
                       'The logic first tries to find GPI codes where the drug name and extension are found in the Carepro',
                       '     drug name in their entirety.  If it finds a match, the program uses that GPI10 code.  If there',
                       '     is no match, the program looks to find a GPI base drug name that is found in the Carepro name',
                       '     in its entirety.  If so, it uses one of the GPI10 codes that match.',
                       'If 2 drugs from the GPI table match completely, the program assigns the GPI10 code of the drug with',
                       '     the longer drug name + extension.'],
                WSA=WSA,
                ColumnNames=ColumnNames,
                ColumnGroups=ColumnGroups,
                DataSource='Carepro drug table (EDW.DIM.Medication) and Medispan GPI table (EDW.MSTR.DrugClassification)',
                images=[],
                minColWidth=9,
                headerLines=3)
Excel.close()
