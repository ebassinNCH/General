InputDir = 'c:/AdvAnalytics/Reference/Rawdata/'
OutputDir = 'c:/AdvAnalytics/Reference/'
# # Build Reference Database
# This file contains code used to build and maintain the reference database.

import sqlalchemy as sql
import pandas as pd
import numpy as np
import os
os.chdir(OutputDir)
import sys
sys.path.append('c:/Gode/General')
from NCHGeneral import *


# DRG Data files.
# Source: https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/AcuteInpatientPPS/FY2017-IPPS-Final-Rule-Home-Page-Items/FY2017-IPPS-Final-Rule-Tables.html
#      Use Table 5 as input.  The file that you use will need to be renamed to DRGCodes.xlsx.
def readDRG():
    df = pd.read_excel(InputDir + 'DRGCodes.xlsx', skiprows=1)
    df.columns = ['DRG', 'PostAcuteDRG', 'SpecialPayDRG', 'MDC', 'DRGType', 'DRG_lbl', 'DRGWeights', 'GeometicMeanLOS',
                  'ArithmeticMeanLOS']
    df['DRG'] = df['DRG'].apply(lambda x: str(x))
    df['DRG'] = df['DRG'].apply(lambda x: x.zfill(3))
    # Create Base DRG
    df['ShortLabel'] = df.DRG_lbl[:-12]
    print(df.columns.tolist())
    dfG = df.groupby(['MDC', 'DRGType', 'ShortLabel'])
    dfA = dfG.agg({'DRG': {'BaseDRG' : 'min'}})
    dfA = postAgg(dfA)
    df = pd.merge(df, dfA, on=['MDC', 'DRGType', 'ShortLabel'], how='left')
    del df['ShortLabel']
    # Improve the formatting in the DRG and BaseDRG columns.
    df['BaseDRG'].fillna(-1, inplace=True)
    df['BaseDRG'] = df['BaseDRG'].apply(lambda x : int(x))
    df['BaseDRG'] = df['BaseDRG'].apply(lambda x : str(x))
    df['BaseDRG'] = df['BaseDRG'].apply(lambda x : x.zfill(3))
    df['DRG_lbl'] = df.DRG_lbl.apply(lambda x: str(x))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.title())
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' Or ', ' or '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' Of ', ' of '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Ecmo', 'ECMO'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' Mv ', ' MV '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Mcc', 'MCC'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Cc', 'CC'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' W ', ' w/ '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' Wo ', ' w/o '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' W/O ', ' w/o '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' Pdx ', ' PDx '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Diagnoses', 'Dx'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Diagnosis', 'Dx'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Procedures', 'Px'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Procedure',  'Px'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' Proc ',  ' Px '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Multiple Sclerosis', 'MS'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Acute Ischemic Stroke', 'Stroke'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' Tpa', ' TPA'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Infections', 'Infect.'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Malignancy', 'Malig.'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace(' Uri ', ' URI '))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('O.R.', 'OR'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Chronic Obstructive Pulmonary Disease', 'COPD'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Ami/', 'AMI/'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Hf/', 'HF/'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Ptca', 'PTCA'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Coronary Bypass', 'CABG'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Ptca', 'PTCA'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Aicd', 'AICD'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Acute Myocardial Infarction', 'AMI'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Except', 'exc.'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Circulatory', 'Circ.'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Vascular', 'Vasc.'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Cardiovascular', 'Cardiovasc.'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Complicated', 'Comp.'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Principal', 'Princ.'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Gastrointestinal', 'GI'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('G.I.', 'GI'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Inflammatory Bowel Disease', 'IBD'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('C.D.E.', 'CDE'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x :
                        x.replace('Major Joint Replacement or Reattachment of Lower Extremity',
                                  'Major Lower Joint'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('System', 'Sys'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Musculoskeletal', 'Musculoskel'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Fractures', 'Fx'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Connective', 'Conn'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('The', 'the'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Inflammation', 'Inflam'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Reproductive', 'Reprod'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Malignancy', 'Malig'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Major', 'Maj'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Minor', 'Min'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Sdx', 'SDx'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Secondary', 'Sec'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Diseases', 'Dis'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Disease', 'Dis'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Rehabilitation', 'Rehab'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Dependence', 'Depend'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Hiv', 'HIV'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Left Ama', 'Left AMA'))
    df['DRG_lbl'] = df['DRG_lbl'].apply(lambda x : x.replace('Principal', 'Princ'))
    # Save the output of the reference file.  The reference file includes more than just the name.
    Save(df, 'c:/AdvAnalytics/Reference/ref_DRG')
    # Create a file that just has codes and associated names
    df2 = df[['DRG', 'DRG_lbl']]
    Save(df2, 'c:/AdvAnalytics/Reference/code_DRG')

    # Create a file that has codes and associated names for the base DRG categories.
    df = df[['BaseDRG', 'DRG_lbl']]
    df['DRG_lbl']=df.DRG_lbl.apply(lambda x : x.replace(' w/o CC/MCC', ''))
    df['DRG_lbl']=df.DRG_lbl.apply(lambda x : x.replace(' w/o MCC/CC', ''))
    df['DRG_lbl']=df.DRG_lbl.apply(lambda x : x.replace(' w/ CC/MCC', ''))
    df['DRG_lbl']=df.DRG_lbl.apply(lambda x : x.replace(' w/ MCC/CC', ''))
    df['DRG_lbl']=df.DRG_lbl.apply(lambda x : x.replace(' w/ MCC', ''))
    df['DRG_lbl']=df.DRG_lbl.apply(lambda x : x.replace(' w/ CC', ''))
    df['DRG_lbl']=df.DRG_lbl.apply(lambda x : x.replace(' w/o CC', ''))
    df['DRG_lbl']=df.DRG_lbl.apply(lambda x : x.replace(' w/o MCC', ''))
    df.rename(columns={'DRG_lbl' : 'BaseDRG_lbl'}, inplace=True)
    df.drop_duplicates(inplace=True)
    Save(df, 'c:/AdvAnalytics/Reference/code_BaseDRG')

#########################
## Facility Type Codes ##
#########################
# These codes are part of the UB-04 form.  Source: https://www.resdac.org/cms-data/variables/Claim-Facility-Type-Code
def readFacilityType():
    df = pd.read_csv(InputDir + '/FacilityType.txt', delimiter='\t')
    Save(df, OutputDir + '/code_FacilityType')
readFacilityType()


############################
## Place of Service Codes ##
############################
def readServicePlace():
    df = pd.read_table(InputDir + '/ServicePlace.txt', delimiter='\t', quotechar='"')
    Save(df, OutputDir + 'ref_ServicePlace')
    df = df[['ServicePlace', 'ServicePlace_lbl']]
    Save(df, OutputDir + 'code_ServicePlace')
readServicePlace()

######################################
## Line Level Type of Service Codes ##
######################################
# These are Part B line level type of service codes.  These codes are found on DME and Carrier file claims, but not on hospital
# outpatient claims.  To address that issue, I have added a code for '#' indicating hospital outpatient.  Not great, but it's the
# best I could do with those.  Source: https://www.resdac.org/sites/resdac.umn.edu/files/CMS%20Type%20of%20Service%20Table.txt
def readLineServiceType():
    df = pd.read_csv(InputDir + 'ServiceType.txt', delimiter='=', names=['ServiceType', 'ServiceType_lbl'],
                     dtype={'ServicePlace':'str'})
    for c in df.columns.tolist():
        df[c] = df[c].apply(lambda x: x.strip())
    Save(df, OutputDir + 'code_ServiceType')
readLineServiceType()

#####################################
## CCS Procedure Codes (CPT/HCPCS) ##
#####################################
# This is a new mapping from CPT/HCPCs to CCS Procedure Categories.  I suspect that it will be the successor to BETOS.
# It's tricky to work with because it has code ranges.
# Source: https://www.hcup-us.ahrq.gov/toolssoftware/ccs_svcsproc/ccscpt_downloading.jsp
def readCCSCPT():
    df = pd.read_csv(InputDir + '2017_ccs_services_procedures.csv', skiprows=1)
    df.columns=['CodeRange', 'CCSProc', 'CCSProc_lbl']
    df['CodeRange'] = df.CodeRange.apply(lambda x: x.replace("'", ""))
    df['CCSProc_lbl'] = df.CCSProc_lbl.apply(lambda x: x.replace('"', ''))
    df['CodeRangeStart'], df['CodeRangeEnd'] = df['CodeRange'].str.split('-', 1).str
    df = df[['CodeRange', 'CodeRangeStart', 'CodeRangeEnd', 'CCSProc', 'CCSProc_lbl']]
    Save(df, OutputDir + 'ref_CCSProcs')
    df1 = df[['CCSProc', 'CCSProc_lbl']]
    df1.drop_duplicates(inplace=True)
    df1.sort_values('CCSProc', inplace=True)
    Save(df1, OutputDir + '')
readCCSCPT()

#########################
## CPT and HCPCS Codes ##
#########################
# This is one of the most complicated table loads in the reference database.  It first gets the Medicare Coverage
# Database procedures file.  That file includes the history of each code, so we need to keep the latest version of
# each code.  In addition, I merge this file the CCS crosswalks to make it much easier and faster to get the CCS
# Procedure Groups onto the claims.  NOTE THAT THIS TABLE IS INCOMPLETE. It does not contain many CPT/HCPCS codes.

# Source: https://www.cms.gov/medicare-coverage-database/downloads/downloadable-databases.aspx  On that page, you need
# to select "Current LDCs"  That will download a zip file that contains a zip archive within it.  Open the zip
# archive and get the needed file.
def readCPT():
    df = pd.read_csv(InputDir + '/lcd_x_hcpc_code.csv', delimiter=',', quotechar='"',
                     dtype={'last_updated': 'str',
                            'hcpc_code': 'str'},
                     encoding='Latin1')
    df.columns = ['del', 'del2', 'CPT', 'del3', 'CPTGroup', 'Range', 'LastUpdate', 'LongDescription', 'CPT_lbl']
    for c in df.columns.tolist():
        if c[:3]=='del':
            del df[c]
    df['CPT'] = df.CPT.apply(lambda x: x.replace('Pet ', 'PET '))
    df['CPT'] = df.CPT.apply(lambda x: x.replace('Mri ', 'MRI '))
    df['CPT'] = df.CPT.apply(lambda x: x.replace('Ct ', 'CT '))
    df['CPT'] = df.CPT.apply(lambda x: x.replace('w/ct ', 'w/CT '))
    df['CPT'] = df.CPT.apply(lambda x: x.replace('Srs ', 'SRS '))
    df['CPT'] = df.CPT.apply(lambda x: x.replace('Us ', 'US '))
    df.sort_values(['CPT', 'LastUpdate'], inplace=True)
    del df['LastUpdate']
    df = df.groupby('CPT').last().reset_index()
    del df['CPTGroup']
    import sqlite3
    #Import the xwalk file
    xw = Use('c:/AdvAnalytics/Reference/ref_CCSProcs')
    xw = xw[['CodeRangeStart', 'CodeRangeEnd', 'CCSProc']]
    # Prep the CPT dataframe
    df['RowID'] = range(len(df))
    dfw = df[['RowID', 'CPT']]
    # Create the db in memory
    conn = sqlite3.connect(':memory:')
    # Put the tables into the memory db
    xw.to_sql('xw', conn, index=False)
    dfw.to_sql('df', conn, index=False)
    qry = '''
        SELECT df.RowID, xw.CCSProc
        FROM df, xw
        WHERE df.CPT between xw.CodeRangeStart and xw.CodeRangeEnd
        '''
    print('Starting query at ' + ctime())
    dfres = pd.read_sql_query(qry, conn)
    print('Finished query at ' + ctime())
    df = pd.merge(df, dfres, on='RowID', how='left')
    del df['RowID']
    df.CCSProc.fillna(-1, inplace=True)
    df['CCSProc']=df.CCSProc.astype('int16')
    Save(df, OutputDir + 'ref_CPT')
    dfx = df[['CPT', 'CCSProc']]
    Save(dfx, OutputDir + 'xw_CPT2CCSProc')
    df = df[['CPT', 'CPT_lbl']]
    Save(df, OutputDir + 'code_CPT')
readCPT()

################################################
## CPT/HCPCS and RVUs and related information ##
################################################
# After trying the previous approach, I found a different way to get the CPT codes, by downloading the physician fee
# schedule files.  This function reads that dataset (which is an ugly mess) and extracts both an RVU file and the CPT
# mapping files that we need.  It uses the same logic as the prior file to add CCSProc to the CPT codes.
# Source:
#    https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/PhysicianFeeSched/PFS-Relative-Value-Files-Items/RVU17C.html?DLPage=1&DLEntries=10&DLSort=0&DLSortDir=descending
def readRVU():
    df = pd.read_fwf(InputDir + 'PPRRVU17_JULY_V0503.txt', skiprows=7, header=None,
                     widths=(5,2,50,1,7,7,2,7,2,6,7,6,1,3,3,3,3,1,1,1,1,1,7,5,8,1,2,1,1,2,10,7,6),
                     names=['CPT', 'CPTMod', 'CPT_lbl', 'StatusCode', 'WorkRVU', 'NonFacilityExpenseRVU',
                            'NonFacilityNA', 'FacilityExpenseRVU', 'FacilityNA', 'MalpracticeRVU',
                            'TotalNonFacilityRVU', 'TotalFacilityRVU', 'PC_TCIndicator', 'GlobalSurgery',
                            'PreOpPct', 'IntraOpPct', 'PostOpPct', 'MultiProcFlag', 'BilateralSurgFlag',
                            'AsstSurgeonFlag', 'CoSurgeonFlag', 'TeamSurgeryFlag', 'Filler',
                            'EndoscopicBaseCode', 'ConversionFactor', 'del1', 'PhysSupervision', 'del2',
                            'DiagImagingFamily', 'NonFacPracticeAmt', 'FacilityPracticeAmt',
                            'MalpracticeAmt', 'del4'])
    for c in df.columns.tolist():
        if c[:3]=='del':
            del df[c]
    df['CPT'] = df.CPT.astype('str')
    Save(df, OutputDir + 'ref_RVU')
    df = df[['CPT', 'CPT_lbl']]
    df.drop_duplicates(subset='CPT', inplace=True)
    Save(df, OutputDir + 'code_CPT')
    import sqlite3
    #Import the xwalk file
    xw = Use('c:/AdvAnalytics/Reference/ref_CCSProcs')
    xw = xw[['CodeRangeStart', 'CodeRangeEnd', 'CCSProc']]
    # Prep the CPT dataframe
    df['RowID'] = range(len(df))
    dfw = df[['RowID', 'CPT']]
    # Create the db in memory
    conn = sqlite3.connect(':memory:')
    # Put the tables into the memory db
    xw.to_sql('xw', conn, index=False)
    dfw.to_sql('df', conn, index=False)
    qry = '''
        SELECT df.RowID, xw.CCSProc
        FROM df, xw
        WHERE df.CPT between xw.CodeRangeStart and xw.CodeRangeEnd
        '''
    print('Starting query at ' + ctime())
    dfres = pd.read_sql_query(qry, conn)
    print('Finished query at ' + ctime())
    df = pd.merge(df, dfres, on='RowID', how='left')
    del df['RowID']
    df.CCSProc.fillna(-1, inplace=True)
    df['CCSProc']=df.CCSProc.astype('int16')
    Save(df, OutputDir + 'ref_CPT')
    dfx = df[['CPT', 'CCSProc']]
    Save(dfx, OutputDir + 'xw_CPT2CCSProc')
readRVU()

##########################
## ICD to HCC Crosswalk ##
##########################
# This section provides crosswalks between diagnosis codes and HCC (hierarchical coexisting conditons) categories.
# The HCCs are used in a number of different projects including OCM.
# The input data for these crosswalks are found at http://www.nber.org/data/icd-hcc-crosswalk-icd-rxhcc-crosswalk.html
def icd2HCC():
    df = pd.read_stata('c:/AdvAnalytics/Reference/Rawdata/icd2hccxw2014.dta')
    df = df[['icd', 'hcc']]
    df = df[df.hcc.between(1,1000)]
    df['icd'] = df.icd.apply(lambda x : str(x))
    df['hcc'] = df.hcc.apply(lambda x : str(int(x)))
    df.rename(columns={'icd': 'Dx',
                       'hcc': 'HCC'}, inplace=True)
    df.drop_duplicates(inplace=True)
    print(df.head())
    Save(df, 'c:/AdvAnalytics/Reference/Rawdata/xw_ICD9ToHCC')
    df = pd.read_stata('c:/AdvAnalytics/Reference/Rawdata/icd2hccxw2016.dta')
    df = df[['icd', 'hcc']]
    df = df[df.hcc.between(1,1000)]
    df['icd'] = df.icd.apply(lambda x : str(x))
    df['hcc'] = df.hcc.apply(lambda x : str(int(x)))
    df = df[~df.hcc.isnull()]
    df.drop_duplicates(inplace=True)
    df.rename(columns={'icd': 'Dx',
                       'hcc': 'HCC'}, inplace=True)
    Save(df, 'c:/AdvAnalytics/Reference/Rawdata/xw_ICD10ToHCC')
    print(df.head())
    return
icd2HCC()


################
## HCC Labels ##
################
# This code generates the HCC code descriptions.  The best source I could find for the HCC codes was in the SAS code.
# The file needs to be edited before reading it into this script.  In particular, there are a few HCCs where the
# description of the code is on a separate line from the code itself.  In those cases, you need to delete the carriage
# return and put the description on the same line as the HCC code. It's also possible that you will want to have either
# a flag indicating what year's code the description maps to, as the codes change meaning over time, or separate
# reference files by year.
# The source for this file is:  https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Risk-Adjustors.html
def HCCLabels():
    df = pd.read_csv(InputDir + '/V22H79L1.txt', skiprows=8, header=None,
                     names=['HCC', 'HCC_lbl'], delimiter='=', quotechar='"' )
    df['HCC'] = df.HCC.apply(lambda x: x.strip())
    df['HCC'] = df.HCC.apply(lambda x: x[3:])
    df['HCC'] = df.HCC.apply(lambda x: int(x))
    Save(df, OutputDir + '/code_HCC')
    return df
df = HCCLabels()


# ## HCC List
# This code generates a list of HCC codes with a flag indicating if the code is used in OCM.  
dfhcc = df.copy()
df = pd.read_excel('c:/AdvAnalytics/OCM/Reference/Input/ocm-predictionmodel-codelist.xlsx',
                  sheetname='ComorbidityFlags', skiprows=2)
df.columns=['HCC', 'Description']
df['HCC'] = df.HCC.apply(lambda x : x[4:])
df['HCC'] = df.HCC.apply(lambda x : int(x))
del df['Description']
df['Used In OCM'] = 'Y'
types = df.apply(lambda x: pd.lib.infer_dtype(x.values))
df = pd.merge(dfhcc, df, how='outer', on='HCC')
df['HCC'] = df.HCC.apply(lambda x : int(x))
df['Used In OCM'].fillna('N', inplace=True)
Save(df, 'c:/AdvAnalytics/OCM/Reference/ref_OCMHCC')


#########################
## Home Health Compare ##
#########################
# This section loads the Home Health Compare provider file.  The source can be found at
# https://data.medicare.gov/data/home-health-compare.
df = pd.read_csv('c:/AdvAnalytics/Reference/Input/HHC_SOCRATA_HHCAHPS_PRVDR.csv',
                 dtype = {'Zip' : 'str', 
                         'CMS Certification Number (CCN)' : 'str'},
                 parse_dates=['Date Certified'])
df.columns=['State','CCN','ProviderName','Address','City','Zip','Phone',
            'OwnershipType','OffersNursing','OffersPT','OffersOT','OffersST',
            'OffersMedicalSocialServices','OffersHHAides','DateCertified',
            'HHCAHPSStars','FNHHCAHPS','ProfessionalCareStars',
            'FNProfessionalCare','ProfessionalCareReportedPctPatients','FNProfCarePct',
            'CommunicationStars','FNCommunication','CommunicationPctPatients',
            'FNCommPct','DiscussedMedsPainSafetyStars','FNStarsDiscussion',
            'DiscussionPctPatients','FNDiscusionPctPatients',
            'OverallRatingStars','FNOverallStars','PctPatientsTopOverallRanking',
            'FNOverallRankPct','PctPatientsWouldRecommend','FNPctWouldRecommend',
            'CompletedSurveys','FNnumberofcompletedsurveys','Responserate','Footnoteforresponserate']
Save(df, OutputDir + '/HHCompare')


######################
## Hospital Compare ##
######################
# Source: https://data.medicare.gov/data/hospital-compare
# I use the Hospital General Information file.
# CCN is used like in Home Health Compare and needs similar formatting.


##########################
## Nursing Home Compare ##
##########################
# Source: https://data.medicare.gov/data/nursing-home-compare
# I use the ProviderInfo_Download file/table
# Same formatting issues apply to CCN


#######################
## Physician Compare ##
#######################
# Source:  https://data.medicare.gov/data/physician-compare
# I use this file primarily to understand provider affiliations.  It is the only place I know of where one can find PECOS data.
# I load only the “big file” from the archive.


##############
## NPI Data ##
##############
# This is here only because it doesn't have another home yet.
# This is the code I used to import the NPPES NPI files (http://download.cms.gov/nppes/NPI_Files.html)
dfnpi = pd.read_csv(InputDir + 'NPI.csv',
                    usecols=['NPI', 'Entity Type Code', 'Employer Identification Number (EIN)',
                             'Provider Organization Name (Legal Business Name)',
                             'Provider Last Name (Legal Name)', 'Provider First Name', 'Provider Middle Name',
                             'Provider First Line Business Mailing Address', 'Provider Second Line Business Mailing Address',
                             'Provider Business Mailing Address City Name', 'Provider Business Mailing Address State Name',
                             'Provider Business Mailing Address Postal Code',
                             'Provider Business Mailing Address Country Code (If outside U.S.)',
                             'Provider Business Mailing Address Telephone Number',
                             'Provider First Line Business Practice Location Address',
                             'Provider Second Line Business Practice Location Address',
                             'Provider Business Practice Location Address City Name',
                             'Provider Business Practice Location Address State Name',
                             'Provider Business Practice Location Address Postal Code',
                             'Provider Business Practice Location Address Country Code (If outside U.S.)',
                             'Provider Business Practice Location Address Telephone Number',
                             'Provider Gender Code',
                             'Healthcare Provider Taxonomy Code_1',
                             'Healthcare Provider Taxonomy Code_2',
                             'Healthcare Provider Taxonomy Code_3',
                             'Is Sole Proprietor', 'Is Organization Subpart',
                             'Parent Organization LBN', 'Parent Organization TIN',
                             'Healthcare Provider Taxonomy Group_1', 'Healthcare Provider Taxonomy Group_2'],
                   dtype={'Employer Identification Number (EIN)' : 'str',
                          'Provider Business Mailing Address Postal Code' : 'str',
                          'Provider Business Practice Location Address Postal Code' : 'str',
                          'Healthcare Provider Taxonomy Code_1' : 'str',
                          'Healthcare Provider Taxonomy Code_2' : 'str',
                          'Healthcare Provider Taxonomy Code_3' : 'str',
                          'Parent Organization TIN' : 'str',
                          'Healthcare Provider Taxonomy Group_1' : 'str',
                          'Healthcare Provider Taxonomy Group_2' : 'str',
                          'Provider Business Mailing Address Telephone Number' : 'str',
                          'Provider Business Practice Location Address Telephone Number' : 'str'})
listCols=list(dfnpi.columns)
dfnpi['NPI_lbl'] = (dfnpi['Provider Last Name (Legal Name)'].apply(lambda x: str(x).title()) +
                    ', ' + dfnpi['Provider First Name'].apply(lambda x: str(x).title()) +
                    dfnpi['Provider Middle Name'].apply(lambda x: str(x)[:1]))
Save(dfnpi, OutputDir + 'ref_NPI')
dfnpi = dfnpi[['NPI', 'NPI_lbl']]
Save(dfnpi, OutputDir + 'code_NPI')


########################
## NPI Taxonomy Codes ##
########################
# These files are found at http://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57
def readTaxonomy():
    df = pd.read_csv(InputDir + '/nucc_taxonomy_171.csv')
    df.rename(columns={'Code': 'NPITaxonomy'}, inplace=True)
    df['NPITaxonomy_lbl'] = df.Classification + ': ' + df.Specialization
    Save(df, OutputDir + '/ref_NPITaxonomy')
    df = df[['NPITaxonomy', 'NPITaxonomy_lbl']]
    Save(df, OutputDir + '/code_NPITaxonomy')
readTaxonomy()


#################
## HCPCS codes ##
#################
# This only has the Level 2 codes, not the CPT codes.
# HCPCs file can be found at https://www.cms.gov/medicare/coding/hcpcsreleasecodesets/alpha-numeric-hcpcs.html
def readHCPCS():
    df = pd.read_excel(InputDir + '/HCPCS.xlsx')
    df = df[['HCPC', 'BETOS', 'TOS1']]
    for c in list(df.columns):
        df[c] = df[c].astype(str)
    Save(df, OutputDir + '/xw_HCPCS_TOS_BETOS')
    df = pd.read_excel(InputDir + '/HCPCS.xlsx')
    df = df[['HCPC', 'SHORT DESCRIPTION', 'BETOS']]
    df.columns = ['hcpcs', 'hcpcs_lbl', 'BETOS']
    for c in list(df.columns):
        df[c] = df[c].astype(str)
    Save(df, OutputDir + '/ref_hcpcs')

    df = pd.read_excel(InputDir + '/HCPCS.xlsx')
    df = df[df.PRICE1==51]
    df = df[['HCPC', 'SHORT DESCRIPTION']]
    df.columns = ['CPT', 'CPT_lbl']
    for c in list(df.columns):
        df[c] = df[c].apply(lambda x : x.encode('ascii', 'ignore'))
    df.reset_index(inplace=True)
    del df['index']
    Save(df, OutputDir + '/Filter_HCPCS_Drugs')
readHCPCS()

###############
## CCS Codes ##
###############
# This section imports the single level CCS diagnosis and procedure classifications.
# The source is https://www.hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp
def readCCS():
    df = pd.read_csv('c:/AdvAnalytics/Reference/RawData/$dxref 2015.csv',header=1, quotechar="'")
    df.columns = ['Dx', 'CCS', 'CCS_lbl', 'Dx_lbl', 'skip', 'skip2']
    df = df[['Dx', 'CCS', 'CCS_lbl', 'Dx_lbl']]
    df['Dx']=df.Dx.apply(lambda x : str(x))
    df['Dx']=df['Dx'].apply(lambda x : x.strip())
    Save(df, 'c:/AdvAnalytics/Reference/ref_CCS')
    df2=df[['Dx', 'CCS']]
    Save(df2, 'c:/AdvAnalytics/Reference/xw_Dx2CCS')
    df2=df[['CCS', 'CCS_lbl']]
    Save(df2, 'c:/AdvAnalytics/Reference/code_CCS')


    df = pd.read_csv('c:/AdvAnalytics/Reference/RawData/$prref 2015.csv',header=1, quotechar="'")
    df.columns = ['Px', 'PxCCS', 'PxCCS_lbl', 'Px_lbl']
    df = df[['Px', 'PxCCS', 'PxCCS_lbl', 'Px_lbl']]
    df['Px']=df.Px.apply(lambda x : str(x))
    df['Px']=df['Px'].apply(lambda x : x.strip())
    Save(df, 'c:/AdvAnalytics/Reference/ref_CCSPx')
    df2=df[['Px', 'PxCCS']]
    Save(df2, 'c:/AdvAnalytics/Reference/xw_Px2CCSPx')
    df2=df[['PxCCS', 'PxCCS_lbl']]
    Save(df2, 'c:/AdvAnalytics/Reference/code_CCSPx')

    # This section imports the ICD-10 mappings.  I had to get rid of the single quote separators.
    df = pd.read_csv('c:/AdvAnalytics/Reference/RawData/ccs_dx_icd10cm_2017.csv',header=0, quotechar='"')
    df.columns = ['Dx', 'CCS', 'Dx_lbl', 'CCS_lbl',
                  'MultiCCS1', 'MultiCCS1_lbl', 'MultiCCS2', 'MultiCCS2_lbl']
    df['Dx'] = df.Dx.apply(lambda x: x[1:])
    Save(df, 'c:/AdvAnalytics/Reference/ref_CCS_ICD10')
    df2 = df[['Dx', 'Dx_lbl']]
    Save(df2, 'c:/AdvAnalytics/Reference/code_dx10')
    df2 = df[['Dx', 'CCS']]
    Save(df2, 'c:/AdvAnalytics/Reference/xw_Dx10ToCCS')
    df2 = df[['CCS', 'CCS_lbl']].copy()
    df2.drop_duplicates(inplace=True)
    Save(df2, 'c:/AdvAnalytics/Reference/code_CCS')
    df2 = df[['MultiCCS1', 'MultiCCS1_lbl']].copy()
    df2.drop_duplicates(inplace=True)
    Save(df2, 'c:/AdvAnalytics/Reference/code_MultiCCS1')
    df2 = df[['MultiCCS2', 'MultiCCS2_lbl']].copy()
    df2.drop_duplicates(inplace=True)
    Save(df2, 'c:/AdvAnalytics/Reference/code_MultiCCS2')
readCCS()

############################
## ICD-10 Diagnosis Codes ##
############################
# These data come from https://www.cms.gov/Medicare/Coding/ICD10/2017-ICD-10-CM-and-GEMs.html
# Use the 2MB file to get the data that you need.
def readICD10():
    df = pd.read_table(InputDir + '/icd10cm_codes_2017.txt', delimiter='\t', header=None,
                    names=['Dx'])
    df['Dx_lbl'] = df.Dx.apply(lambda x: x[7:])
    df['Dx'] = df.Dx.apply(lambda x : x[:7])
    df['Dx'] = df.Dx.apply(lambda x : x.strip())
    df['Dx_lbl'] = df.Dx_lbl.apply(lambda x: x.strip())
    for c in list(df.columns):
        df[c] = df[c].astype(str)
    Save(df, OutputDir + 'code_Dx')
readICD10()


##################################
## General Equivalence Mappings ##
##################################
# These files come from the same place as the ICD10 files.
def readGEM():
    df = pd.read_table(InputDir + '/2017_I9gem.txt', delim_whitespace=True, header=None,
                       names = ['ICD9', 'ICD10', 'Junk'],
                       dtype={'ICD9': 'str',
                              'ICD10':'str'})
    del df['Junk']
    Save(df, OutputDir + '/xw_I9_To_I10')
    df = pd.read_table(InputDir + '/2017_I10gem.txt', delim_whitespace=True, header=None,
                       names = ['ICD10', 'ICD19', 'Junk'],
                       dtype={'ICD9': 'str',
                              'ICD10':'str'})
    del df['Junk']
    Save(df, OutputDir + '/xw_I10_To_I9')
readGem()

#################
## BETOS Codes ##
#################
# BETOS is no longer being updated.  I got these descriptions from the 2016 Record Layout table in
# https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets/Alpha-Numeric-HCPCS.html.
# I copied and pasted the section with the BETOS codes to LibreOffice, then read them below.
# I found a newer, better crosswalk/rollup at this site.
# https://www.reddit.com/r/healthIT/comments/44b8f4/berenson_eggers_type_of_service_betos_codes/ 
def readBETOS():
    df = pd.read_table(InputDir + 'betpuf14.txt', delim_whitespace=True, header=None,
                       names=['CPT', 'BETOS'], dtype={'CPT': 'str'})
    Save(df, OutputDir + '/xw_CPT2BETOS')
    df = pd.read_csv(InputDir + 'cms_betos_code_description_20160217.csv')
    df.columns = ['BETOS', 'Level1Group', 'Level1Group_lbl', 'Level2Group', 'Level2Group_lbl',
                  'Level3Group', 'Level3Group_lbl']
    Save(df, OutputDir + 'ref_BETOS')
    df = df[['BETOS', 'Level1Group']]
    df.columns = ['BETOS', 'BETOS_lbl']
    Save(df, OutputDir + 'code_BETOS')
readBETOS()

# ## Type of Service Codes (related to HCPCS)
# I got these descriptions from the 2017 Record Layout table in
# https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets/Alpha-Numeric-HCPCS.html.
# I copied and pasted the section with the TOS codes to an editor, then read them below.
# Currently not working and not urgent.
df = pd.read_excel(InputDir + '/ref_ServiceType.xlsx')
for c in list(df.columns):
    df[c]=df[c].apply(lambda x : str(x))
    df[c]=df[c].apply(lambda x : x.strip())
Save(df, OutputDir + '/ref_ServiceType')

################################
## Oncology MS-DRG categories ##
################################
# This list was drawn from the American Hospital Directory.  https://www.ahd.com/hlp/msdrg/2010/Oncology.html
def readOncoDRG():
    df = pd.read_table('c:/AdvAnalytics/Reference/RawData/OncologyDRGs.txt', dtype={'MS-DRG' : 'object'})
    df.columns = ['DRG', 'DRG_lbl']
    Save(df, OutputDir + 'list_OncologyDRGs')
readOncoDRG()

## HCFA Specialty Codes
# These descriptions come from https://www.resdac.org/sites/resdac.umn.edu/files/HCFA%20Provider%20Specialty%20Table.txt
def readHCFASpec():
    df = pd.read_table(InputDir + '/hcfaspec.txt', delimiter='=', names=['HCFASpecialty', 'HCFASpecialty_lbl'],
                       dtype={'HCFASpecialty': 'str'})
    for c in df.columns.tolist():
        df[c] = df[c].apply(lambda x: x.strip())
    Save(df, OutputDir + '/code_HCFASpecialty')
readHCFASpec()

#########################################
## Revenue Center (Revenue Code) Codes ##
#########################################
# These descriptions come from http://www.resdac.org/sites/resdac.org/files/Revenue%20Center%20Table.txt
def readRevCode():
    df1 = pd.read_table(InputDir + 'RevCodes.txt', header=None, names=['x'])
    df = pd.DataFrame(df1.x.str.split(' = ',1).tolist(),
                                       columns = ['RevenueCode', 'RevenueCode_lbl'])
    for c in df.columns.tolist():
        df[c] = df[c].apply(lambda x: x.strip())
    Save(df, OutputDir + 'ref_RevenueCode')
readRevCode()

############################
## Discharge Status Codes ##
############################
def readDischargeStatus():
    df = pd.read_table(InputDir + '/DischargeStatus.txt', header=None, names=['x'])
    df['DischargeStatus'] = df.x.apply(lambda x: x[:2].strip())
    df['DischargeStatus_lbl'] = df.x.apply(lambda x: x[3:])
    del df['x']
    Save(df, OutputDir + '/code_DischargeStatus')
readDischargeStatus()


###############################
## Dartmouth Atlas Geography ##
###############################
# ## Zip Codes, Hospital Service Areas, and Hospital Referral Regions
# The data for this come from http://www.dartmouthatlas.org/tools/downloads.aspx/?tab=39
def readDartmouth():
    df = pd.read_excel(InputDir + '/ZipHsaHrr15.xls')
    df.columns = ['ZipCode', 'HSA', 'HSACity', 'HSAState', 'HRR', 'hrrcity', 'hrrstate']
    df['ZipCode'] = df.ZipCode.apply(lambda x : str(x))
    df['ZipCode'] = df.ZipCode.apply(lambda x : x.zfill(5))
    df1 = df[['ZipCode', 'HSA']]
    Save(df1, OutputDir + '/xw_Zip2HSA')
    df1 = df[['ZipCode', 'HRR']]
    Save(df1, OutputDir + '/xw_Zip2HSA')
    df['HSA_lbl'] = df.HSACity + ', ' + df.HSAState
    df['HRR_lbl'] = df.hrrcity + ', ' + df.hrrstate
    df['HSA_lbl'] = df.HSA_lbl.apply(lambda x : str(x))
    df['HRR_lbl'] = df.HRR_lbl.apply(lambda x : str(x))
    df1 = df[['HRR', 'HRR_lbl']]
    df1.drop_duplicates(inplace=True)
    Save(df1, OutputDir + '/code_HRR')
    df1 = df[['HSA', 'HSA_lbl']]
    df1.drop_duplicates(inplace=True)
    Save(df1, OutputDir + '/code_HSA')
readDartmouth()

#########################
## State Abbreviations ##
#########################
# From http://statetable.com/
def readStates():
    df = pd.read_csv(InputDir + '/state_table.csv')
    df.fillna(0, inplace=True)
    df['fips_state'] = df['fips_state'].apply(lambda x : int(x))
    df['census_region'] = df['census_region'].apply(lambda x: int(x))
    del df['notes']
    del df['assoc_press']
    for c in ['standard_federal_region', 'census_region_name', 'census_division_name',
              'circuit_court']:
        df[c] = df[c].apply(lambda x: str(x))
    Save(df, OutputDir + '/ref_states')
readStates()

# ## Cancer Therapy Code Lookup Files
# These files come from the NIH Cancer Research Network site.  They give specific NDC, HCPCs, and CPT-4 codes that are commonly used for chemotherapy and cancer treatment.  The codes can be found at https://crn.cancer.gov/resources/codes.html
df = pd.read_csv('c:/AdvAnalytics/OCM/Reference/Input/ctcodes-drugs.csv',
                 dtype={'NDC' : 'str'})
Save(df, 'c:/AdvAnalytics/Reference/CancerNDC')
df = pd.read_csv('c:/AdvAnalytics/OCM/Reference/Input/ctcodes-procedures.csv',
                 dtype={'PX' : 'str'})
df.rename(columns={'PX' : 'CPT'}, inplace=True)
Save(df, 'c:/AdvAnalytics/Reference/CancerProcs')

###############################
## Provider of Service files ##
###############################
# This file is obtained from https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Provider-of-Services/
def readProviderOfService():
    dictDtype = {'PRVDR_CTGRY_SBTYP_CD': 'str', 'PRVDR_CTGRY_CD': 'str', 'CITY_NAME': 'str', 'SSA_CNTY_CD': 'str',
                 'CROSS_REF_PROVIDER_NUMBER': 'str', 'FAC_NAME': 'str', 'PRVDR_NUM': 'str', 'RGN_CD': 'str',
                 'STATE_CD': 'str', 'SSA_STATE_CD': 'str', 'STATE_RGN_CD': 'str', 'PHNE_NUM': 'str',
                 'PGM_TRMNTN_CD': 'str', 'ZIP_CD': 'str', 'FIPS_STATE_CD': 'str', 'FIPS_CNTY_CD': 'str',
                 'CBSA_URBN_RRL_IND': 'str', 'CBSA_CD': 'str', 'MEDICARE_MEDICAID_PRVDR_NUMBER': 'str',
                 'PARENT_PROVIDER_NUMBER': 'str'}
    df = pd.read_csv('c:/AdvAnalytics/Reference/RawData/POS_OTHER_JUN17.csv', dtype=dictDtype)
    df = df[['PRVDR_CTGRY_SBTYP_CD', 'PRVDR_CTGRY_CD', 'CITY_NAME', 'SSA_CNTY_CD', 'CROSS_REF_PROVIDER_NUMBER',
             'FAC_NAME', 'PRVDR_NUM', 'RGN_CD', 'STATE_CD', 'SSA_STATE_CD', 'STATE_RGN_CD', 'ST_ADR', 'PHNE_NUM',
             'PGM_TRMNTN_CD', 'ZIP_CD', 'FIPS_STATE_CD', 'FIPS_CNTY_CD', 'CBSA_URBN_RRL_IND', 'CBSA_CD',
             'ACRDTN_TYPE_CD', 'AFLTD_PRVDR_CNT', 'LAB_SRVC_CD', 'PHRMCY_SRVC_CD', 'RDLGY_SRVC_CD',
             'CRTFD_BED_CNT', 'MDCR_SNF_BED_CNT', 'HOSPC_BED_CNT', 'REHAB_BED_CNT', 'BED_CNT',
             'MEDICARE_MEDICAID_PRVDR_NUMBER', 'MLT_FAC_ORG_NAME', 'TOT_OFSITE_CNCR_HOSP_CNT',
             'PARENT_PROVIDER_NUMBER', 'CHMTHRPY_SRVC_CD', 'ICU_SRVC_CD', 'NUCLR_MDCN_SRVC_CD',
             'PHYSN_EMPLEE_SW', 'HH_AIDE_CNT', 'RN_CNT', 'EMPLEE_CNT']]
    df.rename(columns={'PRVDR_CTGRY_SBTYP_CD': 'ProvCategorySubtype',
                       'PRVDR_CTGRY_CD': 'ProviderCategory',
                       'CITY_NAME': 'City',
                       'SSA_CNTY_CD': 'SSACounty',
                       'CROSS_REF_PROVIDER_NUMBER': 'XrefProviderNumber',
                       'FAC_NAME': 'CCN_lbl',
                       'PRVDR_NUM': 'CCN',
                       'RGN_CD': 'Region',
                       'STATE_CD': 'State',
                       'SSA_STATE_CD': 'SSAState',
                       'STATE_RGN_CD': 'StateRegion',
                       'ST_ADR': 'StreetAddress',
                       'PHNE_NUM': 'PhoneNumber',
                       'PGM_TRMNTN_CD': 'ProgramTreatmentCode',
                       'ZIP_CD': 'ZipCode',
                       'FIPS_STATE_CD': 'FIPSState',
                       'FIPS_CNTY_CD': 'FIPSCounty',
                       'CBSA_URBN_RRL_IND': 'UrbanRuralFlag',
                       'CBSA_CD': 'CBSA',
                       'ACRDTN_TYPE_CD': 'AccreditationType',
                       'AFLTD_PRVDR_CNT': 'AffiliatedProviders',
                       'LAB_SRVC_CD': 'LabServiceCode',
                       'PHRMCY_SRVC_CD': 'PharmServiceCode',
                       'RDLGY_SRVC_CD': 'RadiologyServiceCode',
                       'CRTFD_BED_CNT': 'CertifiedBeds',
                       'MDCR_SNF_BED_CNT': 'SNFBeds',
                       'HOSPC_BED_CNT': 'HospiceBeds',
                       'REHAB_BED_CNT': 'RehabBeds',
                       'BED_CNT': 'Beds',
                       'MEDICARE_MEDICAID_PRVDR_NUMBER': 'medicareMedicaidProviderNumber',
                       'MLT_FAC_ORG_NAME': 'MilitaryFacility',
                       'TOT_OFSITE_CNCR_HOSP_CNT': 'OffsiteCancerHospitals',
                       'PARENT_PROVIDER_NUMBER': 'ParentCCN',
                       'CHMTHRPY_SRVC_CD': 'ChemoServiceCode',
                       'ICU_SRVC_CD': 'ICUServiceCode',
                       'NUCLR_MDCN_SRVC_CD': 'NuclearMedicineServiceCode',
                       'PHYSN_EMPLEE_SW': 'PhysicianEmployeeSwitch',
                       'HH_AIDE_CNT': 'HHAAides',
                       'RN_CNT': 'RNs',
                       'EMPLEE_CNT': 'Employees'}, inplace=True)
    Save(df, 'c:/AdvAnalytics/Reference/ref_ProviderOfService')
    df2 = df[['CCN', 'CCN_lbl']]
    Save(df2, 'c:/AdvAnalytics/Reference/code_CCN')
readProviderOfService()

##################################
## Crosswalk for HCPCS and NDC. ##
##################################
#  Link to get the crosswalk file: https://www.dmepdac.com/crosswalk/2017.html
#  You should get the latest file (replacing 2017 in the URL with the current year) and rename the file to
#  XWalkNDCHCPCS.xls
# 
# Its NDC column data has '-' in between numbers,
#  so we are removing those. The resulting NDC column is of string datatype. <br>
# 
#  Note that this file gets updates by the website every month so we might want to take updated one everytime
#  we use this.
def readNDCHCPCS():
    df = pd.read_excel(InputDir + '/XWalkNDCHCPCS.xls')
    df = df[['NDC', 'HCPCS']]
    df.sort_values('HCPCS', inplace=True)
    df['HCPCS'] = df.HCPCS.apply(lambda x: str(x))
    df.NDC= df.NDC.str.replace('-','')
    Save(df, OutputDir + '/xw_NDC2HCPCS')
    df.drop_duplicates(subset='HCPCS', inplace=True)
    Save(df, OutputDir + '/xw_HCPCS2NDC')
readNDCHCPCS()


##############################
## OCM Initiating Therapies ##
##############################
def readOCMInitCodes():
    df = pd.read_excel('c:/AdvAnalytics/OCM/Reference/Input/OCM Initiating Cancer Therapies and Codes Effective 07.02.17_Baseline_20170428.xlsx',
                       sheetname='NDC Codes',
                       dtype={'NDC CODE' : 'str'})
    df = df[['NDC CODE']]
    df.columns = ['NDC9']
    Save(df, 'c:/AdvAnalytics/OCM/Reference/list_InitiatingNDC')
    df = pd.read_excel('c:/AdvAnalytics/OCM/Reference/Input/OCM Initiating Cancer Therapies and Codes Effective 07.02.17_Baseline_20170428.xlsx',
                       sheetname='HCPCS Codes',
                       dtype={'HCPCS code' : 'str'})
    df = df[['HCPCS code']]
    df.columns = ['CPT']
    Save(df, 'c:/AdvAnalytics/OCM/Reference/list_InitiatingCPT')
readOCMInitCodes()

#########################
## MediSpan NDC tables ##
#########################
def readMediSpan():
    myQuery = '''
        SELECT 
              [NDC]
              ,[Drug_Name]
              ,[Dosage_Form]
              ,[route_of_administration]
              ,[dea_class_code]
              ,[bioequivalence_code]
              ,[controlled_substance_code]
              ,[brand_name_code]
              ,[generic_product_identifier]
          FROM [EDW].[MSTR].[NDC]
        '''
    server = 'BIdataca2'
    engine = sql.create_engine('mssql+pymssql://{}'.format(server))
    df = pd.read_sql_query(myQuery, engine)
    df.rename(columns={'DrugName': 'NDC_lbl',
                       'Dosage_Form': 'DosageForm',
                       'route_of_administration' : 'RouteOfAdministration',
                       'dea_class_code': 'DEAClassCode',
                       'bioequivalence_code': 'BioequivalenceCode',
                       'controlled_substance_code': 'ControlledSubstanceCode',
                       'brand_name_code': 'BrandNameDrug',
                       'generic_product_identifier': 'GPI'}, inplace=True)
    Save(df, OutputDir + 'ref_NDC')

    myQuery = '''
        SELECT TOP (1000) [DrugClassificationSK]
              ,[GenericProductIdentifier_Code]
              ,[Therapeutic_Class]
              ,[Drug_Group]
              ,[Drug_Class]
              ,[Drug_SubClass]
              ,[Drug_Base_Name]
              ,[Drug_Name_Extension]
              ,[Drug_Name_Dosage_Form]
          FROM [EDW].[MSTR].[DrugClassification]
        '''
    server = 'BIdataca2'
    engine = sql.create_engine('mssql+pymssql://{}'.format(server))
    df = pd.read_sql_query(myQuery, engine)
    df.rename(columns={'GenericProductIdentifier_Code': 'GPI',
                       'Therapeutic_Class': 'TherapeuticClassLevel1',
                       'Drug_Group': 'TherapeuticClassLevel2',
                       'Drug_Class': 'TherapeuticClassLevel3',
                       'Drug_SubClass': 'TherapeuticClassLevel4',
                       'Drug_Base_Name': 'DrugName',
                       'Drug_Name_Extension': 'DrugNameDetailed',
                       'Drug_Name_Dosage_Form': 'DrugNameWithDose'}, inplace=True)
    Save(df, OutputDir + 'ref_GPI')

    myQuery = '''
        SELECT 
              [Ndc_Upc_Hri] as NDC
              ,[Price_Code] as PriceCode
              ,[Price_Effective_Date] as PriceEffectiveDate
              ,[Unit_Price] as UnitPrice
              ,[Extended_Unit_Price] as ExtendedUnitPrice
              ,[Package_Price] as PackagePrice
              ,[Awp_Indicator_Code] as AWPIndicator
              ,[Transaction_Code] as TransactionCode
              ,[Last_Change_Date] as LastChangeDate
          FROM [EDW].[MSTR].[DrugPrice]
        '''
    server = 'BIdataca2'
    engine = sql.create_engine('mssql+pymssql://{}'.format(server))
    df = pd.read_sql_query(myQuery, engine)
    Save(df, OutputDir + 'ref_NDCPrice')
readMediSpan()


## FIPS County files are found at https://www.census.gov/geo/reference/codes/cou.html.  They need to be copied to a
## text file and column headers need to be added.
def readFIPS():
    df = pd.read_csv('/AdvAnalytics/Reference/RawData/FIPSCodes.csv',
                     dtype={'FIPSCounty': 'str'})
    Save(df, '/AdvAnalytics/Reference/ref_FIPS')
    dfx = df[['FIPSCounty', 'CountyName']]
    dfx.rename(columns={'CountyName': 'FIPSCounty_lbl'})
    Save(dfx, '/AdvAnalytics/Reference/code_FIPSCounty')
    toMySQL(dfx, 'reference', 'code_FIPSCounty')
readFIPS()

## Zip code crosswalks are found at: https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data.
def prepZipCodes():
    df = pd.read_excel(InputDir + '/Zip_County_062017.xlsx')
    df.sort_values(['ZIP', 'TOT_RATIO'], inplace=True)
    df.columns = ['ZipCode', 'County', 'PctResidentialAddress', 'PctBusinessAddress', 'PctOtherAddress',
                  'PctTotalAddress']
    print(len(df.index))
    df = df.groupby('ZipCode').last().reset_index()
    print(len(df.index))
    Save(df, OutputDir + '/ref_ZipCodeCounty')
    df = df[['ZipCode', 'County']]
    Save(df, OutputDir + '/xw_ZipCode2County')
prepZipCodes()


def prepRegimenXW():
    import pyodbc
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=BIDATACA2;DATABASE=EDW;UID=ebassin;\
                           PWD=ebassin;Trusted_Connection=yes')
    myQuery = '''
        SELECT
            X.RegimenSK as Regimen,
            R.RegimenDescription as Regimen_lbl,
            M.BillingCode as CPT,
            M.BrandName,
            M.MedicationTypeName,
            X.RegimenMedicationOrder 
        FROM
            EDW.DIM.Medication as M,
            EDW.DIM.Regimen as R,
            EDW.DIM.RegimenMedication as X
        WHERE
            M.MedicationSK = X.MedicationSK 
            And R.RegimenSK = X.RegimenSK
            And X.RegimenMedicationType = 'Required'
            '''
    df = pd.read_sql(myQuery, conn)
    df.loc[df.CPT=='J9999', 'CPT'] = df.BrandName.apply(lambda x: x[:5].upper())
    Save(df, '/AdvAnalytics/OCM/Reference/ref_regimenDetails')
    df1 = df[['CPT', 'Regimen', 'RegimenMedicationOrder']].copy()
    df1.drop_duplicates(inplace=True)
    Save(df1, '/AdvAnalytics/OCM/Reference/xw_CPT2Regimen')
    df1 = df[['Regimen', 'RegimenMedicationOrder']].copy()
    df1 = df1.groupby('Regimen').max()
    df1.reset_index(inplace=True)
    df1.rename(columns={'RegimenMedicationOrder': 'DrugCount'}, inplace=True)
    Save(df1, '/AdvAnalytics/OCM/Reference/info_RegimenDrugCount')
    df = df[['Regimen', 'Regimen_lbl']].drop_duplicates()
    Save(df, '/AdvAnalytics/OCM/Reference/code_regimen')
prepRegimenXW()
