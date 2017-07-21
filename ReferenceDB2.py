import sqlalchemy as sql
import pandas as pd
import numpy as np
import sys
sys.path.append('c:/Code/General')
from NCHGeneral import *

InputDir = 'c:/AdvAnalytics/Reference/Rawdata/'
OutputDir = 'c:/AdvAnalytics/Reference/'


# ## BETOS Codes
# BETOS is no longer being updated.  I got these descriptions from the 2016 Record Layout table in https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets/Alpha-Numeric-HCPCS.html.  I copied and pasted the section with the BETOS codes to LibreOffice, then read them below.  I found a newer, better crosswalk/rollup at this site.  https://www.reddit.com/r/healthIT/comments/44b8f4/berenson_eggers_type_of_service_betos_codes/
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


## Revenue Codes
df1 = pd.read_table(InputDir + 'RevCodes.txt', header=None, names=['x'])
df = pd.DataFrame(df1.x.str.split(' = ',1).tolist(),
                                   columns = ['RevenueCode', 'RevenueCode_lbl'])
for c in df.columns.tolist():
    df[c] = df[c].apply(lambda x: x.strip())
Save(df, OutputDir + 'ref_RevenueCode')


## MediSpan NDC tables
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
    SELECT 
          [GenericProductIdentifier_Code]
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


## NPI Codes
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
                    ', ' + dfnpi['Provider First Name'].apply(lambda x: str(x).title()) + ' ' + 
                    dfnpi['Provider Middle Name'].apply(lambda x: str(x)[:1]))
Save(dfnpi, OutputDir + 'ref_NPI')
dfnpi = dfnpi[['NPI', 'NPI_lbl']]
Save(dfnpi, OutputDir + 'code_NPI')


## CCS Coding
# This section imports the single level CCS diagnosis and procedure classifications for ICD-9.  
# The source is https://www.hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp
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
readGEM()


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


## Provider of Service files
# This file is obtained from https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Provider-of-Services/
dictDtype={'PRVDR_CTGRY_SBTYP_CD': 'str', 'PRVDR_CTGRY_CD': 'str', 'CITY_NAME': 'str', 'SSA_CNTY_CD': 'str', 
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
df['CCN_lbl'] = df.CCN_lbl.apply(lambda x: x.title())
Save(df, 'c:/AdvAnalytics/Reference/ref_ProviderOfService')
df2 = df[['CCN', 'CCN_lbl']]
Save(df2, 'c:/AdvAnalytics/Reference/code_CCN')
         


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

readDRG()

def icd2HCC():
    # ## ICD to HCC Crosswalk
    # This section provides crosswalks between diagnosis codes and HCC (hierarchical coexisting conditons) categories.
    # The HCCs are used in a number of different projects including OCM.
    # The input data for these crosswalks are found at http://www.nber.org/data/icd-hcc-crosswalk-icd-rxhcc-crosswalk.html
    df = pd.read_stata('c:/AdvAnalytics/Reference/Rawdata/icd2hccxw2014.dta')
    print(df.head(2).T)
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

df = Use('c:/AdvAnalytics/Reference/ref_CCS')

def HCCLabels():
    df = pd.read_csv(InputDir + '/V22H79L1.txt', skiprows=8, header=None, 
                     names=['HCC', 'HCC_lbl'], delimiter='=', quotechar='"' )
    df['HCC'] = df.HCC.apply(lambda x: x.strip())
    df['HCC'] = df.HCC.apply(lambda x: x[3:])
    df['HCC'] = df.HCC.apply(lambda x: int(x))
    return df
df = HCCLabels()
print(df.head())

def readTaxonomy():
    df = pd.read_csv(InputDir + '/nucc_taxonomy_171.csv')
    df.rename(columns={'Code': 'NPITaxonomy'}, inplace=True)
    df['NPITaxonomy_lbl'] = df.Classification + ': ' + df.Specialization
    print(df.head(2).T)
    Save(df, OutputDir + '/ref_Taxonomy')
readTaxonomy()

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


def readDischargeStatus():
    df = pd.read_table(InputDir + '/DischargeStatus.txt', header=None, names=['x'])
    df['DischargeStatus'] = df.x.apply(lambda x: x[:2].strip())
    df['DischargeStatus_lbl'] = df.x.apply(lambda x: x[3:])
    del df['x']
    Save(df, OutputDir + '/code_DischargeStatus')
    print(df.head())
readDischargeStatus()
    


