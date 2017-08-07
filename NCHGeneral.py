import pandas as pd
import numpy as np
import os
from time import ctime
import xlsxwriter
import feather
import pymysql
from sqlalchemy import create_engine

# the first two functions are my simplified methods for reading and writing dataframes in feather format.
def Use(fn):
    '''
    Read a feather format dataframe.

    :param fn: Name of the file to read
    :return: dataframe
    '''
    try:
        df = feather.read_dataframe(fn)
    except:
        df = feather.read_dataframe(fn + '.feather')
    return df

def Save(df, fn):
    '''
    Write a feather dataframe

    :param df: the dataframe to write
    :param fn: the file name, optionally including the path
    :return: None
    '''
    if fn.endswith('.feather')==True:
        #df.to_feather(fn)
        feather.write_dataframe(df, fn)
    else:
        feather.write_dataframe(df, fn + '.feather')
        #df.to_feather(fn + '.feather')
    print('  Saved file ' + fn + ': ' + ctime())
    return


def postAgg(df):
    '''
    This function takes an aggregated dataframe and converts the index columns to regular columns and takes the top
    level off the column labels.  It is commonly used after and aggregation function to put the dataframe back to the
    form that we typically use.

    :param df: the dataframe to restructure
    :return: the dataframe in "standard" form.
    '''
    df.columns = df.columns.droplevel(0)
    df.reset_index(inplace=True)
    return df


def RenameVars(df):
    dictRen = {'MBR' : 'Member',
               'MEMBER-SEX' : 'Sex',
               'BENE_ID': 'BeneSK',
               'CLM_ID': 'ClaimNum',
               'CLM_FROM_DT': 'FromDate',
               'CLM_THRU_DT': 'ThruDate',
               'NCH_WKLY_PROC_DT': 'ProcessDate',
               'CARR_NUM': 'CarrierNum',
               'CARR_CLM_PMT_DNL_CD': 'DenialCode',
               'CLM_PMT_AMT': 'Paid',
               'CARR_CLM_PRMRY_PYR_PD_AMT': 'PrimaryPayerPaid',
               'CARR_CLM_PRVDR_ASGNMT_IND_SW': 'ProviderAssignmentIndicatorSwitch',
               'NCH_CARR_CLM_ALOWD_AMT' : 'Allowed',
               'PRNCPAL_DGNS_CD': 'PrincipalDx',
               'PRNCPAL_DGNS_VRSN_CD': 'PrincipalDxVersion',
               'CLM_CLNCL_TRIL_NUM': 'ClinicalTrialNum',
               'BENE_CNTY_CD': 'PatientCity',
               'BENE_STATE_CD': 'PatientState',
               'BENE_MLG_CNTCT_ZIP_CD': 'PatientZip',
               'EP_ID': 'EpiNum',
               'LINE_NUM': 'LineNum',
               'TAX_NUM': 'TaxID',
               'PRVDR_SPCLTY': 'Specialty',
               'LINE_SRVC_CNT': 'Services',
               'LINE_CMS_TYPE_SRVC_CD': 'ServiceType',
               'LINE_PLACE_OF_SRVC_CD': 'ServicePlace',
               'LINE_1ST_EXPNS_DT': 'LineFromDate',
               'LINE_LAST_EXPNS_DT': 'LineThruDate',
               'HCPCS_CD': 'CPT',
               'HCPCS_1ST_MDFR_CD': 'CPTMod',
               'HCPCS_2ND_MDFR_CD': 'CPTMod2',
               'BETOS_CD': 'BETOS',
               'LINE_NCH_PMT_AMT': 'LinePaid',
               'LINE_ALOWD_CHRG_AMT': 'LineAllowed',
               'LINE_PRCSG_IND_CD': 'LineProcessingIndic',
               'LINE_ICD_DGNS_CD': 'LineDx',
               'LINE_ICD_DGNS_VRSN_CD': 'LineDxVersion',
               'PRVDR_NPI': 'NPI',
               'LINE_NDC_CD': 'NDC',
               'CLM_LINE_STD_PYMT_AMT': 'StdLinePaid',
               'BENE_HICN': 'HICNumber',
               'FIRST_NAME': 'FirstName',
               'LAST_NAME': 'LastName',
               'SEX': 'Sex',
               'DOB': 'BirthDate',
               'AGE': 'Age',
               'DOD': 'DeathDate',
               'ZIPCODE': 'ZipCode',
               'EP_BEG': 'EpiStart',
               'EP_END': 'EpiEnd',
               'EP_LENGTH': 'EpisodeLength',
               'CANCER_TYPE': 'CancerType',
               'RECON_ELIG': 'ReconciliationEligible',
               'DUAL_PTD_LIS': 'DualPartDLIS',
               'INST': 'Institutionalized',
               'RADIATION': 'RadiationFlag',
               'HCC_GRP': 'HCCCount',
               'HRR_REL_COST': 'HRRRelativeCost',
               'SURGERY': 'SurgeryFlag',
               'CLINICAL_TRIAL': 'ClinicalTrialFlag',
               'BMT': 'BoneMarrowTransplant',
               'CLEAN_PD': 'CleanPeriod',
               'PTD_CHEMO': 'PartDChemo',
               'ACTUAL_EXP': 'WinsorizedCost',
               'ACTUAL_EXP_UNADJ': 'ActualCost',
               'CAST_SENS_PROS': 'CastrationSensitiveProstate',
               'LOW_RISK_BLAD': 'LowRiskBladderFlag',
               'BASELINE_PRICE': 'BaselinePrice',
               'EXPERIENCE_ADJ' : 'ExperienceAdjuster',
               'NCH_CLM_TYPE_CD': 'ClaimType',
               'FI_CLM_PROC_DT': 'ClaimProcessingDate',
               'PRVDR_NUM': 'CCN',
               'CLM_FAC_TYPE_CD': 'FacilityType',
               'CLM_MDCR_NON_PMT_RSN_CD': 'NonPayReasonCode',
               'NCH_PRMRY_PYR_CLM_PD_AMT': 'PrimaryPayerPaid',
               'NCH_PRMRY_PYR_CD': 'PrimaryPayerCode',
               'ORG_NPI_NUM': 'OrgNPI',
               'AT_PHYSN_NPI': 'AttendingNPI',
               'PTNT_DSCHRG_STUS_CD': 'DischargeStatus',
               'CLM_TOT_CHRG_AMT' : 'Billed',
               'CLM_HHA_TOT_VISIT_CNT': 'VisitCount',
               'CLM_ADMSN_DT': 'AdmitDate',
               'CLM_MDCL_REC': 'MedicalRecord',
               'CLM_SRVC_CLSFCTN_TYPE_CD': 'ServiceType',
               'CLM_FREQ_CD': 'FreqCode',
               'CLM_STD_PYMT_AMT': 'StandardPaid',
               'CLM_HOSPC_START_DT_ID': 'HospiceStartDateID',
               'BENE_HOSPC_PRD_CNT': 'PatientHospicePeriodCount',
               'NCH_BENE_DSCHRG_DT': 'DischargeDate',
               'PRVDR_STATE_CD': 'ProviderState',
               'OP_PHYSN_NPI': 'SurgeonNPI',
               'CLM_MCO_PD_SW': 'MCOPaidSwitch',
               'CLM_PPS_IND_CD': 'PPSIndicator',
               'CLM_IP_ADMSN_TYPE_CD': 'AdmitType',
               'CLM_SRC_IP_ADMSN_CD': 'AdmitSource',
               'CLM_PASS_THRU_PER_DIEM_AMT': 'PerDiem',
               'NCH_BENE_IP_DDCTBL_AMT': 'Deductible',
               'NCH_BENE_PTA_COINSRNC_LBLTY_AM': 'Coinsurance',
               'NCH_BENE_BLOOD_DDCTBL_LBLTY_AM': 'BloodDeductible',
               'CLM_TOT_PPS_CPTL_AMT': 'TotalCapital',
               'CLM_PPS_CPTL_OUTLIER_AMT': 'OutlierCapital',
               'CLM_PPS_CPTL_DSPRPRTNT_SHR_AMT': 'CapitalDSHAmount',
               'CLM_PPS_CPTL_IME_AMT': 'CapitalIMEAmount',
               'CLM_UTLZTN_DAY_CNT': 'LOS',
               'CLM_DRG_CD': 'DRG',
               'CLM_DRG_OUTLIER_STAY_CD': 'OutlierFlag',
               'NCH_DRG_OUTLIER_APRVD_PMT_AMT': 'OutlierPaid',
               'ADMTG_DGNS_CD': 'AdmitDx',
               'IME_OP_CLM_VAL_AMT': 'IMEPaid',
               'DSH_OP_CLM_VAL_AMT': 'DSHPaid',
               'CLM_LINE_NUM': 'LineNum',
               'REV_CNTR': 'RevCode',
               'REV_CNTR_UNIT_CNT': 'Units',
               'REV_CNTR_RATE_AMT': 'RateAmount',
               'REV_CNTR_NDC_QTY': 'NDCQuantity',
               'REV_CNTR_NDC_QTY_QLFR_CD': 'NDCQtyQualifierCode',
               'REV_CNTR_TOT_CHRG_AMT': 'LineBilled',
               'REV_CNTR_NCVRD_CHRG_AMT': 'LineNonCovered',
               'PDE_ID': 'ClaimNum',
               'DRUG_CVRG_STUS_CD': 'RxCoverageStatus',
               'CTSTRPHC_CVRG_CD': 'RxCatastrophicCoverageCode',
               'PROD_SRVC_ID': 'NDC',
               'PRSCRBR_ID': 'Prescriber',
               'SRVC_DT': 'ServiceDate',
               'FILL_NUM': 'FillNum',
               'QTY_DSPNSD_NUM': 'Quantity',
               'DAYS_SUPLY_NUM': 'DaysSupply',
               'GDC_BLW_OOPT_AMT': 'CostBelowCatastrophic',
               'GDC_ABV_OOPT_AMT': 'CostAboveCatastrophic',
               'LICS_AMT': 'LICSPaid',
               'TOT_RX_CST_AMT': 'TotalClaimPaid',
               'PRF_PHYSN_NPI': 'NPI',
               'CARR_LINE_PRCNG_LCLTY_CD': 'PricingLocality',
               'LINE_PRVDR_PMT_AMT': 'LineProviderPaid',
               'CARR_LINE_MTUS_CNT': 'LineMTUSCount',
               'CARR_LINE_MTUS_CD': 'LineMTUSCode',
               'REV_CNTR_DT': 'RevCodeDate',
               'REV_CNTR_APC_HIPPS_CD': 'HIPPSCode',
               'REV_CNTR_PMT_MTHD_IND_CD': 'PaymentMethod',
               'REV_CNTR_IDE_NDC_UPC_NUM': 'NDC',
               'REV_CNTR_PTNT_RSPNSBLTY_PMT': 'LinePatientPaid',
               'REV_CNTR_PMT_AMT_AMT': 'LinePaid',
               'clm_rev_std_pymt_amt': 'LineStandardPay',
               'CLM_REV_STD_PYMT_AMT': 'LineStandardPay'}
    df.rename(columns = dictRen, inplace=True)
    dictOCMQuarterly = dict(QTR_START_DATE='QuarterStartDate', EM_VISITS='EMVisitsYou',
                            EM_VISITS_ALL='EMVisitsAllProviders', CHEMO_DATE='ChemoStartDate',
                            RISK_SCORE='HCCRiskScore', HIGH_RISK='HighRiskFlag', COMMON_CANCER_TYPE='CommonCancerType',
                            GENDER='Sex', AGE_CATEGORY='AgeGroup', RACE='Race', DUAL='DualEligible',
                            ALL_TOS='TotalPaid', INP_ADMSNS='InpatPaid', INP_EX='InpatPaidExcludingCancerCare',
                            INP_AMB='InpatPaidACSC', UNPLANNED_READ='InpatPaidUnplannedReadmits',
                            ER_OBS_AD='InpatPaidWithObsAndER', ER_AD='InpatPaidERAdmits',
                            OBS_AD='InpatPaidObsStayAdmits', ER_AND_OBS_AD='InpatPaidBothObsAndER',
                            NO_ER_NO_OBS_AD='InpatPaidWOObsOrER', OBS_STAYS='ObsPaid', OBS_ER='ObsPaidWithER',
                            OBS_NO_ER='ObsPaidNoER', ER_NO_AD_OBS='ERPaidNoObs', R_ONC='RadOncPaid',
                            PHY_SRVC='PhysicianPaid', PHY_ONC='PhysicianOncologyPaid', PHY_OTH='PhysicianOtherPaid',
                            ANC_TOT='TestingPaid', ANC_LAB_TOT='TestingLabPaid', ANC_LAB_ADV='TestingAdvancedLabPaid',
                            ANC_LAB_OTHER='TestingOtherLabPaid', ANC_IMAG_TOT='TestingImagPaid',
                            ANC_IMAG_ADV='TestingAdvancedImagPaid', ANC_IMAG_OTH='TestingOtherImagPaid',
                            OUT_OTHER='OPOtherPaid', HHA='HHAPaid', SNF='SNFPaid', LTC='LTCHPaid', IRF='IRFPaid',
                            HSP_TOT='HospicePaid', HSP_FAC='HospiceFacilityPaid', HSP_HOME='HospiceHomePaid',
                            HSP_BOTH='HospiceHomeAndFacilityPaid', DME_NO_DRUGS='DMEPaid', PD_TOT='DrugsPaid',
                            PD_PTB_PHYDME='DrugsPartBOfficePaid', PD_PTB_OUT='DrugsOPPaid', PD_PTD_ALL='DrugsPartDPaid',
                            OTHER='OtherPaid', ALL_TOS_ADJ='TotalPaidAdj', INP_ADMSNS_ADJ='InpatPaidAdj',
                            INP_EX_ADJ='InpatPaidExcludingCancerCareAdj', INP_AMB_ADJ='InpatPaidACSCAdj',
                            UNPLANNED_READ_ADJ='InpatPaidUnplannedReadmitsAdj',
                            ER_OBS_AD_ADJ='InpatPaidWithObsAndERAdj', ER_AD_ADJ='InpatPaidWithERAdj',
                            OBS_AD_ADJ='InpatPaidWithObsAdj', ER_AND_OBS_AD_ADJ='InpatPaidWithERAndObsAdj',
                            NO_ER_NO_OBS_AD_ADJ='InpatPaidWOObsOrERAdj', OBS_STAYS_ADJ='ObsPaidAdj',
                            OBS_ER_ADJ='ObsPaidWithERAdj', OBS_NO_ER_ADJ='ObsPaidWOERAdj', ER_NO_AD_OBS_ADJ='ERPaidAdj',
                            R_ONC_ADJ='RadOncPaidAdj', PHY_SRVC_ADJ='PhysicianPaidAdj',
                            PHY_ONC_ADJ='PhysicianOncologistPaidAdj', PHY_OTH_ADJ='PhysicianOtherPaidAdj',
                            ANC_TOT_ADJ='TestingPaidAdj', ANC_LAB_TOT_ADJ='LabPaidAdj',
                            ANC_LAB_ADV_ADJ='LabAdvancedPaidAdj', ANC_LAB_OTHER_ADJ='LabOtherPaidAdj',
                            ANC_IMAG_TOT_ADJ='ImagPaidAdj', ANC_IMAG_ADV_ADJ='ImagAdvancedPaidAdj',
                            ANC_IMAG_OTH_ADJ='ImagOtherPaidAdj', OUT_OTHER_ADJ='OPOtherPaidAdj', HHA_ADJ='HHAPaidAdj',
                            SNF_ADJ='SNFPaidAdj', LTC_ADJ='LTCHPaidAdj', IRF_ADJ='IRFPaidAdj',
                            HSP_TOT_ADJ='HospicePaidAdj', HSP_FAC_ADJ='HospiceFacilityPaidAdj',
                            HSP_HOME_ADJ='HospiceHomePaidAdj', HSP_BOTH_ADJ='HospiceHomeAndFacilityPaidAdj',
                            DME_NO_DRUGS_ADJ='DMEPaidAdj', PD_TOT_ADJ='DrugsPaidAdj',
                            PD_PTB_PHYDME_ADJ='DrugsPaidPartBAdj', PD_PTB_OUT_ADJ='DrugsPaidOPAdj',
                            PD_PTD_ALL_ADJ='DrugsPaidPartDAdj', OTHER_ADJ='OtherPaidAdj',
                            RISK_ADJ_FACTOR='RiskAdjustmentFactor', INFLATION_FACTOR='InflationFactor',
                            INP_ADMSNS_UTIL='InpatAdmits', INP_EX_UTIL='InpatAdmitsExcludingCancerCare',
                            INP_AMB_UTIL='InpatAdmitsACSC', UNPLANNED_READ_UTIL='InpatUnplannedReadmits',
                            ER_OBS_AD_UTIL='InpatAdmitsFromERAndObs', ER_AD_UTIL='InpatAdmitsFromER',
                            OBS_AD_UTIL='InpatAdmitsFromObs', ER_AND_OBS_AD_UTIL='InpatAdmitsWithObsAndER',
                            NO_ER_NO_OBS_AD_UTIL='InpatAdmitsWithoutObsOrER', OBS_STAYS_UTIL='ObsStays',
                            OBS_ER_UTIL='ObsStaysWithER', OBS_NO_ER_UTIL='ObsStaysWithoutER',
                            ER_NO_AD_OBS_UTIL='ERVisits', R_ONC_UTIL='RadOncServices', PHY_SRVC_UTIL='PhysServices',
                            PHY_ONC_UTIL='PhysOncologistServices', PHY_OTH_UTIL='PhysOtherServices',
                            ANC_LAB_TOT_UTIL='LabServices', ANC_LAB_ADV_UTIL='LabAdvancedServices',
                            ANC_LAB_OTHER_UTIL='LabOtherServices', ANC_IMAG_TOT_UTIL='ImagServices',
                            ANC_IMAG_ADV_UTIL='ImagAdvancedServices', ANC_IMAG_OTH_UTIL='ImagOtherServices',
                            HHA_UTIL='HHAVisits', SNF_UTIL='SNFDays', LTC_UTIL='LTCHUtil', IRF_UTIL='IRFUtil',
                            HSP_UTIL='HospiceUtil', DEATH='DeathDate', DIED='DiedFlag',
                            HSP_30DAYS_ALL='HospiceCareLast30Days', ANY_HSP_CARE='HospiceCareFlag',
                            HSP_DAYS='HospiceDays', HOSPITAL_USE='InpatLast14DaysOfLife',
                            INTENSIVE_CARE_UNIT='ICULast14DaysOfLife', CHEMOTHERAPY='ChemoLast14DaysOfLife',
                            bene_id='BeneSK', clm_id='ClaimNum')
    # print(dictOCMQuarterly)
    df.rename(columns = dictOCMQuarterly, inplace=True)
    dictICD={}
    for c in range(26):
        key = 'ICD_DGNS_CD' + str(c)
        value = 'Dx' + str(c)
        dictICD[key] = value
        key = 'ICD_DGNS_VRSN_CD' + str(c)
        value = 'DxVersion' + str(c)
        dictICD[key] = value
        key = 'ICD_PRCDR_CD' + str(c)
        value = 'Px' + str(c)
        dictICD[key] = value
        key = 'ICD_PRCDR_VRSN_CD' + str(c)
        value = 'PxVersion' + str(c)
        dictICD[key] = value
        key = 'PRCDR_DT' + str(c)
        value = 'Px' + str(c) + 'Date'
        dictICD[key] = value
    df.rename(columns=dictICD, inplace=True)

    listFix = []
    for c in df.columns.tolist():
        if (c==c.upper()) & (c not in ['CPT', 'NDC', 'BETOS', 'NPI', 'CCN', 'LOS', 'DRG']):
            listFix.append(c)
    if c == c.lower():
        listFix.append(c)
    if len(listFix)>0:
        print('Columns that are most likely not renamed')
        print(listFix)
        print()
    return df


def renameHumana(df):
    dictRen = {'OBM-DETAIL-TYPE': 'DetailType',
               'OBM-PLATFORM-CD': 'PlatformCode',
               'OBM-VENDOR-CD': 'VendorCode',
               'OBM-SUBSCRIBER-ID': 'Subscriber',
               'OBM-MEMBER-ID': 'Suffix',
               'OBM-REL-CD': 'RelationshipCode',
               'OBM-MBR-FIRST-NM': 'FirstName',
               'OBM-MBR-LAST-NM': 'LastName',
               'OBM-MBR-MI': 'MiddleInitial',
               'OBM-MBR-BIRTH-DATE': 'BirthDate',
               'OBM-SEX-CD': 'Sex',
               'OBM-MBR-CITY': 'MemberCity',
               'OBM-MBR-STATE-CD': 'MemberState',
               'OBM-MBR-COUNTY-CD': 'MemberCounty',
               'OBM-MBR-GROUP-NBR': 'MemberGroup',
               'OBM-PRODUCT-CD': 'MemberProduct',
               'OBM-LOB-CD': 'LineOfBusiness',
               'OBM-FUNDING-IND': 'FundingIndicator',
               'OBM-CLAIM-ID': 'ClaimID',
               'OBM-CLAIM-TYPE': 'ClaimType',
               'OBM-CLAIM-RECEIVED-DT': 'ClaimReceivedDate',
               'OBM-ICD-CD-SCHEME': 'ICDScheme',
               'OBM-SVC-NUMBER': 'ServiceNum',
               'OBM-SVC-STATUS': 'ServiceStatus',
               'OBM-CLAIM-STATUS': 'ClaimStatus',
               'OBM-AUTH-NUMBER': 'AuthNum',
               'OBM-PAID-DATE': 'PaidDate',
               'OBM-FROM-DOS': 'FromDate',
               'OBM-THRU-DOS': 'ThruDate',
               'OBM-SVC-UNITS': 'Units',
               'OBM-PRIMARY-DIAG-CD': 'Dx1',
               'OBM-DIAG-CD2': 'Dx2',
               'OBM-DIAG-CD3': 'Dx3',
               'OBM-DIAG-CD4': 'Dx4',
               'OBM-DIAG-CD5': 'Dx5',
               'OBM-DIAG-CD6': 'Dx6',
               'OBM-DIAG-CD7': 'Dx7',
               'OBM-DIAG-CD8': 'Dx8',
               'OBM-DIAG-CD9': 'Dx9',
               'OBM-DIAG-CD10': 'Dx10',
               'OBM-SVC-PROCD-CD': 'CPT',
               'OBM-SVC-PROCD-CD-MOD': 'CPTMod1',
               'OBM-SVC-PROCD-CD-MOD2': 'CPTMod2',
               'OBM-SVC-PROCD-CD-MOD3': 'CPTMod3',
               'OBM-SVC-PROCD-CD-MOD4': 'CPTMod4',
               'OBM-SVC-PROCD-CD-MOD5': 'CPTMod5',
               'OBM-REVENUE-CD': 'RevCode',
               'OBM-PLACE-OF-SVC': 'PlaceService',
               'OBM-TYPE-BILL-CD': 'BillType',
               'OBM-CHARGE-AMT': 'Billed',
               'OBM-ALLOWED-AMT': 'Allowed',
               'OBM-PAID-AMT': 'Paid',
               'OBM-DEDUCTIBLE-AMT': 'Deduct',
               'OBM-COPAY-AMT': 'Copay',
               'OBM-COINS-AMT': 'Coinsurance',
               'OBM-OTHER-CARR-PD-AMT': 'OtherCarrierPaid',
               'OBM-SVC-PROV-ID': 'ProviderID',
               'OBM-SVC-PROV-FIRST-NM': 'ProvFirstName',
               'OBM-SVC-PROV-LAST-NM': 'ProvLastName',
               'OBM-SVC-PROV-ADDR1': 'ProvAddress1',
               'OBM-SVC-PROV-ADDR2': 'ProvAddress2',
               'OBM-SVC-PROV-CITY': 'ProvCity',
               'OBM-SVC-PROV-STATE': 'ProvState',
               'OBM-SVC-PROV-ZIP': 'ProvZip',
               'OBM-SVC-PROV-PHONE': 'ProvPhone',
               'OBM-SVC-PROV-TIN': 'TaxID',
               'OBM-SVC-PROV-TYPE-CD': 'ProvTypeCode',
               'OBM-SVC-PROV-SPCLTY-CD': 'ProvSpec',
               'OBM-SVC-PROV-STATUS': 'ProvStatus',
               'OBM-RFR-PROV-ID': 'ReferringProvID',
               'OBM-RFR-PROV-FIRST-NM': 'RefProvFirstName',
               'OBM-RFR-PROV-LAST-NM': 'RefProvLastName',
               'OBM-PRIM-INS-IND': 'PrimaryInsuranceID',
               'OBM-PAID-SVC-UNITS-CNT': 'PaidUnitsCount',
               'OBM-HUM-ACTION-CD': 'ActionCode',
               'OBM-HUM-ACTION-DATE': 'ActionDate',
               'OBM-VNDR-RECOMD-CD1': 'VendorRecommendCode1',
               'OBM-VNDR-RECOMD-CD2': 'VendorRecommendCode2',
               'OBM-VNDR-RECOMD-CD3': 'VendorRecommendCode3',
               'OBM-VNDR-RECOMD-CD4': 'VendorRecommendCode4',
               'OBM-DETAIL-FILLER': 'Filler2'}
    df.rename(columns=dictRen, inplace=True)
    df['MemberName'] = (df.LastName.apply(lambda x: x.strip().title()) + ', ' +
                        df.FirstName.apply(lambda x: x.strip().title()))
    df['ProviderName'] = (df.ProvLastName.apply(lambda x: x.strip().title()) + ', ' +
                          df.ProvFirstName.apply(lambda x: x.strip().title()))
    for c in ['LastName', 'FirstName', 'ProvLastName', 'ProvFirstName']:
        del df[c]
    return df


def getFN(keyString):
    '''
    This function gets the name of the first file in the directory that contains the keyString value.

    :param keyString: the string to search for in the file name.  This keyString will usually be a part of the name that
                      uniquely identifies a single file in the directory.
    :return: the first file containing the keyString.
    '''
    listFiles = [fn for fn in os.listdir('./') if keyString in fn]
    keyFile = listFiles[0]
    return keyFile


def fewSpreadsheets(DF, workbook, sheet, title, notes, WSA,
                    ColumnNames, ColumnGroups, DataSource, images=[],
                    minColWidth=9, headerLines=2):
    df = DF.copy()
    # Create worksheet
    ws = workbook.add_worksheet(sheet)
    ws.freeze_panes(7,0)
    #ws = workbook.add_worksheet(sheet)
    header1 = '&L&G'
    ws.set_header(header1, {'image_left': 'c:/AdvAnalytics/Reference/logo.png'})
    # Define display formats
    fmtBold = workbook.add_format({'bold' : True})
    fmtTextWrap = workbook.add_format({'text_wrap' : True})
    fmtTextShrink = workbook.add_format({'align': 'left'})
    fmtTextShrink.set_shrink(True)
    fmtRowHeading = workbook.add_format({'bold' : True})
    fmtRowHeading.set_font_name('Verdana')
    fmtColGroupHeader = workbook.add_format({'bold' : True,
                                             'align' : 'center', 'text_wrap' : True})
    fmtColGroupHeader.set_bottom(1)
    fmtColGroupHeader.set_font_size(13)
    fmtColHeader = workbook.add_format({'bold' : True, 'underline' : 34,
                                        'align' : 'center', 'text_wrap' : True})
    formatComma0 =workbook.add_format({'align' : 'right'})
    formatComma0.set_num_format('#,##0')#;[Red](#,##0')
    formatComma1 =workbook.add_format({'align' : 'right'})
    formatComma1.set_num_format('#,##0.?')#_;[Red](#,##0.0')
    formatComma2 =workbook.add_format({'align' : 'right'})
    formatComma2.set_num_format('#,##0.0?')#_;[Red](#,##0.00')
    formatCurr0 =workbook.add_format({'align' : 'right'})
    formatCurr0.set_num_format('$#,##0;[Red]($#,##0)')
    formatCurr1 =workbook.add_format({'align' : 'right'})
    formatCurr1.set_num_format('$#,##0.0_;[Red]($#,##0.0)')
    formatCurr2 =workbook.add_format({'align' : 'right'})
    formatCurr2.set_num_format('$#,##0.00_;[Red]($#,##0.00)')
    formatPct0 =workbook.add_format({'align' : 'right'})
    formatPct0.set_num_format('#,##0%;[Red](#,##0%)')
    formatPct1 =workbook.add_format({'align' : 'right'})
    formatPct1.set_num_format('#,##0.0%;[Red](#,##0.0%)')
    formatPct2 =workbook.add_format({'align' : 'right'})
    formatPct2.set_num_format('#,##0.00%;[Red](#,##0.00%)')
    formatDate =workbook.add_format({'align' : 'right'})
    formatDate.set_num_format('mmm dd, yyyy')
    formatText =workbook.add_format({'align' : 'right'})
    formatTextIndent = workbook.add_format()
    formatTextIndent.set_indent(1)

    # Get column list
    listCols = list(df.columns)
    # Add whitespace columns to column list
    colNum = 1
    for c in WSA:
        colPos = listCols.index(c)
        colname = 'Whitespace_' + str(colNum)
        df[colname] = ' '
        listCols.insert(colPos+1, colname)
        colNum += 1
    df = df[listCols]
    listCols = list(df.columns)
    # Cycle through columns
    xlscol = 0
    dictWidth = {}
    dictFormat={}
    for c in listCols:
        if c.find('Whitespace_')!=0:
            ct = df[c].dtype
            ct = str(ct)
            ct = ct[:3]
        else:
            ct = 'obj'
        # Determine column width and best format for column
        if c.find('Whitespace_')==0:
            width = 3
            cform = 'formatComma0'
        elif ct=='obj':
            #get column length
            try:
                df[c] = df[c].apply(lambda x : str(x))
            except:
                pass
            try:
                df['ColumnLength'] = df[c].apply(lambda x : len(x))
                width = int(1.1 * df['ColumnLength'].max())
            except:
                df['ColumnLength'] = 9
                width=minColWidth
            del df['ColumnLength']
            cform = 'fmtTextShrink'
        elif ct=='int':
            df['__X__'] = df[c].apply(lambda x : '{:,.0f}'.format(x))
            df['ColumnLength'] = df['__X__'].apply(lambda x : len(x))
            df.ColumnLength.fillna(1, inplace=True)
            try:
                width = int(1.1 * df['ColumnLength'].max())
            except:
                width = minColWidth
            if width>12:
                width=12
            cform = 'formatComma0'
            del df['ColumnLength']
            del df['__X__']
        elif ct=='flo':
            width=12
            df['__X__'] = df[c].apply(lambda x: '{:,.2f}'.format(x))
            df['ColumnLength'] = df['__X__'].apply(lambda x : len(x))
            try:
                width = int(1.1 * df['ColumnLength'].max())
            except:
                width = minColWidth
            del df['ColumnLength']
            del df['__X__']
            if ( (c.find('Cost')>=0) | (c.find('cost')>=0) | (c.find('cst')>=0) |
                 (c.find('Paid')>=0) | (c.find('paid')>=0) | (c.find('Pay')>=0) |
                 (c.find('pay') >=0) | (c.find('Allow')>=0)| (c.find('allowed')>=0) |
                 (c.find('Billed')>=0)|(c.find('billed')>=0) | (c.find('$')>=0) |
                 (c.find('Price')>=0)|(c.find('NPRA')>=0) ):
                cform = 'formatCurr0'
            elif ( (c.find('Percent')>=0) | (c.find('percent')>=0) | (c.find('Pct')>=0) |
                   (c.find('pct') >=0) | (c.find('%') >=0) ):
                cform = 'formatPct1'
            elif c.find('Per')>=0:
                cform = 'formatComma1'
            else:
                cform = 'formatComma2'
        elif ct=='dat':
            width=17
            cform = 'formatDate'
        else:
            df['__X__'] = df[c].apply(lambda x : str(x))
            df['__X__'] = df['__X__'].apply(lambda x: '{:,.2f}'.format(x))
            df['ColumnLength'] = df['__X__'].apply(lambda x : len(x))
            try:
                width = df['ColumnLength'].max()
            except:
                width = 9
            del df['__X__']
            del df['ColumnLength']
            cform = 'formatComma2'
        if width<minColWidth:
            width=minColWidth
        if (c.find('Whitespace_')!=0):
            wl = max(len(w) for w in c.split())
            if (wl>4) & (wl>width):
                width=min(wl,12)
            if width<5:
                width=5
        if c.find('Whitespace_')==0:
            width = 3
            cform = 'formatComma0'
        dictWidth[c] = width
        dictFormat[c]= cform
    # "set_column"
    xlscol=0
    for c in listCols:
        fmt = dictFormat.get(c)
        if fmt=='formatComma0':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatComma0)
        elif fmt=='fmtTextShrink':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), fmtTextShrink)
        elif fmt=='fmtBold':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), fmtBold)
        elif fmt=='fmtTextWrap':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), fmtTextWrap)
        elif fmt=='fmtTextShrink':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), fmtTextShrink)
        elif fmt=='formatComma0':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatComma0)
        elif fmt=='formatComma1':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatComma1)
        elif fmt=='formatComma2':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatComma2)
        elif fmt=='formatCurr0':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatCurr0)
        elif fmt=='formatCurr1':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatCurr1)
        elif fmt=='formatCurr2':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatCurr2)
        elif fmt=='formatPct0':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatPct0)
        elif fmt=='formatPct1':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatPct1)
        elif fmt=='formatPct2':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatPct2)
        elif fmt=='formatDate':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatDate)
        elif fmt=='formatText':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatText)
        elif fmt=='formatTendIndent':
            ws.set_column(xlscol, xlscol, dictWidth.get(c), formatTextIndent)
        xlscol += 1
    # Write report header (title, date and time)
    fmtRptTitle = workbook.add_format({'bold' : True})
    fmtRptTitle.set_font_size(16)
    fmtRptSubtitle = workbook.add_format({'align' : 'left',
                                          'italic' : True})
    fmtRptSubtitle.set_font_size(13)
    ws.write_string('A1', title)
    ws.write_string('A2', ctime())
    ws.write_string('A3', 'Data Source: ' + DataSource)
    ws.set_row(0, 28, fmtRptTitle)
    ws.set_row(1, 21, fmtRptSubtitle)
    ws.set_row(2, 21, fmtRptSubtitle)
    ws.set_row(6, 14*headerLines, fmtRptSubtitle)
    listCols = list(df.columns)
    # If 'notes' is not null, create a hyperlink to the notes that appear at the bottom
    #    of the sheet
    # Add the standard url link format.
    url_format = workbook.add_format({
        'font_color': 'blue',
        'underline':  1
    })
    if len(notes)>1:
        notesStart = 'A' + str(len(df.index) + 11)
        locNotes = 'internal:' + sheet + '!' + notesStart
        ws.write_url('A4', locNotes, url_format, 'Report Notes')
        ws.write_string(len(df.index)+9, 0, 'Report Notes', fmtBold)
    # Write group header (column sets)
    for grp in ColumnGroups:
        cs = grp[0]
        ce = grp[1]
        ct = grp[2]
        startPos = listCols.index(cs)
        endPos   = listCols.index(ce)
        ws.merge_range(5,startPos,5,endPos,ct,fmtColGroupHeader)
    # Write column header, replacing column names with a dictionary passed to the program.
    #   If the column name is not in the dictionary, use the column name
    colnum = 0
    for c in listCols:
        if c[:11]=='Whitespace_':
            vvvv=0
        else:
            if c in list(ColumnNames.keys()):
                CN = ColumnNames.get(c)
            else:
                CN = c
            ws.write_string(6,colnum, CN, fmtColHeader)
        colnum += 1
    # Write data
    numRows = len(df.index)
    for r in range(numRows):
        rn = r + 7
        cn = 0
        ws.set_row(rn, 21)
        for c in listCols:
            cdata = df.at[r,c]
            if cdata is None:
                cdata = ''
            elif cdata==pd.to_datetime('1/1/1970'):
                cdata = ''
            ws.write(rn, cn, cdata)
            cn+=1
    # Add notes to bottom of the spreadsheet
    nr = len(df.index) + 10
    for note in notes:
        ws.write_string(nr, 0, note, formatTextIndent)
        nr += 1
    ws.hide_gridlines(2)
    r = nr + 2
    c = 0
    for img in images:
        ws.insert_image(r,c,img)
        if c==0:
            c+=8
        else:
            c=0
            r+=20
    print(sheet + ' written at ' + ctime())

def toMySQL(df, schema, table):
    print('  Starting writing table ' + schema + '.' + table + ' at ' + ctime())
    engine = create_engine('mysql+pymysql://ebassin:117Sutton@localhost:3306/' + schema)
    df.to_sql(name=table, con=engine, if_exists='replace', index=False)
    print('  Finished writing table ' + schema + '.' + table + ' at ' + ctime())
    return
