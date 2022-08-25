
views_list = [ "DEMO_DB.RAW.VW_BP_SUMMARY_ORDERS_CLAIMS_STATUSES",
               "DEMO_DB.RAW.VW_MEDICARE_CLAIMS_STATUSES_ALL",
               "DEMO_DB.RAW.VW_ALLINSURANCE_CLAIMS_STATUSES_ALL",
               "DEMO_DB.RAW.VW_ALLCLAIMS_VISITINFO",
               "DEMO_DB.PUBLIC.VW_LAST_LOAD_TIME",
               "DEMO_DB.RAW.VW_EPISODIC_BP_LUPA_RISK",
               "DEMO_DB.RAW.VW_BLANK_LUPA",
               "DEMO_DB.RAW.VW_INCORRECT_MRNS",
               "DEMO_DB.RAW.VW_DISCHARGE_PLANNING",
               "DEMO_DB.RAW.VW_SCHEDULE_DEVIATION",
               "DEMO_DB.RAW.VW_BP_VISIT_ALLOWABLE_SUMMARY",
               "DEMO_DB.RAW.VW_PRODUCTIVITY_DETAILS",
               "DEMO_DB.RAW.VW_PATIENT_EPISODE_MISSING_AUTH_VISITS",
               "DEMO_DB.RAW.VW_PATIENT_EPISODE_MISSING_AUTH_SUMMARY",
               "DEMO_DB.RAW.HNTS_ADJUSTED_HIPPS_RATES",
               "DEMO_DB.RAW.VW_BLANK_LUPA_TEST",
               "DEMO_DB.RAW.VW_SCHEDULEREPORTS_VISITSBYSTATUS",
               "DEMO_DB.RAW.VW_VISITSBYSTATUS_TASK_CATEGORY",
               "DEMO_DB.RAW.VW_PATIENT_ROSTER",
               "DEMO_DB.RAW.MEDICARE_MCRADV_VISITPLANNING_NOAUTH",
               "DEMO_DB.RAW.VW_MEDICARE_MCRADV_VISITPLANNING_CURRENT_EPISODES",
               "DEMO_DB.RAW.VW_PATIENT_ROSTER_CA",
               "DEMO_DB.RAW.VW_MONTHLY_CENSUS_BY_PAYOR_CATEGORY",
               "DEMO_DB.RAW.VW_PCC_DASHBOARD",
               "DEMO_DB.RAW.VW_ORDERS_COMMENTS_FINANCES_FULL_SUMMARY_2",
               "DEMO_DB.RAW.VW_BP_FINANCE_SUMMARY_SIMPLE",
               "DEMO_DB.RAW.VW_ORDERS_CORRECT_EPISODE_ID",
               "DEMO_DB.RAW.VW_CLAIMS_STATUSES_WITH_EXPECTED_REVENUE",
               "DEMO_DB.USER_INPUTS.ACTIVE_USERS",
               "DEMO_DB.USER_INPUTS.COMMENT_CATEGORIES"]


#TODO:  Go get the column names from the viewlist
#TODO:  construct_sql
#TODO:  Naga needs to sleep

'''

replicate the scripts
use database demo_db;
use schema raw;

create or replace function py_udf_simple_mask(role varchar, database_name varchar, schema_name varchar, view_name varchar, column_name varchar, column_value varchar)
returns string
language python
runtime_version = 3.8
handler = 'simple_mask'
as $$

import hashlib

def simple_mask(role,database_name, schema_name, view_name, column_name,column_value):
    if role == 'SYSADMIN':
        return column_value
    elif (database_name == 'DEMO_DB' and schema_name == 'RAW' and view_name == 'VW_PCC_DASHBOARD' and column_name == 'MRN'):
        if column_value:
            return (hashlib.md5(column_value.encode())).hexdigest() 
    elif (database_name == 'DEMO_DB' and schema_name == 'RAW' and view_name == 'VW_PCC_DASHBOARD' and column_name == 'PATIENT'):
        if column_value:
            return 'XXXX YYYY ZZZZ'
    else:
        return column_value

$$;

-- test with a simple string 
select py_udf_simple_mask('ANALYST','DEMO_DB', 'RAW', 'VW_PCC_DASHBOARD', 'PATIENT', 'Renga');

create or replace view VW_PCC_DASHBOARD(
	MRN, 
	INSURANCE_CODE,
	PATIENT_STATUS,
	PATIENT,
	INSURANCE_NAME,
	CASE_MANAGER,
	EPISODE_START_DATE,
	EPISODE_END_DATE,
	EPISODE_STATUS,
	PRIMARY_INSURANCE_PROVIDER,
	INSURANCE_ID,
	SECONDARY_INSURANCE_PROVIDER,
	MEDICAID_NUMBER,
	EPISODE_FRONT_LOADED_VISITS,
	EPISODE_FRONT_LOADED_MISSED_VISITS,
	HOSPITAL_HOLD,
	RE_ADMISSION,
	DIAGNOSIS,
	DIAGNOSIS_CODE,
	FRONT_LOADED,
	LUPA_RISK,
	LUPA_RISK_30_DAY_EPISODES,
	TOTAL_MARGIN,
	"1-60 days Disc: T/S/C/M",
	EXPECTED_REVENUE,
	COMPLETED_REVENUE,
	AUTH_VISIT_COUNT,
	UTILIZED_AUTH_VISITS,
	MISSING_AUTH,
	ORDERS_IN_PLACE
) as
select 
	py_udf_simple_mask(current_role(),'DEMO_DB', 'RAW', 'VW_PCC_DASHBOARD', 'MRN', MRN),
	INSURANCE_CODE,
	PATIENT_STATUS,
	py_udf_simple_mask(current_role(),'DEMO_DB', 'RAW', 'VW_PCC_DASHBOARD', 'PATIENT', PATIENT),
	INSURANCE_NAME,
	CASE_MANAGER,
	EPISODE_START_DATE,
	EPISODE_END_DATE,
	EPISODE_STATUS,
	PRIMARY_INSURANCE_PROVIDER,
	INSURANCE_ID,
	SECONDARY_INSURANCE_PROVIDER,
	MEDICAID_NUMBER,
	EPISODE_FRONT_LOADED_VISITS,
	EPISODE_FRONT_LOADED_MISSED_VISITS,
	HOSPITAL_HOLD,
	RE_ADMISSION,
	DIAGNOSIS,
	DIAGNOSIS_CODE,
	FRONT_LOADED,
	LUPA_RISK,
	LUPA_RISK_30_DAY_EPISODES,
	TOTAL_MARGIN,
	"1-60 days Disc: T/S/C/M",
	EXPECTED_REVENUE,
	COMPLETED_REVENUE,
	AUTH_VISIT_COUNT,
	UTILIZED_AUTH_VISITS,
	MISSING_AUTH,
	ORDERS_IN_PLACE
from AXXESS_API.RAW.VW_PCC_DASHBOARD;

use role sysadmin;
select distinct mrn from VW_PCC_DASHBOARD;

use role analyst_readonly_role;
select distinct mrn from VW_PCC_DASHBOARD; -- it should return all unique mrn md5 hashed values 
select distinct patient from VW_PCC_DASHBOARD; -- it should return only XXXX YYYY ZZZZ




'''




for v in views_list:

    parts = v.split('.')
    # for p in parts:
    # print(p)
    mystr = f"""Create or replace view DEMO_DB.{parts[1]}.{parts[2]}\nas\nselect * from {v};"""
    print(mystr)

