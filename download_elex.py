from datetime import date
from calendar import Calendar
import os
import urllib
import zipfile
from time import sleep
import pandas as pd
import sqlalchemy as sa

database_user=''
database_pass=''
database_url=''
database_port=''
database_name=''

localfolder='E:/Users/Scott/workspace/EBR/prec_results/'

db_string="postgresql+psycopg2://"+database_user+":"+database_pass+"@"+database_url+":"+database_port+"/"+database_name

#azurepipeline=surf_azure(db_string)


month=11
day=0
years=[2010, 2012, 2014, 2016]

cal=Calendar(0)
folder_urls=[]
for year in years:
    weeks=cal.monthdays2calendar(year, month)
    # print weeks[0][1][0], year    https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2010_11_0/year2010110
    if weeks[0][0][0] != 0:
        election_day=weeks[0][1][0]
    else:
        election_day=weeks[1][1][0]
    if len(str(election_day)) == 1:
      election_day=str('0')+str(election_day)
    print(str(month)+'/'+str(election_day)+'/'+str(year))
    folder_urls.append({'path': 'https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/'+str(year)+'_'+str(month)+'_'+str(election_day)+'/', 'year': str(year)+str(month)+str(election_day)})
prec_results= []
voter_stats=[]
history_stats=[]
absentee=[]
provisional=[]
for x in folder_urls:
    print(x)
    prec_results.append({'url': x['path']+'results_sort_'+x['year']+'.zip', 'filename': 'results_sort_'+x['year']+'.zip', 'year': x['year'], 'filepath':localfolder+x['year']+'/'})
    

for election in prec_results:
    #Check for our download directory and create it if it doesn't exists
    if not os.path.exists(election['filepath']):
        os.makedirs(election['filepath'])
    #download the Zip
    urllib.request.urlretrieve(election['url'], election['filepath']+election['filename'])
    #Extract the Zip
    with zipfile.ZipFile(election['filepath']+election['filename'],"r") as zip_ref:
        zip_ref.extractall(election['filepath'])
                
    if '2010' in election['year'] or '2012' in election['year']:
        df=pd.read_csv(election['filepath']+'results_sort_'+election['year']+'.txt')
    else:
        df=pd.read_table(election['filepath']+'results_sort_'+election['year']+'.txt')
    print(df)
    #df.to_sql('results_'+election['year'], azurepipeline, if_exists='replace', schema='election_results')

translations=[["*COUNTY COMMISSIONER*" , "Commission"], ["*BOARD OF COMMISSIONERS*" , "Commission"], ["*CLERK OF SUPERIOR*" , "Clerk of Court"], ["CLERK OF COURT*" , "Clerk of Court"], [ "*DISTRICT COURT JUDGE*" , "District Judge"], ["*NC HOUSE OF REPRESENTATIVES DISTRICT*" , "NC House"], ["NC STATE HOUSE*" , "NC House"], [ "*STATE SENATE*" , "NC Senate"], ["*COUNTY REGISTER OF DEEDS" , "Register of Deeds"], [ "*BOARD OF EDUCATION*" , "Board of Education"], [ "BOARD OF EDUCATION*" , "Board of Education"], [ "US HOUSE OF REPRESENTATIVES*" , "US House"], ["US CONGRESS DISTRICT*" , "US House"], [ "PRESIDENT AND VICE PRESIDENT OF THE UNITED STATES" , "President"], [ "COMMISSIONER OF*" , "Council of State"], [ "NC COMMISSIONER OF*" , "Council of State"], ["*COUNTY SHERIFF" , "Sheriff"], ["SHERIFF" , "Sheriff"], ["US SENATE" , "US Senate"], ["GOVERNOR" , "Governor"], ["NC GOVERNOR" , "Governor"], ["*SOIL AND WATER*" , "Soil and Water"], ["*CITY COUNCIL*" , "City Council"], ["ATTORNEY GENERAL" , "Council of State"], ["AUDITOR" , "Council of State"], ["TREASURER" , "Council of State"], ["NC ATTORNEY GENERAL" , "Council of State"], ["NC AUDITOR" , "Council of State"], ["NC TREASURER" , "Council of State"], ["DISTRICT ATTORNEY*" , "District Attorney"], ["*DISTRICT ATTORNEY*" , "District Attorney"], ["MAYOR*" , "Mayor"], ["*MAYOR" , "Mayor"], ["*COUNTY COMMISSION*" , "Commission"], ["*SCHOOL BOARD*" , "Board of Education"], ["SCHOOL BOARD*" , "Board of Education"], ["CITY OF*" , "Municipal Special"], ["CORONER" , "Coroner"], ["*CORONER" , "Coroner"], ["COURT OF APPEALS JUDGE*" , "COA Judge"], ["*COURT OF APPEALS JUDGE*" , "COA Judge"], ["LIEUTENANT GOVERNOR" , "Council of State"], ["NC LIEUTENANT GOVERNOR" , "Council of State"], ["REGISTER OF DEEDS" , "Register of Deeds"], ["SECRETARY OF STATE" , "Council of State"], ["SUPERINTENDENT OF PUBLIC INSTRUCTION" , "Council of State"], ["NC SECRETARY OF STATE" , "Council of State"], ["NC SUPERINTENDENT OF PUBLIC INSTRUCTION" , "Council of State"], ["SUPERIOR COURT JUDGE*" , "Superior Court Judge"], ["*SUPERIOR COURT JUDGE*" , "Superior Court Judge"], ["TOWN OF*" , "Special Municipal"], ["STRAIGHT PARTY" , "Straight Party"], [ "NC SUPREME COURT*" , "Supreme Court"], [ "SUPREME COURT*" , "Supreme Court"], [ "US Senate" , "US Senate"]]


### Download Files
## Clean and standardize files
### Upload files
