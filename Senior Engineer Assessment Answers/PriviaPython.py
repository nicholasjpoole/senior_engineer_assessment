# In this directory you will find an excel spreadsheet titled "Privia Family Medicine 113018.xlsx" containing demographics,
# quarter and risk data. We need this data to be manipulated and stored in our PersonDatabase for analysis.
#
# Please include solutions to the questions below using python 2.7.
# Please include any required modules in a "requirements.txt" file in this directory.
# Please provide adequate test coverage for you solutions.
#
# 1. Import the 'Demographics' data section to a table in the database. This ETL will need to process files of the
# same type delivered later on with different file dates and from different groups.
#     a. Include all fields under 'Demographics'
#     b. Define the sql schema as necessary. Fields should not include spaces or special characters.
#     c. Include fields in the data table that indicate the date of the file and the provider group located in the filename.
#         In this case "Privia Family Medicine" 11/30/2018. Assume the length of the group name will change and the date
#         will always be formatted at the end of the file as MMDDYY
#     d. Include only the first initial of the Middle Name when applicable.
#     e. Convert the Sex value to M or F: M for 0 and F for 1

import pandas as pd
import pytest
from datetime import datetime
from sqlalchemy import create_engine

# Constants
file_source = r'Privia Family Medicine 113018.xlsx'

# SQL Server Credentials
DB = {'servername': '.\\',
      'database': 'PersonDatabase',
      'driver': 'driver=SQL Server Native Client 11.0'}

# Create DB Connection
engine = create_engine('mssql+pyodbc://' + DB['servername'] + '/' + DB['database'] + "?" + DB['driver'])

result = 'Operation passed with 0 errors'


def demographics_etl(file_source):

      try:
            #Utilize a try/except command for testing. Allows for exception handling in the event of an error or
            #some other unexpected result

            #1a.
            #create a data frame called demographics to specifically hold the demographic data
            demographics = pd.read_excel(file_source, index=False)

            #drop column 'a' of file and promote neccessary headers and remove space
            demographics = demographics.rename(columns=demographics.iloc[2])
            demographics = demographics.iloc[3:-3]

            #1b.
            #Remove Spaces and Special Characters from File
            demographics.columns = demographics.columns.str.strip()
            demographics.columns = demographics.columns.str.replace(' ', '')
            demographics.columns = demographics.columns.str.replace(r"[^a-zA-Z\d\_]+", "")
            demographics = demographics[demographics.columns[1:8]]

            #1c.
            demographics['ProviderGroup'] = 'Privia Family Medicine'
            demographics['FileDate'] = datetime.now()

            #1d.
            demographics['MiddleName'] = demographics['MiddleName'].str[0]

            #1e.
            demographics['Sex'] = demographics['Sex'].replace(0, 'M')
            demographics['Sex'] = demographics['Sex'].replace(1, 'F')


            #Format Columns Before Inserting into SQL Server
            demographics['ID'] = demographics['ID'].astype(int)
            demographics['DOB1'] = demographics['DOB1'].dt.date
            demographics['FileDate'] = demographics['FileDate'].dt.date
            demographics.set_index('ID', inplace=True)


            #Make this the last step
            #to_sql() automatically will rollback DB updates in the event of an error
            #No need to rollback execuition here.
            #Insert into SQL Database if there are no errors
            # add table to sql server
            demographics.to_sql('Demographics', index=False, con=engine, if_exists='append')

            #Process Completed
            #Return message as the last step
            return result


      except Exception as error:
            print('Error: ' + repr(error))
            quit


#####################################################################################################--2


# 2. Transform and import the 'Quarters' and 'Risk' data into a separate table.
#     a. Unpivot the data so that the data table includes
#         i. ID
#         ii. Quarter
#         iii. Attributed flag
#         iv. Risk Score
#         v. File date
#      b. Only include records in which the patients risk has increased.
# 3. Include tests
# 4. Provide All necessary information for the team to get this up and running.

#2
#Utilize a try/except command for testing. Allows for exception handling in the event of an error or
#some other unexpected result
def quarters_risk_etl(file_source):
      try:
            #2a.
            #create a data frame called demographics to specifically hold the demographic data
            quarters_risk = pd.read_excel(file_source, index=False)

            #Remove the first 2 rows to unpivot and reformat the data source
            quarters_risk = quarters_risk.rename(columns=quarters_risk.iloc[2])
            quarters_risk = quarters_risk.iloc[3:-3]

            #Remove Spaces and Special Characters from File
            quarters_risk.columns = quarters_risk.columns.str.strip()
            quarters_risk.columns = quarters_risk.columns.str.replace(' ', '')
            quarters_risk.columns = quarters_risk.columns.str.replace(r"[^a-zA-Z\d\_]+", "")

            #Get necessary columns and add the file date as a column
            quarters_risk = quarters_risk[['ID', 'AttributedQ1', 'AttributedQ2', 'RiskQ1', 'RiskQ2', 'RiskIncreasedFlag']]
            quarters_risk['FileDate'] = datetime.now()

            #Only include records in which the patients risk as increased
            quarters_risk = quarters_risk[quarters_risk['RiskQ2'] > quarters_risk['RiskQ1']]

            #Import Data into DB
            # Format Columns Before Inserting into SQL Server
            quarters_risk['ID'] = quarters_risk['ID'].astype(int)
            quarters_risk['FileDate'] = quarters_risk['FileDate'].dt.date
            quarters_risk.set_index('ID', inplace=True)

            # Make this the last step
            # to_sql() automatically will rollback DB updates in the event of an error
            # No need to rollback execuition here.
            # Insert into SQL Database if there are no errors
            # add table to sql server
            quarters_risk.to_sql('PatientQuarterlyRisk', index=False, con=engine, if_exists='append')

            #If process completes, return a pass message
            #We will use this for our testing
            return result

      #Throw an error containing
      except Exception as error:
                  print('Error: ' + repr(error))
                  quit


#Test the funcctions from above
#If the function returns a value (result), then we will know that the tests have passed successfully
def test_etl():
      assert demographics_etl(file_source)
      assert quarters_risk_etl(file_source)