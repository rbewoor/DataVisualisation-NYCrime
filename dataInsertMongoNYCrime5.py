# -*- coding: utf-8 -*-
"""
Created on Mon Dec 31 14:08:19 2018

@author: RB
"""
import re
import sys
import csv
import pandas as pd
from pymongo import MongoClient
import json
from datetime import datetime
import logging

# read the command line args and process
#numRowsSkip = specify the value as CLA
#numrowsRead = specify the value as CLA
global counters, mongoDbName, mongoCollName, nRowsSkip, nRowsRead, fileInputCsv, logFilename
counters =                                                                                                                              \
    {"countRowsReadFromCsvFile": 0, "countMongoWriteSuccess": 0, "countMongoWriteFailed": 0}
fileInputCsv = "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/NYPD_Complaint_Data_Historic-evenmoreTRUNCATED.csv"

### ----------------- FUNCTION DEFINITIONS START ------------- ###
def processCLAsAndSetup():
    global fileInputCsv, nRowsRead, nRowsSkip, mongoDbName, mongoCollName, logFilename

    print(f"#CLA passed = {len(sys.argv)}")
    
    for claNumber, ele in enumerate(sys.argv):
        print(f"Argument #{claNumber} = {ele}")
    
    if len(sys.argv) not in [6, 7]: # must have exactly 6 or 7 CLA (including program name):
        print(f'ERROR: Check CLA. Expected 6 or 7 args. Command should be as:\n\t\tpython <progNAme> <inputCSVfile> <#nRowsRead> <nRowsSkip> <mongoDbName> <mongoCollectionName> <logFilename>')
        print(f"Exiting program with return code = 100")
        exit(100)
    
    #number of parameters passed is fine, setup the arguments
    fileInputCsv = sys.argv[1]
    nRowsRead = int( sys.argv[2])
    nRowsSkip = int( sys.argv[3])
    mongoDbName = sys.argv[4]
    mongoCollName = sys.argv[5]
    # if the logFilename is passed as a CLA then use it else default format of the logfile name
    if len(sys.argv) == 7:
        logFilename = sys.argv[6]
    else:
        logFilename = 'LOG_' + sys.argv[0] + '.log'
    
    # all good, return with RC = 0
    return(0)
####
def readCsvFileIntoDataframe(inFileCsv, nRowsSkip, nRowsRead):
    '''
    Read the data from the csv file and info data frame.
    If the function is called with nRowsRead = -1 then it will read the WHOLE csv file.
    Else, specify nRrowsSkip and nRowsRead before calling and pass these during call.
    '''
    # https://honingds.com/blog/pandas-read_csv/
    # https://towardsdatascience.com/why-and-how-to-use-pandas-with-large-data-9594dda2ea4c
    # https://cmdlinetips.com/2018/01/how-to-load-a-massive-file-as-small-chunks-in-pandas
    
    print(f"\nReading CSV based on parameters\n\tnRowsRead = {nRowsRead}\tnRowsSkip = {nRowsSkip}")
    logging.warning(f"\nReading CSV based on parameters\n\tnRowsRead = {nRowsRead}\tnRowsSkip = {nRowsSkip}")

    # Make list of column names. Later will use list comprehension to drop unwanted columns.
    colNamesFound = list(pd.read_csv(inFileCsv, nrows =1, encoding='utf-8' ) )
    colNamesNotToBeLoaded = ['Lat_Lon']
    print(f"Found these columns:\n\t\t{colNamesFound}\n")
    logging.warning(f"Found these columns:\n\t\t{colNamesFound}\n")
    print(f"These columns will NOT be loaded:\n\t\t{colNamesNotToBeLoaded}\n")
    logging.warning(f"These columns will NOT be loaded:\n\t\t{colNamesNotToBeLoaded}\n")

    if nRowsRead == -1:
        # more basic version WITHOUT nrows and skiprows parameters
        print(f"Reading the ENTIRE CSV file into memory.")
        logging.warning(f"Reading the ENTIRE CSV file into memory.")
        df = pd.read_csv( inFileCsv, delimiter=',', quotechar='"',                                                              \
        encoding='utf-8', header=0, usecols =[col for col in colNamesFound if col not in colNamesNotToBeLoaded ] )
    else:
        # version which has the nrows and skiprows parameters being used
        print(f"Reading {nRowsRead} rows from CSV file into memory.")
        logging.warning(f"Reading {nRowsRead} rows from CSV file into memory.")
        df = pd.read_csv( inFileCsv, delimiter=',', nrows=nRowsRead, skiprows = range(1, nRowsSkip+1), quotechar='"',                       \
        encoding='utf-8', header=0, usecols =[col for col in colNamesFound if col not in colNamesNotToBeLoaded ] )

    #df = df.fillna('hadNaValue')
    
    return(df, 0)
####
def summaryPrint():
    print(f"#Count of rows read from input CSV = {counters['countRowsReadFromCsvFile']}")
    logging.warning(f"#Count of rows read from input CSV = {counters['countRowsReadFromCsvFile']}")

    print(f"#Total writes to Mongo = {counters['countMongoWriteSuccess']}")
    logging.warning(f"#Total writes to Mongo = {counters['countMongoWriteSuccess']}")

    print(f"#Total failed writes to Mongo = {counters['countMongoWriteFailed']}")
    logging.warning(f"#Total failed writes to output CSV = {counters['countMongoWriteFailed']}")
####
def insertIntoMongoDb(df):
    # https://stackoverflow.com/questions/27416296/how-to-push-a-csv-data-to-mongodb-using-python
    # https://datatofish.com/export-pandas-dataframe-json/
    global mongoDbName, mongoCollName, nRowsSkip, nRowsRead
    #print(f"\n\n\n\t\t\t\tDataframe=\n{df}")
    #logging.warning(f"\n\n\n\t\t\t\tDataframe=\n{df}")
    jsonFileName = 'D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/temp-nr' + str( int(nRowsRead/1000 ) ) + 'k-nsk' + str( int(nRowsSkip/1000 ) ) + 'k.json'
    #df.to_json('D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/temp.json', orient='records')
    df.to_json(jsonFileName, orient='records')
    jdf = open(jsonFileName).read()
    data = json.loads(jdf)      # data is a list of dictionaries [{}, {}, etc] where each {} is the info for a particular record (row) from csv.
    #print(f"data=\n\n{data}\n\n")
    #logging.warning(f"data=\n\n{data}\n\n")
    client = MongoClient('localhost:27017')
    #database = client['testdsdbname1']
    #collection = database['testdscollname1']
    database = client[mongoDbName]
    collection = database[mongoCollName]
    #collection = client.testdsdbname1.testdscollname1
    print(f"\t\tDb already has {collection.find().count()} records.")
    logging.warning(f"\t\tDb already has {collection.find().count()} records.")
    logging.warning(f"\t\tDb already has {collection.find().count()} records.")
    try:
        insertReturnInfo = collection.insert_many(data)
    except Exception as e:
        print(f'ERROR: Mongo write problem.\nError type={type(e)} and Error={e}')
        logging.error(f'ERROR: Mongo write problem.\nError type={type(e)} and Error={e}')
        client.close()
        return(1)
    print(f"\t\t\t\tInsert Success -- Inserted {len(insertReturnInfo.inserted_ids)} records.")
    logging.warning(f"\t\t\t\tInsert Success -- Inserted {len(insertReturnInfo.inserted_ids)} records.")
    counters['countMongoWriteSuccess'] += len(insertReturnInfo.inserted_ids)
    print(f"\t\tNow Db has {collection.find().count()} records.")
    logging.warning(f"\t\tNow Db has {collection.find().count()} records.")
    client.close()
    return(0)
####
### ----------------- FUNCTION DEFINITIONS  END  ------------- ###
### ------------------- MAIN LOGIC STARTS HERE  -------------- ###
print(f'\nStart time of program: {datetime.now().strftime("%c")}')

if ( processCLAsAndSetup() != 0 ):
    print(f"ERROR: Problem processing the CLA.")
    exit(0)

pd.set_option('display.max_columns', 100)
logging.basicConfig(level=logging.WARNING, filename=logFilename,                                                       \
    filemode='w', format='%(asctime)s %(levelname)s:%(message)s')

print(f'\nCompleted CLA processing and logging to file: {logFilename}')
logging.warning(f'\nCompleted CLA processing and logging to file: {logFilename}')
print(f'\nStart time: {datetime.now().strftime("%c")}')
logging.warning(f'\nStart time: {datetime.now().strftime("%c")}')

# logging the command line argument information
logging.warning(f"#CLA passed = {len(sys.argv)}")
for claNumber, ele in enumerate(sys.argv):
    logging.warning(f"Argument #{claNumber} = {ele}")

#dfData = pd.DataFrame()
#nRowsSkip = 0, nRowsRead = -1
dfData, retCodeReadCsvFileIntoDataframe = readCsvFileIntoDataframe(fileInputCsv, nRowsSkip, nRowsRead)
if ( retCodeReadCsvFileIntoDataframe != 0):
    print(f"ERROR: Problem reading in the CSV file")
    print(f"Exiting with error 200")
    loggin.error(f"ERROR: Problem reading in the CSV file")
    exit(200)
else:
    print(f"Finished reading the CSV file into the panda dataframe dfData.")
    logging.warning(f"Finished reading the CSV file into the panda dataframe dfData.")

# for testing -- START
# for eleNum, ele in enumerate(dataArray[0:2]):
#     print(f"\n\nElement #{eleNum} = \n{ele}")
# for testing -- END

if ( insertIntoMongoDb(dfData) != 0 ):
    print(f"ERROR: Insertion into MongoDb had some problem.")
    logging.warning(f"ERROR: Insertion into MongoDb had some problem.")
else:
    print(f"Insertion into Mongo success, RC=0.")
    logging.warning(f"Insertion into Mongo success, RC=0.")

summaryPrint()
print(f'\nEnd time of program: {datetime.now().strftime("%c")}')
logging.warning(f'\nEnd time of program: {datetime.now().strftime("%c")}')
exit(0)
### -------------------  MAIN LOGIC ENDS HERE   -------------- ###