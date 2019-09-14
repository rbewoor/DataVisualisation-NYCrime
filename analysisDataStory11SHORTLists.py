import logging
from pymongo import MongoClient
from datetime import datetime
import numpy as np
import pandas as pd
import csv

pd.set_option('display.max_columns', 100)
logging.basicConfig(level=logging.WARNING, filename='LOG_analysisDataStory11SHORTLists1.log',                                                       \
    filemode='w', format='%(asctime)s %(levelname)s:%(message)s')

global arrPortNames, arrKY_CD, arrOFNS_DESC, arrPREM_DESC_TYPE, arrLAW_CAT_CD
global arrPedesYear, arrPedesMonth
global arrCrimeDbBoroughNames, arrPedesDbBoroughNames
global FIELDNAMES, outputCSVfile, countTotalRowsWrittenToOutputFile

outputCSVfile = 'D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/outCSVSHORTLists/outCSVFileSHORT.csv'

arrPedesYear = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017]
arrPedesMonth = [5, 9]

arrCrimeDbBoroughNames = [ "QUEENS", "BRONX", "BROOKLYN", "STATEN ISLAND", "MANHATTAN" ]
arrPedesDbBoroughNames = [ "Queens", "Bronx", "Brooklyn", "Staten Island", "Manhattan" ]

arrPortsDbPortNames = [ "new york", "newark", "los angeles" ]

arrKY_CD = [101, 102, 103, 104, 105, 106, 107, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 230, 231, 232, 233, 234, 235, 236, 237, 238, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 455, 460, 571, 572, 577, 578, 672, 675, 676, 677, 678, 685, 881]

arrOFNS_DESC = ['ABORTION', 'ADMINISTRATIVE CODE', 'ADMINISTRATIVE CODES', 'AGRICULTURE & MRKTS LAW-UNCLASSIFIED', 'ALCOHOLIC BEVERAGE CONTROL LAW', 'ANTICIPATORY OFFENSES', 'ARSON', 'ASSAULT 3 & RELATED OFFENSES', "BURGLAR'S TOOLS", 'BURGLARY', 'CHILD ABANDONMENT/NON SUPPORT', 'CRIMINAL MISCHIEF & RELATED OF', 'CRIMINAL TRESPASS', 'DANGEROUS DRUGS', 'DANGEROUS WEAPONS', 'DISORDERLY CONDUCT', 'DISRUPTION OF A RELIGIOUS SERV', 'ENDAN WELFARE INCOMP', 'ESCAPE 3', 'FELONY ASSAULT', 'FORGERY', 'FORTUNE TELLING', 'FRAUDS', 'FRAUDULENT ACCOSTING', 'GAMBLING', 'GRAND LARCENY', 'GRAND LARCENY OF MOTOR VEHICLE', 'HARRASSMENT 2', 'HOMICIDE-NEGLIGENT,UNCLASSIFIE', 'HOMICIDE-NEGLIGENT-VEHICLE', 'INTOXICATED & IMPAIRED DRIVING', 'INTOXICATED/IMPAIRED DRIVING', 'JOSTLING', 'KIDNAPPING', 'KIDNAPPING & RELATED OFFENSES', 'KIDNAPPING AND RELATED OFFENSES', 'LOITERING', 'LOITERING FOR DRUG PURPOSES', 'LOITERING/DEVIATE SEX', 'LOITERING/GAMBLING (CARDS, DIC', 'MISCELLANEOUS PENAL LAW', 'MURDER & NON-NEGL. MANSLAUGHTER', 'NEW YORK CITY HEALTH CODE', 'NYS LAWS-UNCLASSIFIED FELONY', 'NYS LAWS-UNCLASSIFIED VIOLATION', 'OFF. AGNST PUB ORD SENSBLTY &', 'OFFENSES AGAINST MARRIAGE UNCL', 'OFFENSES AGAINST PUBLIC ADMINI', 'OFFENSES AGAINST PUBLIC SAFETY', 'OFFENSES AGAINST THE PERSON', 'OFFENSES INVOLVING FRAUD', 'OFFENSES RELATED TO CHILDREN', 'OTHER OFFENSES RELATED TO THEF', 'OTHER STATE LAWS', 'OTHER STATE LAWS (NON PENAL LA', 'OTHER STATE LAWS (NON PENAL LAW)', 'OTHER TRAFFIC INFRACTION', 'PETIT LARCENY', 'PETIT LARCENY OF MOTOR VEHICLE', 'POSSESSION OF STOLEN PROPERTY', 'PROSTITUTION & RELATED OFFENSES', 'RAPE', 'ROBBERY', 'SEX CRIMES', 'THEFT OF SERVICES', 'THEFT-FRAUD', 'UNAUTHORIZED USE OF A VEHICLE', 'UNDER THE INFLUENCE OF DRUGS', 'UNLAWFUL POSS. WEAP. ON SCHOOL', 'VEHICLE AND TRAFFIC LAWS']

arrPREM_DESC_TYPE = ['ABANDONED BUILDING', 'AIRPORT TERMINAL', 'ATM', 'BANK', 'BAR/NIGHT CLUB', 'BEAUTY & NAIL SALON', 'BOOK/CARD', 'BRIDGE', 'BUS (NYC TRANSIT)', 'BUS (OTHER)', 'BUS STOP', 'BUS TERMINAL', 'CANDY STORE', 'CEMETERY', 'CHAIN STORE', 'CHECK CASHING BUSINESS', 'CHURCH', 'CLOTHING/BOUTIQUE', 'COMMERCIAL BUILDING', 'CONSTRUCTION SITE', 'DEPARTMENT STORE', 'DOCTOR/DENTIST OFFICE', 'DRUG STORE', 'DRY CLEANER/LAUNDRY', 'FACTORY/WAREHOUSE', 'FAST FOOD', 'FERRY/FERRY TERMINAL', 'FOOD SUPERMARKET', 'GAS STATION', 'GROCERY/BODEGA', 'GYM/FITNESS FACILITY', 'HIGHWAY/PARKWAY', 'HOSPITAL', 'HOTEL/MOTEL', 'JEWELRY', 'LIQUOR STORE', 'LOAN COMPANY', 'MAILBOX INSIDE', 'MAILBOX OUTSIDE', 'MARINA/PIER', 'MOSQUE', 'OPEN AREAS (OPEN LOTS)', 'OTHER', 'OTHER HOUSE OF WORSHIP', 'PARK/PLAYGROUND', 'PARKING LOT/GARAGE (PRIVATE)', 'PARKING LOT/GARAGE (PUBLIC)', 'PHOTO/COPY', 'PRIVATE/PAROCHIAL SCHOOL', 'PUBLIC BUILDING', 'PUBLIC SCHOOL', 'RESIDENCE - APT. HOUSE', 'RESIDENCE - PUBLIC HOUSING', 'RESIDENCE-HOUSE', 'RESTAURANT/DINER', 'SHOE', 'SMALL MERCHANT', 'SOCIAL CLUB/POLICY', 'STORAGE FACILITY', 'STORE UNCLASSIFIED', 'STREET', 'SYNAGOGUE', 'TAXI (LIVERY LICENSED)', 'TAXI (YELLOW LICENSED)', 'TAXI/LIVERY (UNLICENSED)', 'TELECOMM. STORE', 'TRAMWAY', 'TRANSIT - NYC SUBWAY', 'TRANSIT FACILITY (OTHER)', 'TUNNEL', 'VARIETY STORE', 'VIDEO STORE']

arrLAW_CAT_CD = [ "FELONY", "MISDEMEANOR", "VIOLATION" ]

FIELDNAMES = [ 'year' , 'month' , 'boroName' , 'countPedes' , 'portNY' , 'countPortNY' , 'portNewark' , 'countPortNewark' , 'portLA' , 'countPortLA' , 'crimeTotal' , 'crimeKY_CD1', 'sumCrimeKY_CD1', 'crimeKY_CD2', 'sumCrimeKY_CD2', 'crimeKY_CD3', 'sumCrimeKY_CD3', 'crimeKY_CD4', 'sumCrimeKY_CD4', 'crimeKY_CD5', 'sumCrimeKY_CD5', 'crimeKY_CD6', 'sumCrimeKY_CD6', 'crimeKY_CD7', 'sumCrimeKY_CD7', 'crimeKY_CD8', 'sumCrimeKY_CD8', 'crimeKY_CD9', 'sumCrimeKY_CD9', 'crimeKY_CD10', 'sumCrimeKY_CD10', 'crimeKY_CD11', 'sumCrimeKY_CD11', 'crimeKY_CD12', 'sumCrimeKY_CD12', 'crimeKY_CD13', 'sumCrimeKY_CD13', 'crimeKY_CD14', 'sumCrimeKY_CD14', 'crimeKY_CD15', 'sumCrimeKY_CD15', 'crimeKY_CD16', 'sumCrimeKY_CD16', 'crimeKY_CD17', 'sumCrimeKY_CD17', 'crimeKY_CD18', 'sumCrimeKY_CD18', 'crimeKY_CD19', 'sumCrimeKY_CD19', 'crimeKY_CD20', 'sumCrimeKY_CD20', 'crimeKY_CD21', 'sumCrimeKY_CD21', 'crimeKY_CD22', 'sumCrimeKY_CD22', 'crimeKY_CD23', 'sumCrimeKY_CD23', 'crimeKY_CD24', 'sumCrimeKY_CD24', 'crimeKY_CD25', 'sumCrimeKY_CD25', 'crimeKY_CD26', 'sumCrimeKY_CD26', 'crimeKY_CD27', 'sumCrimeKY_CD27', 'crimeKY_CD28', 'sumCrimeKY_CD28', 'crimeKY_CD29', 'sumCrimeKY_CD29', 'crimeKY_CD30', 'sumCrimeKY_CD30', 'crimeKY_CD31', 'sumCrimeKY_CD31', 'crimeKY_CD32', 'sumCrimeKY_CD32', 'crimeKY_CD33', 'sumCrimeKY_CD33', 'crimeKY_CD34', 'sumCrimeKY_CD34', 'crimeKY_CD35', 'sumCrimeKY_CD35', 'crimeKY_CD36', 'sumCrimeKY_CD36', 'crimeKY_CD37', 'sumCrimeKY_CD37', 'crimeKY_CD38', 'sumCrimeKY_CD38', 'crimeKY_CD39', 'sumCrimeKY_CD39', 'crimeKY_CD40', 'sumCrimeKY_CD40', 'crimeKY_CD41', 'sumCrimeKY_CD41', 'crimeKY_CD42', 'sumCrimeKY_CD42', 'crimeKY_CD43', 'sumCrimeKY_CD43', 'crimeKY_CD44', 'sumCrimeKY_CD44', 'crimeKY_CD45', 'sumCrimeKY_CD45', 'crimeKY_CD46', 'sumCrimeKY_CD46', 'crimeKY_CD47', 'sumCrimeKY_CD47', 'crimeKY_CD48', 'sumCrimeKY_CD48', 'crimeKY_CD49', 'sumCrimeKY_CD49', 'crimeKY_CD50', 'sumCrimeKY_CD50', 'crimeKY_CD51', 'sumCrimeKY_CD51', 'crimeKY_CD52', 'sumCrimeKY_CD52', 'crimeKY_CD53', 'sumCrimeKY_CD53', 'crimeKY_CD54', 'sumCrimeKY_CD54', 'crimeKY_CD55', 'sumCrimeKY_CD55', 'crimeKY_CD56', 'sumCrimeKY_CD56', 'crimeKY_CD57', 'sumCrimeKY_CD57', 'crimeKY_CD58', 'sumCrimeKY_CD58', 'crimeKY_CD59', 'sumCrimeKY_CD59', 'crimeKY_CD60', 'sumCrimeKY_CD60', 'crimeKY_CD61', 'sumCrimeKY_CD61', 'crimeKY_CD62', 'sumCrimeKY_CD62', 'crimeKY_CD63', 'sumCrimeKY_CD63', 'crimeKY_CD64', 'sumCrimeKY_CD64', 'crimeKY_CD65', 'sumCrimeKY_CD65', 'crimeKY_CD66', 'sumCrimeKY_CD66', 'crimeKY_CD67', 'sumCrimeKY_CD67', 'crimeKY_CD68', 'sumCrimeKY_CD68', 'crimeKY_CD69', 'sumCrimeKY_CD69', 'crimeKY_CD70', 'sumCrimeKY_CD70', 'crimeKY_CD71', 'sumCrimeKY_CD71', 'crimeKY_CD72', 'sumCrimeKY_CD72', 'crimeKY_CD73', 'sumCrimeKY_CD73', 'crimeKY_CD74', 'sumCrimeKY_CD74', 'crimeOFNS_DESC1', 'sumCrimeOFNS_DESC1', 'crimeOFNS_DESC2', 'sumCrimeOFNS_DESC2', 'crimeOFNS_DESC3', 'sumCrimeOFNS_DESC3', 'crimeOFNS_DESC4', 'sumCrimeOFNS_DESC4', 'crimeOFNS_DESC5', 'sumCrimeOFNS_DESC5', 'crimeOFNS_DESC6', 'sumCrimeOFNS_DESC6', 'crimeOFNS_DESC7', 'sumCrimeOFNS_DESC7', 'crimeOFNS_DESC8', 'sumCrimeOFNS_DESC8', 'crimeOFNS_DESC9', 'sumCrimeOFNS_DESC9', 'crimeOFNS_DESC10', 'sumCrimeOFNS_DESC10', 'crimeOFNS_DESC11', 'sumCrimeOFNS_DESC11', 'crimeOFNS_DESC12', 'sumCrimeOFNS_DESC12', 'crimeOFNS_DESC13', 'sumCrimeOFNS_DESC13', 'crimeOFNS_DESC14', 'sumCrimeOFNS_DESC14', 'crimeOFNS_DESC15', 'sumCrimeOFNS_DESC15', 'crimeOFNS_DESC16', 'sumCrimeOFNS_DESC16', 'crimeOFNS_DESC17', 'sumCrimeOFNS_DESC17', 'crimeOFNS_DESC18', 'sumCrimeOFNS_DESC18', 'crimeOFNS_DESC19', 'sumCrimeOFNS_DESC19', 'crimeOFNS_DESC20', 'sumCrimeOFNS_DESC20', 'crimeOFNS_DESC21', 'sumCrimeOFNS_DESC21', 'crimeOFNS_DESC22', 'sumCrimeOFNS_DESC22', 'crimeOFNS_DESC23', 'sumCrimeOFNS_DESC23', 'crimeOFNS_DESC24', 'sumCrimeOFNS_DESC24', 'crimeOFNS_DESC25', 'sumCrimeOFNS_DESC25', 'crimeOFNS_DESC26', 'sumCrimeOFNS_DESC26', 'crimeOFNS_DESC27', 'sumCrimeOFNS_DESC27', 'crimeOFNS_DESC28', 'sumCrimeOFNS_DESC28', 'crimeOFNS_DESC29', 'sumCrimeOFNS_DESC29', 'crimeOFNS_DESC30', 'sumCrimeOFNS_DESC30', 'crimeOFNS_DESC31', 'sumCrimeOFNS_DESC31', 'crimeOFNS_DESC32', 'sumCrimeOFNS_DESC32', 'crimeOFNS_DESC33', 'sumCrimeOFNS_DESC33', 'crimeOFNS_DESC34', 'sumCrimeOFNS_DESC34', 'crimeOFNS_DESC35', 'sumCrimeOFNS_DESC35', 'crimeOFNS_DESC36', 'sumCrimeOFNS_DESC36', 'crimeOFNS_DESC37', 'sumCrimeOFNS_DESC37', 'crimeOFNS_DESC38', 'sumCrimeOFNS_DESC38', 'crimeOFNS_DESC39', 'sumCrimeOFNS_DESC39', 'crimeOFNS_DESC40', 'sumCrimeOFNS_DESC40', 'crimeOFNS_DESC41', 'sumCrimeOFNS_DESC41', 'crimeOFNS_DESC42', 'sumCrimeOFNS_DESC42', 'crimeOFNS_DESC43', 'sumCrimeOFNS_DESC43', 'crimeOFNS_DESC44', 'sumCrimeOFNS_DESC44', 'crimeOFNS_DESC45', 'sumCrimeOFNS_DESC45', 'crimeOFNS_DESC46', 'sumCrimeOFNS_DESC46', 'crimeOFNS_DESC47', 'sumCrimeOFNS_DESC47', 'crimeOFNS_DESC48', 'sumCrimeOFNS_DESC48', 'crimeOFNS_DESC49', 'sumCrimeOFNS_DESC49', 'crimeOFNS_DESC50', 'sumCrimeOFNS_DESC50', 'crimeOFNS_DESC51', 'sumCrimeOFNS_DESC51', 'crimeOFNS_DESC52', 'sumCrimeOFNS_DESC52', 'crimeOFNS_DESC53', 'sumCrimeOFNS_DESC53', 'crimeOFNS_DESC54', 'sumCrimeOFNS_DESC54', 'crimeOFNS_DESC55', 'sumCrimeOFNS_DESC55', 'crimeOFNS_DESC56', 'sumCrimeOFNS_DESC56', 'crimeOFNS_DESC57', 'sumCrimeOFNS_DESC57', 'crimeOFNS_DESC58', 'sumCrimeOFNS_DESC58', 'crimeOFNS_DESC59', 'sumCrimeOFNS_DESC59', 'crimeOFNS_DESC60', 'sumCrimeOFNS_DESC60', 'crimeOFNS_DESC61', 'sumCrimeOFNS_DESC61', 'crimeOFNS_DESC62', 'sumCrimeOFNS_DESC62', 'crimeOFNS_DESC63', 'sumCrimeOFNS_DESC63', 'crimeOFNS_DESC64', 'sumCrimeOFNS_DESC64', 'crimeOFNS_DESC65', 'sumCrimeOFNS_DESC65', 'crimeOFNS_DESC66', 'sumCrimeOFNS_DESC66', 'crimeOFNS_DESC67', 'sumCrimeOFNS_DESC67', 'crimeOFNS_DESC68', 'sumCrimeOFNS_DESC68', 'crimeOFNS_DESC69', 'sumCrimeOFNS_DESC69', 'crimeOFNS_DESC70', 'sumCrimeOFNS_DESC70', 'crimePREM_TYPE_DESC1', 'sumCrimePREM_TYPE_DESC1', 'crimePREM_TYPE_DESC2', 'sumCrimePREM_TYPE_DESC2', 'crimePREM_TYPE_DESC3', 'sumCrimePREM_TYPE_DESC3', 'crimePREM_TYPE_DESC4', 'sumCrimePREM_TYPE_DESC4', 'crimePREM_TYPE_DESC5', 'sumCrimePREM_TYPE_DESC5', 'crimePREM_TYPE_DESC6', 'sumCrimePREM_TYPE_DESC6', 'crimePREM_TYPE_DESC7', 'sumCrimePREM_TYPE_DESC7', 'crimePREM_TYPE_DESC8', 'sumCrimePREM_TYPE_DESC8', 'crimePREM_TYPE_DESC9', 'sumCrimePREM_TYPE_DESC9', 'crimePREM_TYPE_DESC10', 'sumCrimePREM_TYPE_DESC10', 'crimePREM_TYPE_DESC11', 'sumCrimePREM_TYPE_DESC11', 'crimePREM_TYPE_DESC12', 'sumCrimePREM_TYPE_DESC12', 'crimePREM_TYPE_DESC13', 'sumCrimePREM_TYPE_DESC13', 'crimePREM_TYPE_DESC14', 'sumCrimePREM_TYPE_DESC14', 'crimePREM_TYPE_DESC15', 'sumCrimePREM_TYPE_DESC15', 'crimePREM_TYPE_DESC16', 'sumCrimePREM_TYPE_DESC16', 'crimePREM_TYPE_DESC17', 'sumCrimePREM_TYPE_DESC17', 'crimePREM_TYPE_DESC18', 'sumCrimePREM_TYPE_DESC18', 'crimePREM_TYPE_DESC19', 'sumCrimePREM_TYPE_DESC19', 'crimePREM_TYPE_DESC20', 'sumCrimePREM_TYPE_DESC20', 'crimePREM_TYPE_DESC21', 'sumCrimePREM_TYPE_DESC21', 'crimePREM_TYPE_DESC22', 'sumCrimePREM_TYPE_DESC22', 'crimePREM_TYPE_DESC23', 'sumCrimePREM_TYPE_DESC23', 'crimePREM_TYPE_DESC24', 'sumCrimePREM_TYPE_DESC24', 'crimePREM_TYPE_DESC25', 'sumCrimePREM_TYPE_DESC25', 'crimePREM_TYPE_DESC26', 'sumCrimePREM_TYPE_DESC26', 'crimePREM_TYPE_DESC27', 'sumCrimePREM_TYPE_DESC27', 'crimePREM_TYPE_DESC28', 'sumCrimePREM_TYPE_DESC28', 'crimePREM_TYPE_DESC29', 'sumCrimePREM_TYPE_DESC29', 'crimePREM_TYPE_DESC30', 'sumCrimePREM_TYPE_DESC30', 'crimePREM_TYPE_DESC31', 'sumCrimePREM_TYPE_DESC31', 'crimePREM_TYPE_DESC32', 'sumCrimePREM_TYPE_DESC32', 'crimePREM_TYPE_DESC33', 'sumCrimePREM_TYPE_DESC33', 'crimePREM_TYPE_DESC34', 'sumCrimePREM_TYPE_DESC34', 'crimePREM_TYPE_DESC35', 'sumCrimePREM_TYPE_DESC35', 'crimePREM_TYPE_DESC36', 'sumCrimePREM_TYPE_DESC36', 'crimePREM_TYPE_DESC37', 'sumCrimePREM_TYPE_DESC37', 'crimePREM_TYPE_DESC38', 'sumCrimePREM_TYPE_DESC38', 'crimePREM_TYPE_DESC39', 'sumCrimePREM_TYPE_DESC39', 'crimePREM_TYPE_DESC40', 'sumCrimePREM_TYPE_DESC40', 'crimePREM_TYPE_DESC41', 'sumCrimePREM_TYPE_DESC41', 'crimePREM_TYPE_DESC42', 'sumCrimePREM_TYPE_DESC42', 'crimePREM_TYPE_DESC43', 'sumCrimePREM_TYPE_DESC43', 'crimePREM_TYPE_DESC44', 'sumCrimePREM_TYPE_DESC44', 'crimePREM_TYPE_DESC45', 'sumCrimePREM_TYPE_DESC45', 'crimePREM_TYPE_DESC46', 'sumCrimePREM_TYPE_DESC46', 'crimePREM_TYPE_DESC47', 'sumCrimePREM_TYPE_DESC47', 'crimePREM_TYPE_DESC48', 'sumCrimePREM_TYPE_DESC48', 'crimePREM_TYPE_DESC49', 'sumCrimePREM_TYPE_DESC49', 'crimePREM_TYPE_DESC50', 'sumCrimePREM_TYPE_DESC50', 'crimePREM_TYPE_DESC51', 'sumCrimePREM_TYPE_DESC51', 'crimePREM_TYPE_DESC52', 'sumCrimePREM_TYPE_DESC52', 'crimePREM_TYPE_DESC53', 'sumCrimePREM_TYPE_DESC53', 'crimePREM_TYPE_DESC54', 'sumCrimePREM_TYPE_DESC54', 'crimePREM_TYPE_DESC55', 'sumCrimePREM_TYPE_DESC55', 'crimePREM_TYPE_DESC56', 'sumCrimePREM_TYPE_DESC56', 'crimePREM_TYPE_DESC57', 'sumCrimePREM_TYPE_DESC57', 'crimePREM_TYPE_DESC58', 'sumCrimePREM_TYPE_DESC58', 'crimePREM_TYPE_DESC59', 'sumCrimePREM_TYPE_DESC59', 'crimePREM_TYPE_DESC60', 'sumCrimePREM_TYPE_DESC60', 'crimePREM_TYPE_DESC61', 'sumCrimePREM_TYPE_DESC61', 'crimePREM_TYPE_DESC62', 'sumCrimePREM_TYPE_DESC62', 'crimePREM_TYPE_DESC63', 'sumCrimePREM_TYPE_DESC63', 'crimePREM_TYPE_DESC64', 'sumCrimePREM_TYPE_DESC64', 'crimePREM_TYPE_DESC65', 'sumCrimePREM_TYPE_DESC65', 'crimePREM_TYPE_DESC66', 'sumCrimePREM_TYPE_DESC66', 'crimePREM_TYPE_DESC67', 'sumCrimePREM_TYPE_DESC67', 'crimePREM_TYPE_DESC68', 'sumCrimePREM_TYPE_DESC68', 'crimePREM_TYPE_DESC69', 'sumCrimePREM_TYPE_DESC69', 'crimePREM_TYPE_DESC70', 'sumCrimePREM_TYPE_DESC70', 'crimePREM_TYPE_DESC71', 'sumCrimePREM_TYPE_DESC71', 'crimePREM_TYPE_DESC72', 'sumCrimePREM_TYPE_DESC72', 'crimeLAW_CAT_CD1', 'sumCrimeLAW_CAT_CD1', 'crimeLAW_CAT_CD2', 'sumCrimeLAW_CAT_CD2', 'crimeLAW_CAT_CD3', 'sumCrimeLAW_CAT_CD3' ]

### ----------------- FUNCTION DEFINITIONS START ------------- ###
def processDbEntriesAndWriteDataToOutputFile():
    global outputCSVfile, countTotalRowsWrittenToOutputFile
    with open(outputCSVfile, 'a', newline='') as opCsvFile:
        mywriter = csv.writer(opCsvFile) #, quotechar='"')
        # setup mongo database access parameters
        client = MongoClient('localhost:27017')
        database = client['db2story']
        coll1CrimeData = database['coll21nyccrimedata']
        coll2PedesData = database['coll22pedesdata']
        coll3PortData = database['coll23portsdata']
        
        for currPedesBoroBeingProcessed in arrPedesDbBoroughNames:
            for currPedesYearBeingProcessed in arrPedesYear:
                for currPedesMonthBeingProcessed in arrPedesMonth:
                    print(f"\n\n*** **** **** ****\nProcessing for:\nYear={currPedesYearBeingProcessed}\tMonth={currPedesMonthBeingProcessed}\t\tBoro={currPedesBoroBeingProcessed}")
                    logging.warning(f"\n\n*** **** **** ****\nProcessing for:\nYear={currPedesYearBeingProcessed}\tMonth={currPedesMonthBeingProcessed}\t\tBoro={currPedesBoroBeingProcessed}")
                    
                    rowDataToWrite = []
                    rowDataToWrite.append(currPedesYearBeingProcessed)
                    rowDataToWrite.append(currPedesMonthBeingProcessed)
                    rowDataToWrite.append(currPedesBoroBeingProcessed)

                    # read the count of pedestrians and store it  -- pipeline 1
                    # -- db.coll2pedesdata.aggregate([ {"$match": {"year": 2007, "month": 5, "boroughBridgeName": "Queens"} } , {"$group": {"_id": 1, "TotalPedesCount": {"$sum": "$count"}}} ]) --- shows 199427 count of pedes summed
                    pipeline1 = [ {"$match": {"year": 0, "month": 0, "boroughBridgeName": "garbage"} } , {"$group": {"_id": 1, "TotalPedesCount": {"$sum": "$count"}}} ]
                    pipeline1[0]['$match']['year'] = currPedesYearBeingProcessed
                    pipeline1[0]['$match']['month'] = currPedesMonthBeingProcessed
                    pipeline1[0]['$match']['boroughBridgeName'] = currPedesBoroBeingProcessed
                    cursorTotalPedesCount = coll2PedesData.aggregate(pipeline1)
                    for cursor in cursorTotalPedesCount:
                        #print(f"     entered for cursor\t cursor={cursor}")
                        logging.warning(f"     entered for cursor\t cursor={cursor}")
                        rowDataToWrite.append(cursor["TotalPedesCount"])
                    cursorTotalPedesCount.close()
                    print(f"Cur 1 done")
                    logging.warning(f"Cur 1 done")
                    
                    # process the ports data  -- pipeline 2
                    # setup and read the port passenger entry count and store it
                    # -- db.coll3portsdata.aggregate([ {"$match": {"portName": "new york", "year" : 2007, "month" : 5} } ]) --- get the document for May'2007
                    for currPortBeingProcessed in arrPortsDbPortNames:
                        #print(f"     entered for cursor\t cursor={cursor}")
                        logging.warning(f"     entered for cursor\t cursor={cursor}")
                        pipeline2 = [ {"$match": {"portName": "garbage", "year" : 0, "month" : 0} } ]
                        pipeline2[0]['$match']['year'] = currPedesYearBeingProcessed
                        pipeline2[0]['$match']['month'] = currPedesMonthBeingProcessed
                        pipeline2[0]['$match']['portName'] = currPortBeingProcessed
                        rowDataToWrite.append(currPortBeingProcessed)
                        cursorTotalPortEntryCount = coll3PortData.aggregate(pipeline2)
                        for cursor in cursorTotalPortEntryCount:
                            #print(f"     entered for cursor\t cursor={cursor}")
                            rowDataToWrite.append(cursor["count"])
                        cursorTotalPortEntryCount.close()
                    print(f"Cur 2 done")
                    logging.warning(f"Cur 2 done")
                    
                    # process for the total count of crimes in the month, year, boro and store it  -- pipeline 3
                    # setup process total crimes count for the month, year, boro
                    # -- db.coll1nyccrimedata.find({"BORO_NM": "QUEENS", "CMPLNT_FR_DT": {$regex: /^05[/][0-9]{2}[/]2007$/}}).count()   -- gave answer 9214
                    # -- db.coll1nyccrimedata.aggregate([ {"$match": {"BORO_NM": "QUEENS", "CMPLNT_FR_DT": {$regex: /^05[/][0-9]{2}[/]2007$/} } } , {"$group": {"_id": 1, "TotalCrimesForMonthYearBoro": {"$sum": 1}}} ])  -- gave answer { "_id" : 1, "TotalCrimesForMonthYearBoro" : 9214 }
                    pipeline3 = [ {"$match": {"BORO_NM": "garbage", "CMPLNT_FR_DT": {"$regex": "garbage"} } } , {"$group": {"_id": 1, "TotalCrimesForMonthYearBoro": {"$sum": 1}}} ]
                    newRegexString = '^' + str(currPedesMonthBeingProcessed).zfill(2) + '[/][0-9]{2}[/]' + str(currPedesYearBeingProcessed) + '$'
                    #print(f"\nnewRegexString=\n{newRegexString}")
                    #print(f"before pipeline3=\n{pipeline3}")
                    pipeline3[0]['$match']['CMPLNT_FR_DT']['$regex'] = newRegexString
                    pipeline3[0]['$match']['BORO_NM'] = currPedesBoroBeingProcessed.upper()
                    #print(f"query pipeline3=\n{pipeline3}")
                    cursorTotalCrimesInMonthYearBoroCount = coll1CrimeData.aggregate(pipeline3)
                    for cursor in cursorTotalCrimesInMonthYearBoroCount:
                        #print(f"     entered for cursor\t cursor={cursor}")
                        rowDataToWrite.append(cursor["TotalCrimesForMonthYearBoro"])
                    cursorTotalCrimesInMonthYearBoroCount.close()
                    print(f"Cur 3 done")
                    logging.warning(f"Cur 3 done")
                    
                    # process the KY_CD data and store it  -- pipeline 4
                    # setup and process for month, year, boro and KY_CD and store it
                    # db.coll1nyccrimedata.aggregate([ {"$match": {"KY_CD": 106, "BORO_NM": "QUEENS", "CMPLNT_FR_DT": {"$regex": "^05[/][0-9]{2}[/]2007$" } } } , {"$group": {"_id": 1, "TotalCrimesForKY_CDForMonthYearBoro": {"$sum": 1}}} ]) --- get the document for May'2007, specific boro, for the specific KY_CD   -- get answer = { "_id" : 1, "TotalCrimesForKY_CDForMonthYearBoro" : 324 }
                    # db.coll1nyccrimedata.aggregate( [ {"$match": {"KY_CD": 106, "BORO_NM": "QUEENS", "CMPLNT_FR_DT": {"$regex": "^05[/][0-9]{2}[/]2007$" } } } , {"$group": {"_id": 1, "TotalCrimesForKY_CDForMonthYearBoro": {"$sum": 1}}} ] )
                    # db.coll1nyccrimedata.distinct("BORO_NM", {"KY_CD": 106, "CMPLNT_FR_DT": {$regex: '^05[/][0-9]{2}[/]2007$' } })  -- [ "QUEENS", "BROOKLYN", "MANHATTAN", "BRONX", null, "STATEN ISLAND" ]
                    for currKY_CDBeingProcessed in arrKY_CD:
                        #print(f"\nProcessing for:\nKY_CD={currKY_CDBeingProcessed}")
                        logging.warning(f"\nProcessing for:\nKY_CD={currKY_CDBeingProcessed}")
                        pipeline4 = [ {"$match": {"KY_CD": 0, "BORO_NM": "garbage", "CMPLNT_FR_DT": {"$regex": "garbage" } } } , {"$group": {"_id": 1, "TotalCrimesForKY_CDForMonthYearBoro": {"$sum": 1}}} ]
                        newRegexString = '^' + str(currPedesMonthBeingProcessed).zfill(2) + '[/][0-9]{2}[/]' + str(currPedesYearBeingProcessed) + '$'
                        pipeline4[0]['$match']['CMPLNT_FR_DT']['$regex'] = newRegexString
                        pipeline4[0]['$match']['BORO_NM'] = currPedesBoroBeingProcessed.upper()
                        pipeline4[0]['$match']['KY_CD'] = currKY_CDBeingProcessed
                        #print(f"query pipeline4=\n{pipeline4}")
                        rowDataToWrite.append(currKY_CDBeingProcessed)
                        cursorTotalCrimesInKY_CDMonthYearBoroCount = coll1CrimeData.aggregate(pipeline4)
                        toAppendValue = 0
                        for cursor in cursorTotalCrimesInKY_CDMonthYearBoroCount:
                            #print(f"     entered for cursor\t cursor={cursor}")
                            logging.warning(f"     entered for cursor\t cursor={cursor}")
                            try:
                                toAppendValue = cursor["TotalCrimesForKY_CDForMonthYearBoro"]
                            except KeyError:
                                #print(f"\n\t\tKeyError for KY_CD={currKY_CDBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                #print(f"\t\tSo inserted 0 for the crime count and continued.")
                                logging.warning(f"\n\t\tKeyError for KY_CD={currKY_CDBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                logging.warning(f"\n\t\tSo inserted 0 for the crime count and continued.")
                                toAppendValue = 0
                            except Exception as e:
                                cursorTotalCrimesInKY_CDMonthYearBoroCount.close()
                                print(f"\nERROR: Unhandled exception processing for:\n\t\tKeyError for KY_CD={currKY_CDBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                print(f"ERROR: Error details = {e}")
                                logging.error(f"\nERROR: Unhandled exception processing for:\n\t\tKeyError for KY_CD={currKY_CDBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                logging.error(f"ERROR: Error details = {e}")
                                return(204)
                        cursorTotalCrimesInKY_CDMonthYearBoroCount.close()
                        rowDataToWrite.append(toAppendValue)
                    print(f"Cur 4 done")
                    logging.warning(f"Cur 4 done")
                    
                    # process the OFNS_DESC data and store it -- pipeline 5
                    # setup and process for month, year, boro and OFNS_DESC and store it
                    # db.coll1nyccrimedata.aggregate([ {"$match": {"OFNS_DESC": "FELONY ASSAULT", "BORO_NM": "QUEENS", "CMPLNT_FR_DT": {"$regex": "^05[/][0-9]{2}[/]2007$" } } } , {"$group": {"_id": 1, "TotalCrimesForOFNS_DESCForMonthYearBoro": {"$sum": 1}}} ]) --- get the document for May'2007, specific boro, for the specific OFNS_DESC   -- get answer = { "_id" : 1, "TotalCrimesForOFNS_DESCForMonthYearBoro" : 324 }
                    # db.coll1nyccrimedata.distinct("OFNS_DESC", {"BORO_NM": "QUEENS", "CMPLNT_FR_DT": {$regex: '^05[/][0-9]{2}[/]2007$' } })  -- answer = arrOFNS_DESC = ['FELONY ASSAULT', 'HARRASSMENT 2', 'BURGLARY', and more items totalling to 40 approx ]
                    for currOFNS_DESCBeingProcessed in arrOFNS_DESC:
                        #print(f"\nProcessing for:\nOFNS_DESC={currOFNS_DESCBeingProcessed}")
                        logging.warning(f"\nProcessing for:\nOFNS_DESC={currOFNS_DESCBeingProcessed}")
                        pipeline5 = [ {"$match": {"OFNS_DESC": "garbage", "BORO_NM": "garbage", "CMPLNT_FR_DT": {"$regex": "garbage" } } } , {"$group": {"_id": 1, "TotalCrimesForOFNS_DESCForMonthYearBoro": {"$sum": 1}}} ]
                        newRegexString = '^' + str(currPedesMonthBeingProcessed).zfill(2) + '[/][0-9]{2}[/]' + str(currPedesYearBeingProcessed) + '$'
                        pipeline5[0]['$match']['CMPLNT_FR_DT']['$regex'] = newRegexString
                        pipeline5[0]['$match']['BORO_NM'] = currPedesBoroBeingProcessed.upper()
                        pipeline5[0]['$match']['OFNS_DESC'] = currOFNS_DESCBeingProcessed
                        #print(f"query pipeline5=\n{pipeline5}")
                        rowDataToWrite.append(currOFNS_DESCBeingProcessed)
                        cursorTotalCrimesInOFNS_DESCMonthYearBoroCount = coll1CrimeData.aggregate(pipeline5)
                        toAppendValue = 0
                        for cursor in cursorTotalCrimesInOFNS_DESCMonthYearBoroCount:
                            #print(f"     entered for cursor\t cursor={cursor}")
                            logging.warning(f"     entered for cursor\t cursor={cursor}")
                            try:
                                toAppendValue = cursor["TotalCrimesForOFNS_DESCForMonthYearBoro"]
                            except KeyError:
                                #print(f"\n\t\tKeyError for OFNS_DESC={currOFNS_DESCBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                #print(f"\t\tSo inserted 0 for the crime count and continued.")
                                logging.warning(f"\n\t\tKeyError for OFNS_DESC={currOFNS_DESCBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                logging.warning(f"\n\t\tSo inserted 0 for the crime count and continued.")
                                toAppendValue = 0
                            except Exception as e:
                                cursorTotalCrimesInOFNS_DESCMonthYearBoroCount.close()
                                print(f"\nERROR: Unhandled exception processing for:\n\t\tKeyError for OFNS_DESC={currOFNS_DESCBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                print(f"ERROR: Error details = {e}")
                                logging.error(f"\nERROR: Unhandled exception processing for:\n\t\tKeyError for OFNS_DESC={currOFNS_DESCBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                logging.error(f"ERROR: Error details = {e}")
                                return(205)
                        cursorTotalCrimesInOFNS_DESCMonthYearBoroCount.close()
                        rowDataToWrite.append(toAppendValue)
                    print(f"Cur 5 done")
                    logging.warning(f"Cur 5 done")

                    # process the PREM_TYP_DESC data and store it -- pipeline 8
                    # setup and process for month, year, boro and PREM_TYP_DESC and store it
                    # db.coll1nyccrimedata.aggregate([ {"$match": {"PREM_TYP_DESC": "STREET", "BORO_NM": "QUEENS", "CMPLNT_FR_DT": {"$regex": "^05[/][0-9]{2}[/]2007$" } } } , {"$group": {"_id": 1, "TotalCrimesForPREM_TYP_DESCForMonthYearBoro": {"$sum": 1}}} ]) --- get the document for May'2007, specific boro, for the specific PREM_TYP_DESC   -- get answer = { "_id" : 1, "TotalCrimesForPREM_TYP_DESCForMonthYearBoro" : 3159 }
                    # db.coll1nyccrimedata.distinct("PREM_TYP_DESC", {"BORO_NM": "QUEENS", "CMPLNT_FR_DT": {$regex: '^05[/][0-9]{2}[/]2007$' } })  -- answer = ["BAR/NIGHT CLUB", "STREET", "RESIDENCE-HOUSE", and many more items ]
                    for currPREM_TYP_DESCBeingProcessed in arrPREM_DESC_TYPE:
                        #print(f"\nProcessing for:\nPREM_TYP_DESC={currPREM_TYP_DESCBeingProcessed}")
                        logging.warning(f"\nProcessing for:\nPREM_TYP_DESC={currPREM_TYP_DESCBeingProcessed}")
                        pipeline8 = [ {"$match": {"PREM_TYP_DESC": "garbage", "BORO_NM": "garbage", "CMPLNT_FR_DT": {"$regex": "garbage" } } } , {"$group": {"_id": 1, "TotalCrimesForPREM_TYP_DESCForMonthYearBoro": {"$sum": 1}}} ]
                        newRegexString = '^' + str(currPedesMonthBeingProcessed).zfill(2) + '[/][0-9]{2}[/]' + str(currPedesYearBeingProcessed) + '$'
                        pipeline8[0]['$match']['CMPLNT_FR_DT']['$regex'] = newRegexString
                        pipeline8[0]['$match']['BORO_NM'] = currPedesBoroBeingProcessed.upper()
                        pipeline8[0]['$match']['PREM_TYP_DESC'] = currPREM_TYP_DESCBeingProcessed
                        #print(f"query pipeline8=\n{pipeline8}")
                        rowDataToWrite.append(currPREM_TYP_DESCBeingProcessed)
                        cursorTotalCrimesInPREM_TYP_DESCMonthYearBoroCount = coll1CrimeData.aggregate(pipeline8)
                        toAppendValue = 0
                        for cursor in cursorTotalCrimesInPREM_TYP_DESCMonthYearBoroCount:
                            #print(f"     entered for cursor\t cursor={cursor}")
                            logging.warning(f"     entered for cursor\t cursor={cursor}")
                            try:
                                toAppendValue = cursor["TotalCrimesForPREM_TYP_DESCForMonthYearBoro"]
                            except KeyError:
                                #print(f"\n\t\tKeyError for PREM_TYP_DESC={currPREM_TYP_DESCBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                #print(f"\t\tSo inserted 0 for the crime count and continued.")
                                logging.warning(f"\n\t\tKeyError for PREM_TYP_DESC={currPREM_TYP_DESCBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                logging.warning(f"\n\t\tSo inserted 0 for the crime count and continued.")
                                toAppendValue = 0
                            except Exception as e:
                                cursorTotalCrimesInPREM_TYP_DESCMonthYearBoroCount.close()
                                print(f"\nERROR: Unhandled exception processing for:\n\t\tKeyError for PREM_TYP_DESC={currPREM_TYP_DESCBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                print(f"ERROR: Error details = {e}")
                                logging.error(f"\nERROR: Unhandled exception processing for:\n\t\tKeyError for PREM_TYP_DESC={currPREM_TYP_DESCBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                logging.error(f"ERROR: Error details = {e}")
                                return(208)
                        cursorTotalCrimesInPREM_TYP_DESCMonthYearBoroCount.close()
                        rowDataToWrite.append(toAppendValue)
                    print(f"Cur 8 done")
                    logging.warning(f"Cur 8 done")
                    
                    # process the LAW_CAT_CD data and store it -- pipeline 9
                    # setup and process for month, year, boro and LAW_CAT_CD and store it
                    # db.coll1nyccrimedata.aggregate([ {"$match": {"LAW_CAT_CD": "FELONY", "BORO_NM": "QUEENS", "CMPLNT_FR_DT": {"$regex": "^05[/][0-9]{2}[/]2007$" } } } , {"$group": {"_id": 1, "TotalCrimesForLAW_CAT_CDForMonthYearBoro": {"$sum": 1}}} ]) --- get the document for May'2007, specific boro, for the specific LAW_CAT_CD   -- get answer = { "_id" : 1, "TotalCrimesForLAW_CAT_CDForMonthYearBoro" : 3006 }
                    # db.coll1nyccrimedata.distinct("LAW_CAT_CD", {"BORO_NM": "QUEENS", "CMPLNT_FR_DT": {$regex: '^05[/][0-9]{2}[/]2007$' } })  -- answer = [ "FELONY", "VIOLATION", "MISDEMEANOR" ]
                    for currLAW_CAT_CDBeingProcessed in arrLAW_CAT_CD:
                        #print(f"\nProcessing for:\nLAW_CAT_CD={currLAW_CAT_CDBeingProcessed}")
                        logging.warning(f"\nProcessing for:\nLAW_CAT_CD={currLAW_CAT_CDBeingProcessed}")
                        pipeline9 = [ {"$match": {"LAW_CAT_CD": "garbage", "BORO_NM": "garbage", "CMPLNT_FR_DT": {"$regex": "garbage" } } } , {"$group": {"_id": 1, "TotalCrimesForLAW_CAT_CDForMonthYearBoro": {"$sum": 1}}} ]
                        newRegexString = '^' + str(currPedesMonthBeingProcessed).zfill(2) + '[/][0-9]{2}[/]' + str(currPedesYearBeingProcessed) + '$'
                        pipeline9[0]['$match']['CMPLNT_FR_DT']['$regex'] = newRegexString
                        pipeline9[0]['$match']['BORO_NM'] = currPedesBoroBeingProcessed.upper()
                        pipeline9[0]['$match']['LAW_CAT_CD'] = currLAW_CAT_CDBeingProcessed
                        #print(f"query pipeline9=\n{pipeline9}")
                        rowDataToWrite.append(currLAW_CAT_CDBeingProcessed)
                        cursorTotalCrimesInLAW_CAT_CDMonthYearBoroCount = coll1CrimeData.aggregate(pipeline9)
                        toAppendValue = 0
                        for cursor in cursorTotalCrimesInLAW_CAT_CDMonthYearBoroCount:
                            #print(f"     entered for cursor\t cursor={cursor}")
                            logging.warning(f"     entered for cursor\t cursor={cursor}")
                            try:
                                toAppendValue = cursor["TotalCrimesForLAW_CAT_CDForMonthYearBoro"]
                            except KeyError:
                                #print(f"\n\t\tKeyError for LAW_CAT_CD={currLAW_CAT_CDBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                #print(f"\t\tSo inserted 0 for the crime count and continued.")
                                logging.warning(f"\n\t\tKeyError for LAW_CAT_CD={currLAW_CAT_CDBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                logging.warning(f"\n\t\tSo inserted 0 for the crime count and continued.")
                                toAppendValue = 0
                            except Exception as e:
                                cursorTotalCrimesInLAW_CAT_CDMonthYearBoroCount.close()
                                print(f"\nERROR: Unhandled exception processing for:\n\t\tKeyError for LAW_CAT_CD={currLAW_CAT_CDBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                print(f"ERROR: Error details = {e}")
                                logging.error(f"\nERROR: Unhandled exception processing for:\n\t\tKeyError for LAW_CAT_CD={currLAW_CAT_CDBeingProcessed} , Year={currPedesYearBeingProcessed} , Month={currPedesMonthBeingProcessed} , Boro={currPedesBoroBeingProcessed.upper()}")
                                logging.error(f"ERROR: Error details = {e}")
                                return(209)
                        cursorTotalCrimesInLAW_CAT_CDMonthYearBoroCount.close()
                        rowDataToWrite.append(toAppendValue)
                    print(f"Cur 9 done")
                    logging.warning(f"Cur 9 done")
                    
                    # all data for the csv file row is ready, so write it to output file
                    try:
                        mywriter.writerow(rowDataToWrite)
                        countTotalRowsWrittenToOutputFile += 1
                    except Exception as e1:
                        print(f"\nERROR: Unhandled exception writing data row:\n\t\t{rowDataToWrite}")
                        logging.error(f"\nERROR: Unhandled exception writing data row:\n\t\t{rowDataToWrite}")
                        print(f"ERROR: Error details = {e1}")
                        logging.error(f"ERROR: Error details = {e1}")
                        return(301)

    return(0)
####
### ----------------- FUNCTION DEFINITIONS  END  ------------- ###

### ------------------- MAIN LOGIC STARTS HERE  -------------- ###

print(f"\nStarted the program\n")
#print('\nStartTime is:',datetime.now().strftime("%c"))
print(f'\nStartTime is: {datetime.now().strftime("%c")}')
logging.warning(f"\nStarted the program\n")
#logging.warning('\nStartTime is:',datetime.now().strftime("%c"))
logging.warning(f'\nStartTime is: {datetime.now().strftime("%c")}')

countTotalRowsWrittenToOutputFile = 0
# open the output file and write the header row
with open(outputCSVfile, 'w', newline='') as opCsvFile:
    mywriter = csv.writer(opCsvFile) #, quotechar='"')
    mywriter.writerow(FIELDNAMES)
    countTotalRowsWrittenToOutputFile += 1
print(f"\t\tOpened output file and wrote the header.\n\t\tOutput file location: {outputCSVfile}")
logging.warning(f"\n\t\tOpened output file and wrote the header.\n\t\tOutput file location: {outputCSVfile}")

# process the entries from mongo dbs and write the data to output file
print(f"\n\t\tStarting mongo db access and data writing...")
logging.warning(f"\n\t\tStarting mongo db access and data writing...")
processEntryRC = 0
processEntryRC = processDbEntriesAndWriteDataToOutputFile()
if processEntryRC != 0:
    print(f"\nERROR: function processDbEntriesAndWriteDataToOutputFile had RC = {processEntryRC}")
    logging.error(f"\nERROR: function processDbEntriesAndWriteDataToOutputFile had RC = {processEntryRC}")
    print(f"Exiting program with RC=100")
    logging.warning(f"Exiting program with RC=100")
    exit(100)
else:
    print(f"\nSuccessfully processed mongo db entries and wrote data to output file.")
    print(f"Total writes, including header, to output file={countTotalRowsWrittenToOutputFile}")
    logging.warning(f"\nSuccessfully processed mongo db entries and wrote data to output file.")
    logging.warning(f"Total writes, including header, to output file={countTotalRowsWrittenToOutputFile}")

## all good, exiting the program
print(f"\n\nNormal exit")
logging.warning(f"\nNormal exit")

print(f'\nEndTime is: {datetime.now().strftime("%c")}')
logging.warning(f'\nEndTime is: {datetime.now().strftime("%c")}')

exit(0)
### -------------------- MAIN LOGIC ENDS HERE  --------------- ###