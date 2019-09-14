# DataVisualisation-NYCrime
Analysis of NY Crime, Pedestrian and Port Arrivals

Please refer the report and presentation uploaded in the repo for an overview of the project.

The information from the Pedestrian and Ports data is converted manually into CSV files:
NYCPedestrianDataPrepared-20190909-clean6.csv,
NYPD_Complaint_Data_Historic.csv, and
PortsDataPrepared-20190906-clean3.csv

After loading to MongoDb, the python script "analysisDataStory11SHORTLists.py" is run to create the final output CSV file. Uploaded in the repo as "outCSVFileSHORT-OIRGINAL.csv".

This output CSV file is changed a little before using it for processing by Tableau and R package. The necessary files are also uploaded to this repo:
File used for R: "RegR-outCSVFileSHORT-NoKY_CD-Sorted.csv"   ,   using the script: "DataStory-R1.R"
File used for Tableau: "outCSVFileSHORT-NoKY_CD-Sorted.csv"



# LOADING THE DATA INTO MONGO DB  ###################
General way to call the python script for loading the three data sources into each MongoDb collection:
python <progName> <inputCSVfile> <#nRowsRead> <nRowsSkip> <mongoDbName> <mongoCollectionName> <logFilename>
#
# loading the Crime Data into MongoDb

python dataInsertMongoNYCrime5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/finalDataSourcesUsed4MongoAGAIN/NYPD_Complaint_Data_Historic.csv" 1000000 0 db2story coll21nyccrimedata LOG_dataInsertMongoNYCrime5-nr1000k-nsk0k-again.log

python dataInsertMongoNYCrime5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/finalDataSourcesUsed4MongoAGAIN/NYPD_Complaint_Data_Historic.csv" 1000000 1000000 db2story coll21nyccrimedata LOG_dataInsertMongoNYCrime5-nr1000k-nsk1000k-again.log

python dataInsertMongoNYCrime5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/finalDataSourcesUsed4MongoAGAIN/NYPD_Complaint_Data_Historic.csv" 1000000 2000000 db2story coll21nyccrimedata LOG_dataInsertMongoNYCrime5-nr1000k-nsk2000k-again.log

python dataInsertMongoNYCrime5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/finalDataSourcesUsed4MongoAGAIN/NYPD_Complaint_Data_Historic.csv" 1000000 3000000 db2story coll21nyccrimedata LOG_dataInsertMongoNYCrime5-nr1000k-nsk3000k-again.log

python dataInsertMongoNYCrime5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/finalDataSourcesUsed4MongoAGAIN/NYPD_Complaint_Data_Historic.csv" 1000000 4000000 db2story coll21nyccrimedata LOG_dataInsertMongoNYCrime5-nr1000k-nsk4000k-again.log

python dataInsertMongoNYCrime5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/finalDataSourcesUsed4MongoAGAIN/NYPD_Complaint_Data_Historic.csv" 1000000 5000000 db2story coll21nyccrimedata LOG_dataInsertMongoNYCrime5-nr1000k-nsk5000k-again.log

python dataInsertMongoNYCrime5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/finalDataSourcesUsed4MongoAGAIN/NYPD_Complaint_Data_Historic.csv" 1000000 6000000 db2story coll21nyccrimedata LOG_dataInsertMongoNYCrime5-nr1000k-nsk6000k-again.log

#
# loading the Pedestrian Data into MongoDb
loading ALL the data into Mongo 7524 records (11 years x 6 per year x 114 per set = 11 x 6 x 114 -- has total 7524 rows
		
python dataInsertMongoNYPedestrian5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/NYCPedestrianDataPrepared-20190909-clean6.csv" -1 0 db1story coll2pedesdata  LOG_dataInsertMongoNYPedestrian5-loadAll.log

#
# loading the Port Arrivals Data into MongoDb
loading ALL the data into Mongo 396 data records (11 years x 3 cities per year x 12 per city = 11 x 3 x 12 -- has total 396 rows
		
python dataInsertMongoNYPortsdata5.py "D:/EverythingD/01SRH-BDBA Acads/Blk7-DataStoryTelling/Data4Analysis/PortsDataPrepared-20190906-clean3.csv" -1 0 db1story coll3portsdata LOG_dataInsertMongoNYPortsdata5-loadAll.log


# RUNNING THE SCRIPT TO ANALYSE DATA  ###################

python analysisDataStory11SHORTLists.py

after running the above script, the output CSV file should have 110 rows by 449 columns -- please refer to the "outCSVFileSHORT-OIRGINAL.csv" uploaded for reference.


