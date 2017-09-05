-- register the piggybank.jar for CSVExcelStorage function
-- the PIG_HOME is an externalized property which is looked up from task_3.properties
REGISTER '$PIG_HOME/lib/piggybank.jar';

-- load the data set using CSVExcelStorage function
-- the INPUT_PATH is an externalized property which is looked up from task_3.properties
crime_data = LOAD '$INPUT_PATH/crimes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX','SKIP_INPUT_HEADER') AS (id:chararray, case_num:chararray, date:chararray, block: chararray, iucr: chararray, pri_type: chararray, desc:chararray, loc_desc:chararray, arrest:boolean, domestic:boolean, beat: chararray, district: chararray, ward:chararray, comm_area: chararray, fbi_code: chararray, x_coord:float, y_coord:float, year:int, updtd_on: chararray,lat:float, lon:float, loc: chararray);

-- FILTER (based on a type and whether an arrest was done ) at the starting itself so that the number of records will be limited
crime_data_filtered = FILTER crime_data BY pri_type=='THEFT' AND arrest;

-- extract only the district because we need to count based on district only
crime_data_projected = FOREACH crime_data_filtered GENERATE district;

-- group crime_data_projected by district because we want to find the count per district
crime_data_grouped = GROUP crime_data_projected BY district;

-- use the COUNT function to find the count per group(i.e. district)
crime_data_count = FOREACH crime_data_grouped GENERATE group AS district, COUNT(crime_data_projected) AS theft_arrests;

-- store the output in local/hadoop file system
-- the OUTPUT_PATH is an externalized property which is looked up from task_3.properties
STORE crime_data_count INTO '$OUTPUT_PATH/task_3' USING PigStorage(',');
