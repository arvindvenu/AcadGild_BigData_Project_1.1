-- register the piggybank.jar for CSVExcelStorage function
-- the PIG_HOME is an externalized property which is looked up from task_2.properties
REGISTER '$PIG_HOME/lib/piggybank.jar';

-- load the data set using CSVExcelStorage function
-- the INPUT_PATH is an externalized property which is looked up from task_2.properties
crime_data = LOAD '$INPUT_PATH/crimes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX','SKIP_INPUT_HEADER') AS (id:chararray, case_num:chararray, date:chararray, block: chararray, iucr: chararray, pri_type: chararray, desc:chararray, loc_desc:chararray, arrest:boolean, domestic:boolean, beat: chararray, district: chararray, ward:chararray, comm_area: chararray, fbi_code: chararray, x_coord:float, y_coord:float, year:int, updtd_on: chararray,lat:float, lon:float, loc: chararray);

-- FILTER based on a specific FBI code at the starting itself so that the number of records
-- will be limited
-- the FBI_CODE is an externalized property which is looked up from task_2.properties
crime_data_filtered = FILTER crime_data BY fbi_code == '$FBI_CODE';

-- extract only the fbi_code because we need to count based on fbi_code only
crime_data_projected = FOREACH crime_data_filtered GENERATE fbi_code;

-- group crime_data_projected by fbi_code because we want to find the count per fbi_code
crime_data_grouped = GROUP crime_data_projected BY fbi_code;

-- use the COUNT function to find the count per group(i.e. fbi_code)
crime_data_count = FOREACH crime_data_grouped GENERATE group AS fbi_code, COUNT(crime_data_projected) AS fbi_code_count;

-- store the output in local/hadoop file system
-- the OUTPUT_PATH is an externalized property which is looked up from task_2.properties
STORE crime_data_count INTO '$OUTPUT_PATH/task_2' USING PigStorage(',');
