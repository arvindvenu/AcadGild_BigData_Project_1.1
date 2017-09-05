-- register the piggybank.jar for CSVExcelStorage function
-- the PIG_HOME is an externalized property which is looked up from task_4.properties
REGISTER '$PIG_HOME/lib/piggybank.jar';

-- load the data set using CSVExcelStorage function
-- the INPUT_PATH is an externalized property which is looked up from task_4.properties
crime_data = LOAD '$INPUT_PATH/crimes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX','SKIP_INPUT_HEADER') AS (id:chararray, case_num:chararray, date:chararray, block: chararray, iucr: chararray, pri_type: chararray, desc:chararray, loc_desc:chararray, arrest:boolean, domestic:boolean, beat: chararray, district: chararray, ward:chararray, comm_area: chararray, fbi_code: chararray, x_coord:float, y_coord:float, year:int, updtd_on: chararray,lat:float, lon:float, loc: chararray);

-- Project only the date and arrest field because we want to find the number of arrests between
-- a from date and a to date
-- the DATE_FORMAT is an externalized property which is looked up from task_4.properties
crime_data_projected = FOREACH crime_data GENERATE ToDate(date,'$DATE_FORMAT') AS  date, arrest ;

-- filter for records between a from date and a to date
-- the FROM_DATE and TO_DATE are externalized properties which are looked up from task_4.properties
-- Please note that for from date the condition is >= and for to date the condition is < because we want to exclude the to date
crime_data_filtered = FILTER crime_data_projected BY arrest AND date >= ToDate('$FROM_DATE', '$DATE_FORMAT') AND date < ToDate('$TO_DATE', '$DATE_FORMAT');

-- GROUP by all fields to find the row count
crime_data_grouped = GROUP crime_data_filtered ALL;

-- now use the COUNT function to find the row count
crime_data_count = FOREACH crime_data_grouped GENERATE COUNT(crime_data_filtered) AS crime_count;

-- store the output in local/hadoop file system
-- the OUTPUT_PATH is an externalized property which is looked up from task_4.properties
STORE crime_data_count INTO '$OUTPUT_PATH/task_4' USING PigStorage(',');
