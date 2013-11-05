/* 
** @Author: Josef Harte		@Student_Number: 12251747		@Date_Last_Modified: 12/03/2013	
** This Pig script determines the number of unique hosts per month in the sample Apache log file "access.log" based upon a single entry per line
** and each entry having the format shown in the following sample entry between << and >>:
** <<192.251.226.205 - - [06/Dec/2010:17:28:34 +0100] "GET /favicon.ico HTTP/1.1" 404 333 "-" "Mozilla/5.0 (X11; U; Linux i686; sv-SE; rv:1.9.0.19) Gecko/2010091807
** Iceweasel/3.0.6 (Debian-3.0.6-3)">>
** Run the script on the local machine with: pig -x local <script_name>
*/

records = LOAD '/home/joe/Desktop/College/Cloud/Project/access.log' USING PigStorage('[') AS (host:chararray, timestamp:chararray); /* Splits the line on [ */
records_temp = FOREACH records GENERATE host, REPLACE( timestamp, SUBSTRING( timestamp, 11, 12), SUBSTRING( timestamp, 0, 3) ) AS timestamp_temp;
monthly_records_temp = FOREACH records_temp GENERATE host, REPLACE( timestamp_temp, SUBSTRING( timestamp_temp, 0, 2), SUBSTRING( timestamp_temp, 7, 11) ) AS timestamp_temp; 
monthly_records = FOREACH monthly_records_temp GENERATE host, SUBSTRING( timestamp_temp, 0, 8) AS month;
unique_records = DISTINCT monthly_records;
grouped_records = GROUP unique_records BY month;
number_of_unique = FOREACH grouped_records GENERATE group, SIZE( unique_records );
STORE number_of_unique INTO '/home/joe/Desktop/pig_output';

