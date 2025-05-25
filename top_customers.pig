-- Load the CSV file
raw_data = LOAD 'hdfs://localhost:8020/user/cloudera/virtInput/dataset.csv' USING PigStorage(',')
AS (id:int, nom:chararray, achat:double, date:chararray);

-- Skip the header row
ranked_data = RANK raw_data;
data = FILTER ranked_data BY rank_raw_data > 1;

-- Group by 'nom' and calculate total 'achat'
grouped = GROUP data BY nom;
totals = FOREACH grouped GENERATE group AS nom, SUM(data.achat) AS total_achats;

-- Order by total_achats descending and limit to 5
ordered_totals = ORDER totals BY total_achats DESC;
top_5 = LIMIT ordered_totals 5;

-- Store the result
STORE top_5 INTO 'hdfs://localhost:8020/user/cloudera/virtOutput/top_customers_pig' USING PigStorage(',');

-- Display the result (optional)
DUMP top_5;
