-- Load the CSV file (assuming comma-separated with header)
raw_data = LOAD 'hdfs://localhost:8020/user/cloudera/virtInput/dataset.csv' USING PigStorage(',')
AS (id:int, nom:chararray, achat:double, date:chararray);

-- Skip the header row (first line)
ranked_data = RANK raw_data;
data = FILTER ranked_data BY rank_raw_data > 1;

-- Group by 'nom' and calculate total 'achat'
grouped = GROUP data BY nom;
totals = FOREACH grouped GENERATE group AS nom, SUM(data.achat) AS total_achats;

-- Store the result in HDFS
STORE totals INTO 'hdfs://localhost:8020/user/cloudera/virtOutput/customer_totals_pig' USING PigStorage(',');

-- Display the result (optional, for local testing)
DUMP totals;
