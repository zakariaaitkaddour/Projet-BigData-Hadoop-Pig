-- Load the CSV file
raw_data = LOAD 'hdfs://localhost:8020/user/cloudera/virtInput/dataset.csv' USING PigStorage(',')
AS (id:int, nom:chararray, achat:double, date:chararray);

-- Skip the header row
ranked_data = RANK raw_data;
data = FILTER ranked_data BY rank_raw_data > 1;

-- Group by 'nom' and count transactions
grouped_by_nom = GROUP data BY nom;
frequency = FOREACH grouped_by_nom GENERATE group AS nom, COUNT(data) AS transaction_count;

-- Order by transaction_count descending
ordered_frequency = ORDER frequency BY transaction_count DESC;

-- Store the result
STORE ordered_frequency INTO 'hdfs://localhost:8020/user/cloudera/virtOutput/purchase_frequency' USING PigStorage(',');

-- Display the result (optional)
DUMP ordered_frequency;

