-- Load the CSV file
raw_data = LOAD 'hdfs://localhost:8020/user/cloudera/virtInput/dataset.csv' USING PigStorage(',')
AS (id:int, nom:chararray, achat:double, date:chararray);

-- Skip the header row
ranked_data = RANK raw_data;
data = FILTER ranked_data BY rank_raw_data > 1;

-- Extract year and month (format YYYY-MM-DD)
monthly_data = FOREACH data GENERATE nom, achat, SUBSTRING(date, 0, 7) AS year_month;

-- Group by month and calculate total 'achat'
grouped_by_month = GROUP monthly_data BY year_month;
monthly_totals = FOREACH grouped_by_month GENERATE group AS year_month, SUM(monthly_data.achat) AS total_achats;

-- Order by year_month ascending
ordered_monthly_totals = ORDER monthly_totals BY year_month ASC;

-- Store the result
STORE ordered_monthly_totals INTO 'hdfs://localhost:8020/user/cloudera/virtOutput/monthly_trends' USING PigStorage(',');

-- Display the result (optional)
DUMP ordered_monthly_totals;
