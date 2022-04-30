SELECT
    date_valid_std,
    tot_precipitation_in,
    probability_of_precipitation_pct 
FROM standard_tile.forecast_day
WHERE postal_code='10128'
ORDER BY date_valid_std
LIMIT 7;
