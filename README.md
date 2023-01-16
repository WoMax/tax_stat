# tax_stat

`tax_stat` calculates statistics about the tax income of a country.

Features:
- Output the overall amount of taxes collected per state
- Output the average amount of taxes collected per state
- Output the average county tax rate per state
- Output the average tax rate of the country 
- Output the collected overall taxes of the country

### Requirements:
```
$ pip install -r requirements.txt
```
The application can use the following data sources: CSV file or sqllite database. See settings in `config.ini`.

CSV file or sqllite table should contain the following columns (case insensitive):
- State \<string\>
- County \<string\>
- Tax Rate \<int or float\>
- Tax amount \<int or float\>

### Arguments:
``` 
  -h, --help                        # optional
  --source_path SOURCE_PATH         # required
  --table_name TABLE_NAME           # required in case of sqllite data source
  --get_amount_taxes_per_state      # optional
  --get_average_taxes_per_state     # optional
  --get_average_tax_rate_per_state  # optional
  --get_average_country_tax_rate    # optional
  --get_country_tax_amount          # optional
```

### Example of usage:
```
$ python tax_stat.py --source_path tests/sources/source_data.csv --get_amount_taxes_per_state --get_country_tax_amount
Amount taxes per state:
         tax amount
state              
State_1    120000.0
State_2    115000.0
State_3    108000.0
State_4     32000.0
State_5    116000.0

Country tax amount:
491000.0
```

### Tests
```
$ coverage run unittests
$ coverage report -m
```
