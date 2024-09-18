# MySQLImportCSV

## Installation

```
pip install mysql-connector-python transliterate
```

Understood! You need to:

* Import all CSV files from the csv folder into one single table.
* The table name should be specified manually.
* The first field in the table should be id with PRIMARY KEY and AUTO_INCREMENT.
* If new fields are found in any CSV file that are not already in the table, the script should automatically add those fields to the table.
* The first row of each CSV file contains field names in Russian, which need to be transliterated into English.