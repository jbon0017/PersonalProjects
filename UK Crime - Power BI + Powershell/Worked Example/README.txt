Here is a step-by-step guide of how this worked example has been approached:

1) Load this URL: https://data.police.uk/
2) Go to Data
3) From Custom Downloads, choose filters, in this case I have chosen the minimum (2017-06 to the maximum at the time 2020-05). I filtered on "Metropolitan Police Service" As Forces, and ticked the 3 available Datasets (i.e. crime data, outcomes data and stop-and-search data)
4) Download the dataset. This will save a CSV for each of the 3 datasets grouped in sub-directories by YYYY-MM.
5) Use the logic in the other file within the same folder, named "PowerShell Script", to loop through all sub-directories downloaded, and merge the crime data dataset (named "YYYY-MM_metropolitan-street.csv") to only one output file.
6) Load the output file in Power BI for further modelling and transformation.
