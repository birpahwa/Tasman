# Tasman
ProjectForTasman

#The data set:
● The USA Jobs reporting database (usajobs.gov) is a very extensive repository of all job
openings in the US government.

● It has a big, well-documented RESTful API that is explained here:
https://developer.usajobs.gov/API-Reference

#Task:
1. Connect to the API with the help of request library and authenticate to the api and get the required JSON Object 
2. Create serverless DB in Sqlite and create Tables as required.
3. Parse the JSON object to get the data and load it in the respective Tables.
4. Perform queries on the tables to get the required Analysis.
5. Export result of analysis into the csv files
6. Send the CSV files attached in email to the required recipient's email
7. Schedule the python script to run daily midnight using Crontab using (0 0 * * * python main.py)