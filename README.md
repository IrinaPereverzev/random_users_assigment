# random users database

#About the project

1. Random user generator was made by the open-source API, randomuser.me.
2. Data base on 4500 random users was created
3. From the core database, 2 new databases were created, splited by gender.
4. From the core database, 10 new databases were created, splited by age group.
5. New databse created thatstored the top 20 last registered males and females form each
one of gender tables
6. New dataset created that combines data from top 20 users table and age group 5 table- no duplicates. 
   The data stored in json file, in the output json data folder.
7. New dataset created that combines data from top 20 users table and age group 2 table- with duplicates. 
   The data stored in json file, in the output json data folder.


#Project Files

The project consists from 2 python files:

1. Irina_main

- class UsersDataApi: fetches the random users data and stores it as DataFrame

- class Singleton: a base singelton class

- class DbConnection(Singleton): connection to the MySql DB, inherits from singelton class, since we want to connect only one time, and use this connection thorought     all functions. 

- class DataPrep: Preparing the data from all queries and data manipulation before uplading the wanted information to the DB

  - def male_female_split(): splits the dataset into 2 seperate dataframes- male and female
   -def age_split(): splits the dataset into 10 seperate dataframes- based on the age group
   -def top_20(): finds the top 20 users from each gender table stored in the database, using Sql query  
   -def combine_20_5(): combining data from 2 tables in the database, top 20 users and age group 5, using Sql query
   -def combine_20_2():  combining data from 2 tables in the database, top 20 users and age group 2, using Sql query


- class DbUploadData: Uploading the data that was created in the DataPrep class to the MySql database.

  -def upload_table_male_female(): Uploading 2 tables, male and female
  -def upload_age_tables(): Uploading 10 tables, devided by age group
  -def upload_top_20(): Uploading the top 20 last registered males and females
    
        
        
         
- class CreateJsonFiles: Creates json files, stored in output_json_data folder

    -def create_first_json():  New dataset created that combines data from top 20 users table and age group 5 table
    -def create_second_json():  New dataset created that combines data from top 20 users table and age group 2 table
    



#Built With
Python
MySql workbanch


