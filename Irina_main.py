import requests as r
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from pandas.io.json import json_normalize
import config

import pathlib
from datetime import datetime
import pymysql




class UsersDataApi:

    def __init__(self, number):
        self.number = number


    def get_users_data_from_api(self):
        ''' Connecting to the randomuser.me API, retrieving the data and save it as DataFrame for latter use '''

        results = r.get(f"https://randomuser.me/api/?results={self.number}&seed=abc").json()["results"]
        json = json_normalize(results)
        df = pd.DataFrame(data=json)
        return df


class Singleton(object):
    ''' Creating singelton in order to create a "connection to database" class that will be a singelton '''

    _instance = None
    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance


class DbConnection(Singleton):
    ''' Database connection and engine creation- create only once since its a singelton '''

    username = config.username
    password = config.password
    host = config.host
    database = config.database

    def create_engine(self):
        engine=create_engine("mysql+pymysql://" + self.username + ":" + self.password + "@" + self.host + "/" + self.database)
        return engine

class DataPrep:
    ''' creating all tables and  relevant data to be exported to database or json files'''

    num_users=4500
    api_users = UsersDataApi(num_users)
    users = api_users.get_users_data_from_api()
    users['registered.date']= pd.to_datetime(users['registered.date'])
    users = users.rename(columns={"registered.date": "registered_date"})

    conn = DbConnection()
    db_conn = conn.create_engine()

    def male_female_split(self):
        male = self.users[self.users.gender == 'male']
        female = self.users[self.users.gender == 'female']
        return male, female

    def age_split(self):

        split_dfs = {}
        bins = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]
        labels = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
        self.users['age_group'] = pd.cut(self.users['dob.age'], bins=bins, labels=labels, right=False)
        for age_group in self.users['age_group'].unique():
            split_dfs[age_group] = self.users[self.users['age_group'] == age_group].drop('age_group', axis=1)
        return split_dfs

    def top_20(self):

        sql_query = """SELECT * FROM (SELECT * FROM interview.Irina_test_male ORDER BY registered_date DESC LIMIT 20) top_20_male UNION ALL SELECT
                                * FROM(SELECT * FROM interview.Irina_test_female ORDER BY registered_date DESC LIMIT 20) top_20_female; """

        query_data = pd.read_sql(sql_query, self.db_conn)
        return query_data

    def combine_20_5(self):

        sql_query = """SELECT * FROM (SELECT * FROM interview.Irina_test_20 test20 UNION SELECT * FROM interview.Irina_test_5 test5) combine_20_5; """

        query_data = pd.read_sql(sql_query, self.db_conn)
        return query_data

    def combine_20_2(self):

        sql_query = """SELECT * FROM (SELECT * FROM interview.Irina_test_20 test20 UNION ALL SELECT * FROM interview.Irina_test_2 test2) combine_20_2; """

        query_data = pd.read_sql(sql_query, self.db_conn)
        return query_data



class DbUploadData:
    ''' uploading tables to database'''

    conn = DbConnection()
    db_conn= conn.create_engine()

    table_names = 'Irina_test'

    def upload_table_male_female(self):
        with self.db_conn.connect() as connection:
            data=DataPrep()
            df_male, df_female = data.male_female_split()
            df_male.to_sql(con=connection, schema='interview', name=f'{self.table_names}_male',
                           if_exists='append',
                           index=False)
            df_female.to_sql(con=connection, schema='interview', name=f'{self.table_names}_female',
                             if_exists='append',
                             index=False)




    def upload_age_tables(self):

        with self.db_conn.connect() as connection:
            data=DataPrep()
            age_tables=data.age_split()
            for age, users in age_tables.items():
                table_name = f'{self.table_names}_{age}'
                users.to_sql(con=connection, schema='interview', name=table_name, if_exists='append', index=False)



    def upload_top_20(self):
        with self.db_conn.connect() as connection:
            data = DataPrep()
            query_data=data.top_20()
            table_name = f'{self.table_names}_20'
            query_data.to_sql(con=connection, schema='interview', name=table_name, if_exists='append',
                              index=False)






class CreateJsonFiles:
    ''' creating json files'''

    def create_first_json(self):
        data = DataPrep()
        query_data=data.combine_20_5()
        query_data.to_json(r'./output_json_data/first.json')

    def create_second_json(self):
        data = DataPrep()
        query_data=data.combine_20_2()
        query_data.to_json(r'./output_json_data/second.json')






DbUploadData().upload_table_male_female()
DbUploadData().upload_age_tables()
DbUploadData().upload_top_20()
CreateJsonFiles().create_first_json()
CreateJsonFiles().create_second_json()

