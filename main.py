from pandas import json_normalize
import urllib.request
import pandas as pd
import logging
import glob
import time
import zlib
import json
import ssl
import os

class Weather():
    def download_weather_data(self):
        """Returns a data frame object with the last
        weather data of all locations and saves it"""
        logging.log(level=20,msg="Starting downloading weather data")
        tries=0
        while tries<5:
            try:
                context = ssl._create_unverified_context()
                download_file=urllib.request.urlopen("https://smn.conagua.gob.mx/webservices/?method=3",context=context) 
                weather_data=zlib.decompress(download_file.read(), 16+zlib.MAX_WBITS)
                break
            except Exception as e:
                logging.log(level=40,msg=e)
                tries+=1
                time.sleep(60*5)
        if tries==5:
            raise Exception("Time out for connection")
        df = json_normalize(json.loads(weather_data))
        df.to_csv("~/airflow/dags/data_archive/"+time.strftime("%Y%m%d-%H%M%S")+".csv")
        logging.log(level=20,msg="New record saved")
        return df

    def construct_average_temp_df(self):
        """Returns a dataframe object with the 
        average temperature of the last two hours"""
        logging.log(level=20,msg="Building average temp dataframe")
        try:
            newest = max(glob.iglob('data_archive/*.csv'), key=os.path.getctime)
        except Exception as e:
            logging.log(level=40,msg=e)
        last_df=pd.read_csv(newest)
        new_df=self.download_weather_data()
        temp_avg=(last_df["temp"]+new_df["temp"].astype(float))/2
        new_df["temp_avg"]=temp_avg
        new_df.to_csv("~/airflow/dags/data_avg/"+time.strftime("%Y%m%d-%H%M%S")+"_avg.csv")
        return new_df

    def join_municipal_table(self):
        """Joins the municipal table data 
        with the most updated weather table"""
        logging.log(level=20,msg="Joining municipal table")
        try:
            newest = max(glob.iglob('data_archive/*.csv'), key=os.path.getctime)
        except Exception as e:
            logging.log(level=40,msg=e)
        new_df=pd.read_csv(newest)
        municipal_data_1=pd.read_csv('data_municipios/20220501/data.csv')
        municipal_data_2=pd.read_csv('data_municipios/20220503/data.csv')
        merged_municipal_data=pd.merge(municipal_data_1,municipal_data_2,how="inner",on=["Cve_Ent","Cve_Mun"])
        df_weather_merged=pd.merge(new_df,merged_municipal_data,how="inner",left_on=["ides","idmun"],right_on=["Cve_Ent","Cve_Mun"])
        df_weather_merged.drop(["Cve_Ent","Cve_Mun"],axis = 1,inplace=True)
        df_weather_merged.to_csv(("~/airflow/dags/current/"+time.strftime("%Y%m%d-%H%M%S")+"_merged.csv"))
        return df_weather_merged


def Run():
    w=Weather()
    w.construct_average_temp_df()
    w.join_municipal_table()
     
if __name__=="__main__":
    Run()