# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
# Imports
from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime as dt
import time
from pytz import timezone
import os

def changeTimeZone(hist_date_tz):
    hist_date_tz.to_csv(temp, index=True)
    hist_temp = pd.read_csv(temp)
    
    Time = []
    eastern = timezone('US/Eastern')
    utc = timezone('UTC')
    for row in range(0,len(hist_temp['date'])):
        try:
            created_at=dt.strptime(hist_temp['date'][row],'%Y-%m-%d %H:%M:%S')
        except:
            created_at=dt.strptime(hist_temp['date'][row],'%Y-%m-%d')
        utc_created_at = utc.localize(created_at)
        est_created_at = utc_created_at.astimezone(eastern)
        Time.append(est_created_at)

    hist_date_tz['Time'] = Time
    hist_date_tz['date']=[str(i.year)+'-'+str(i.month).zfill(2)+'-'+str(i.day).zfill(2)+' '+str(i.hour).zfill(2)+':'+str(i.minute).zfill(2)+':'+str(i.second).zfill(2) for i in hist_date_tz['Time']]
    hist_date_tz = hist_date_tz.drop(columns=['Time'])
    hist_date_tz=hist_date_tz.append(hist_date_tz).reset_index(drop=True)
    os.remove(temp)
    return hist_date_tz

while True:
    try:
        a = dt.now()
        
        # Setup - Add setup values only over here. Will help in getting reinitialised each day!
        pytrends = TrendReq(hl = 'en-US')
        pytrends2 = TrendReq(hl = 'en-US')
        pytrends3 = TrendReq(hl = 'en-US')
        kw_list = ['super bowl 53']
        pytrends.build_payload(kw_list=kw_list, timeframe='today 1-m')
        pytrends2.build_payload(kw_list=kw_list, timeframe='today 1-m', geo='US')
        pytrends3.build_payload(kw_list = kw_list, timeframe = 'now 1-d', geo = 'US')
        df_rq_ = pd.DataFrame()
        temp = "Google_Trends/TemporaryFile.csv" #use this for any temporary csv creation and use os.remove(temp) in the respective def

        
        # Daily Data for One Month - Uses pytrends as it is set as 'today 1-m'
        IOT_Data_Daily = pytrends.interest_over_time()
        IOT_Data_Daily = IOT_Data_Daily.drop(columns = 'isPartial')
        IOT_Data_Daily = changeTimeZone(IOT_Data_Daily)
        IOT_Data_Daily.to_csv("Google_Trends/InterestDaily_GTrends.csv", header=False,mode='w',index=True)
        
#         Interest by State - Uses ptrends2 as it is set to 'geo='US''
        ibr_s_df = pytrends2.interest_by_region(resolution='REGION')
        ibr_s_df.to_csv("Google_Trends/InterestByState.csv",header=False,mode='w',index=True)
        
#         Interest by County
        ibr_dma = pytrends2.interest_by_region(resolution='DMA')
        ibr_dma.to_csv("Google_Trends/InterestByCounty.csv", header=False,mode='w',index = True)
        
#         Interest by City
        ibr_city = pytrends2.interest_by_region(resolution='CITY')
        ibr_city.to_csv("Google_Trends/InterestByCity.csv",header=False,mode='w', index = True)
        
#         Interest by Country - Uses pytrends as no geo tag restriction should be avoided for this
        ibr_Country = pytrends.interest_by_region(resolution='COUNTRY')
        ibr_Country.to_csv("Google_Trends/InterestByCountry.csv", header=False,mode='w',index = True)
        
        counter = 0
        while counter<=24:
            try:

#                 hourly historical data from Day 1 of current month
                d=dt.today()

                today_year = d.year
                today_month = d.month
                today_day = d.day

#                 hist_data = pytrends.get_historical_interest(kw_list, year_start=today_year, month_start=today_month, day_start= 1, year_end=today_year, month_end=today_month+1)
#                 hist_data = hist_data.drop(columns = 'isPartial')
#                 hist_data = changeTimeZone(hist_data)
#                 hist_data.to_csv("HourlyHistoricalData1Month.csv", index = True)
                
#                 Hourly Historical Data per day
                histData_day = pytrends3.interest_over_time()
                histData_day = histData_day.drop(columns = 'isPartial')
                histData_day = changeTimeZone(histData_day)
                histData_day.to_csv("Google_Trends/HourlyHistoricalData1Day.csv",header=False,mode='w', index=True)
                
#               Related Queries for 1 Hour
                rq_ = pytrends3.related_queries()
                q = 'query'
                v = 'value'
                for keys in rq_.keys():
                    df_rq_['Top Queries'] = rq_[keys]['top'][q]
                    df_rq_['Top Query Value '] = rq_[keys]['top'][v]
                    df_rq_['Rising Query'] = rq_[keys]['rising'][q]
                    df_rq_['Rising Query Value'] = rq_[keys]['rising'][v]
                df_rq_.to_csv("Google_Trends/RelatedQueries1Day.csv",header=False,mode='w', index=False)

#                 Related Topics over the last 1 Hour
                ts_ = pytrends3.related_topics()
                df_ts_ = pd.DataFrame(columns=['Title', 'Value', 'Mid'])
                for keys in ts_.keys():
                    df_ts_['Title'] = ts_[keys].title
                    df_ts_['Value'] = ts_[keys].value
                    df_ts_['Mid'] = ts_[keys].mid
                df_ts_.to_csv("Google_Trends/RelatedTopics1Day.csv",header=False,mode='w', index = False)
             
                counter+=1
                print("Hour", counter)

                time.sleep(3600)
                
            except Exception as e:
                print(e)
                time.sleep(300)
                continue
#         print("Daily Code")
    except Exception as e:
        print(e)
        time.sleep(300)
        continue
