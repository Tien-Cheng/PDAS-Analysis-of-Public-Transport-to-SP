# bus_arrival.py
# The purpose of this script is to obtain bus arrival data for bus stops near Singapore Polytechnic. 
import pandas as pd 
import requests
import schedule # task scheduler
import time
import pytz
LTA_API_KEY = "iIjmlFM/RRGXIjsjY5KTnA=="
DataFrame = pd.core.frame.DataFrame
bus_stops = {
    "Dover MRT" : 19039,
    "Alongside Commonwealth Ave" : 19041
}
tz = pytz.timezone("Asia/Singapore")
def getArrivalInformation(stop_code: int, token : str) -> DataFrame:
    try:
        req = requests.get("http://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2", params = {
            "BusStopCode" : stop_code
        }, headers = { "AccountKey" : token })
        print(req)
        req.raise_for_status()
        data = req.json()
        df = pd.json_normalize(data, "Services") # flatten json to dataframe
        df["Timestamp"] = pd.Timestamp.now(tz).round("min")
        df["Bus Stop Code"] = stop_code
        return df
    except Exception as e:
        print(e)
        print(f"Sorry! There was some issue getting data at {pd.Timestamp.now(tz).round('30min')}")
        return None
def job():
    print("job running")
    output_df = None # declare final output
    for code in bus_stops.values(): # for each bus stop code
        data = getArrivalInformation(code, LTA_API_KEY) # get arrival information
        if data is None: # if info is unavailable (error)
              output_df = None # then final output is nothing
              pass
        if output_df is None: 
            output_df = data # dataframe or none 
        else:
            output_df = output_df.append(data, ignore_index = True) # append 
    if output_df is not None:
        output_df.to_csv(f"./bus_arrival/bus_arrival_{pd.Timestamp.now(tz)}.csv".replace(":", "-"))
    else: 
        print("Sorry! We could not create csv file")
schedule.every(30).minutes.do(job)
job()
while True:
    schedule.run_pending()
    time.sleep(1)
    

