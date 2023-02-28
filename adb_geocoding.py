
import os
import sys
import csv
import io
import pandas as pd
import geopandas as gpd
import requests
import urllib
import json
import time
import zipfile
import fiona


# read order csv(order_201xQx.csv)
csv_file_path = input("input your order.csv path here") # e.g. ../order_2012Q1.csv , seperate each order csv file into different folder to avoid overwrite
write_path = os.path.dirname(csv_file_path)
file_name = os.path.basename(csv_file_path)
data = pd.read_csv(csv_file_path)
df = data.copy()
df['product_id'] = df['product_id'].astype(int)
df['arrival_zip_code'] = df['arrival_zip_code'].astype(int)
df['redelivery_count'] = df['redelivery_count'].fillna(0)
df['redelivery_count'] = df['redelivery_count'].astype(int)
df['package_id'] = df['package_id'].astype(object)
df.head()
print("csv size: "+str(len(df)))


# # remove and count the consecutive duplicates:
# not using set to remove all duplicates because after geocoding, the long/lat information is added back to the csv, so we keep the order and the consecutive duplicates count

address_list=[]
duplicates_count_list=[]
first = True
duplicates_count=1
for a,b in zip(df['arrival_zip_code'], df['arrival_address']):
    address = str(a)+b
    if first:
        address_list.append(address)
        duplicates_count_list.append(1)
        first=False
    else:
        if address!=address_list[-1]:
            address_list.append(address)
            duplicates_count_list[-1]=duplicates_count
            duplicates_count_list.append(1)
            duplicates_count=1
        else:
            duplicates_count+=1
            
if address==address_list[-1]:
    duplicates_count_list[-1]=duplicates_count
    
print("addresses: "+str(len(address_list)))
print("sum of duplicate_count_list(should be equal to csv size): "+ str(sum(duplicates_count_list)))

# # remove redundant information(e.g. remove address after 號，since floor number etc does not affect geocoding results)
i=0
for each in address_list:
    if "號" in each: #only roughly 5000 has no "號"
        address_list[i]=each.split("號")[0]+"號"
    i+=1


# # Write addresses into text file for batch geocoding:

# check if geocoding batch result file already exist, if so, skip geocoding process,
# if you wish to redo the geocoding for update, delete the result file in the dir.
result1_file = []
if os.path.exists(os.path.join(write_path,"batch1")):
    result1_file = [each for each in os.listdir(os.path.join(write_path,"batch1")) if "result" in each and ".txt" in each] #check the latest result .txt

result2_file = []
if os.path.exists(os.path.join(write_path, "batch2")):
    result2_file = [each for each in os.listdir(os.path.join(write_path, "batch2")) if "result" in each and ".txt" in each]

if len(result1_file)==0 or (len(address_list) >= 1000000 and len(result2_file)==0):
    print("you don't have your geocoding result file in the directory yet, start geocoding process, if you already have a result file "+
          "and want to use that, please move it under the result folder and kill the execution and rerun")
    start_time = time.time()
    if len(address_list) >= 1000000: # if more than 1million addresses, we seperate them into 2 batches
        with open(os.path.join(write_path,"address_1.txt"), 'w', encoding="utf-8") as f1, open(os.path.join(write_path,"address_2.txt"), 'w') as f2:
        #     f.write(f"searchText,country\n")
            f1.write(f"recId|searchText|country\n")
            f2.write(f"recId|searchText|country\n")
            i=0
            #geocoding allows 1 million records at most
            for each in address_list[:999999]:
                i+=1
                f1.write(f"{i}|{each}|TWN\n")
            for each in address_list[999999:]:#everthing above 1000000th
                i+=1
                f2.write(f"{i}|{each}|TWN\n")
    else:
        with open(os.path.join(write_path,"address.txt"), 'w', encoding="utf-8") as f:
        #     f.write(f"searchText,country\n")
            f.write(f"recId|searchText|country\n")
            i=0
            for each in address_list:
                i+=1
                f.write(f"{i}|{each}|TWN\n")


    output = io.StringIO()
    output2 = io.StringIO()
    if len(address_list) >= 1000000:
        with open(os.path.join(write_path,"address_1.txt"), 'r', encoding="utf-8") as f1, open(os.path.join(write_path,"address_2.txt"), 'r') as f2:
            readCSV = csv.reader(f1)
            readCSV2 = csv.reader(f2)
            for row in readCSV:
                writer = csv.writer(output)
                writer.writerow(row)
            for row in readCSV2:
                writer = csv.writer(output2)
                writer.writerow(row)

    else:
        with open(os.path.join(write_path,"address.txt"), 'r', encoding="utf-8") as f:
            readCSV = csv.reader(f)
            for row in readCSV:
                writer = csv.writer(output)
                writer.writerow(row)

    data = output.getvalue()
    data2 = output2.getvalue()


    headers = {'Content-Type': 'text/plain; charset=utf-8'}
    #apiKey: get yours by registering here api account
    apiKey="5_2-PtL6gVbpibCFAh4cm7ROZoQfwgi08LDWHIKdt-0"
        #"PAR6gIZQYbyC1QNM-Al0DqPBNMbQjUEEdNeicAU_Fbc"
    #output file columns, locationLabel:normalized addresses generated by here geocoding api
    outcols="locationLabel,displayLatitude,displayLongitude"
    # ,houseNumber,street,district,city,postalCode,county,state,country"


    response = requests.post('https://batch.geocoder.ls.hereapi.com/6.2/jobs?apiKey='+apiKey+
                             '&indelim=%7C&outdelim=%7C&action=run&outcols='+outcols+'&outputcombined=false',
                             headers=headers, data=data.encode('utf-8'))
    print(response.content)


    if data2!="" and len(result2_file)==0:
        response2 = requests.post('https://batch.geocoder.ls.hereapi.com/6.2/jobs?apiKey='+apiKey+
                                 '&indelim=%7C&outdelim=%7C&action=run&outcols='+outcols+'&outputcombined=false',
                                 headers=headers, data=data2.encode('utf-8'))
        print(response2.content)


    # # Output geocoding, click the link to download the text file:
    # Large number of addresses: line will not be valid until batch job is finished on the server(1million of addresses takes around 1 hours)
    if len(result1_file)==0:
        requestId = response.text[response.text.index("<RequestId>")+11:response.text.index("</RequestId>")]
        download_url = "https://batch.geocoder.ls.hereapi.com/6.2/jobs/"+ requestId+ "/result?apiKey="+apiKey
        print("batch 1: "+download_url)

    if data2!="" and len(result2_file)==0:
        requestId2 = response2.text[response2.text.index("<RequestId>")+11:response2.text.index("</RequestId>")]
        download_url2 = "https://batch.geocoder.ls.hereapi.com/6.2/jobs/"+ requestId2+ "/result?apiKey="+apiKey
        print("batch 2: "+ download_url2)

        # download geocoded zip file to the path and unzip, need to wait for larger batch's url to be valid.
        r2 = requests.get(download_url2, stream=True)
        #batch2 might finish first because it's the second half chunk (much smaller, only few hundreds rows in our dataset), so we download it first
        while "404" in str(r2):
            print("batch 2:" + r2.content.decode())
            print("wait until batch job completes")
            time.sleep(10) # recheck the link every 10 secs
            r2 = requests.get(download_url2, stream=True)

        print("batch 2 job completed")
        z2 = zipfile.ZipFile(io.BytesIO(r2.content))
        z2.extractall(os.path.join(write_path,"batch2"))#  avoid overlap
        result2_file = [each for each in os.listdir(os.path.join(write_path, "batch2")) if "result" in each and ".txt" in each]  # there should be result file at this point

    if len(result1_file) == 0:
        r = requests.get(download_url, stream=True)
        while "404" in str(r):
            # since batch 1 is close to 1 million records, it takes around 30mins to 1h depending on the server's current workload, so we recheck every 10 mins
            print("batch 1:" + r.content.decode())
            print("wait until batch job completes(check every 5 mins)")
            for i in range(300):
                time.sleep(1)
            r = requests.get(download_url, stream=True)

        print("batch 1 job completed")
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(os.path.join(write_path,"batch1"))
        result1_file = [each for each in os.listdir(os.path.join(write_path,"batch1")) if "result" in each and ".txt" in each] # there should be result file at this point
    print(time.time()-start_time)
else:
    print("you already have your geocoding results file in place, skip geocoding api call, if you wish to redo geocoding process, delete the old result file and rerun")

# # Add lat/long to the original order_201X_qX.csv:
#roughly takes a min
print("Add lat/long to the original csv:")
start_time=time.time()
df["lat"]=""
df["long"]=""
df = df.drop(columns='arrival_address_normalized', errors='ignore')
df.insert(df.columns.get_loc("arrival_address")+1, "arrival_address_normalized","")
normalized_add_index = df.columns.get_loc("arrival_address_normalized")
#read the output geocode txt file
try:
    with open(os.path.join(write_path,"batch1",result1_file[-1]), 'r', encoding="utf-8") as f:
        i = 0
        batch=0
        next(f)
        recId_latest=0
        for row in f:
            row = row.split("|")
            recId = int(row[0])
            if recId_latest != recId: # if not repeat with previous
                recId_latest = recId
    #             print(i, recId-1)
                if i != recId-1:
                    batch+=duplicates_count_list[i]
                    i+=1
                lat_long = row[-2],row[-1].strip()
                normalized_add=row[-3]
                # print(i,lat_long)
                repeat_in_a_row = duplicates_count_list[i]
                df.iloc[batch:batch+repeat_in_a_row, -2:] =[lat_long]
                df.iloc[batch:batch+repeat_in_a_row, normalized_add_index] = normalized_add
                batch+=duplicates_count_list[i]

                i+=1
    #         else:
    #             print("repeat")

    if len(result2_file)!=0:
        with open(os.path.join(write_path,"batch2",result2_file[-1]), 'r', encoding="utf-8") as f:
            i = 999999 #continue from 1000000th record
    #         batch=0
            next(f)
            recId_latest=0
            for row in f:
                row = row.split("|")
                recId = int(row[0])
                if recId_latest != recId: # if not repeat with previous
                    recId_latest = recId
        #             print(i, recId-1)
                    if i != recId-1:
                        batch+=duplicates_count_list[i]
                        i+=1
                    lat_long = row[-2],row[-1].strip()
                    normalized_add=row[-3]
        #             print(i,lat_long)
                    repeat_in_a_row = duplicates_count_list[i]
                    df.iloc[batch:batch+repeat_in_a_row, -2:] =[lat_long]
                    df.iloc[batch:batch+repeat_in_a_row, normalized_add_index] = normalized_add
                    batch+=duplicates_count_list[i]

                    i+=1
        #         else:
        #             print("repeat")
    else:
        print("No second batch need to read")
except Exception as e:
    #if no second batch
    print(e)

df['long'] = pd.to_numeric(df['long'], errors='coerce')
df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
print(time.time()-start_time)

# # write to csv file(optional):
# geocoded_csv_path = os.path.join(write_path,file_name[:-4]+"_geocoded.csv")
# df.to_csv(geocoded_csv_path, encoding='utf-8', index=False)


# # Convert to Geopackage file:

#generate geometry datas using latitude and longtitude
# data = pd.read_csv(geocoded_csv_path,encoding="UTF-8")
print("generating geometry point from coordinates")
data_gdf = gpd.GeoDataFrame(df, geometry = gpd.points_from_xy(df['long'], df['lat']))

#takes 5 to 10 minutes
print("start generate gpkg file...")
start_time=time.time()
# shp file has size limit of 2gb, so use geopackage instead, faster when importing
data_gdf.to_file(os.path.join(write_path,file_name[:-4]+".gpkg") ,driver="GPKG",encoding="utf-8")
print(time.time()-start_time)

print("done, you can see your gpkg file in the folder")
#in cmd type :ogr2ogr -f PostgreSQL PG:"dbname='final_project' host='localhost' port='5432' user='postgres' password='1234'" path/order_2011Q1.gpkg
