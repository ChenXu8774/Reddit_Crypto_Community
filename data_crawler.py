import pandas as pd
import requests
from datetime import datetime
import traceback
import time
import csv
import json
import warnings
from info import client_info

def write_csv_line(writer, obj, category, i):
    output_list = []
    output_list.append(i)
    output_list.append(obj['id'])
    output_list.append(datetime.fromtimestamp(obj['created_utc']).strftime("%Y-%m-%d"))
    output_list.append(obj['created_utc'])
    if category == 'submission':
        output_list.append(obj['title'])
    else:
        output_list.append(obj['body'])
    output_list.append(f"u/{obj['author']}")
    try:
        output_list.append(f"u/{obj['author_fullname']}")
    except:
        output_list.append("u/[deleted]")
    output_list.append(obj['author_created_utc'])
    output_list.append(f"https://www.reddit.com{obj['permalink']}")
    output_list.append(obj['score'])
    writer.writerow(output_list)


def data_download(output, data_category, url_base, output_format, start_datetime, end_datetime, convert_to_ascii):
    print(f"Saving to {output}")
    count = 0
    if output_format == "human" or output_format == "json":
        if convert_to_ascii:
            handle = open(output, 'w', encoding='ascii')
        else:
            handle = open(output, 'w', encoding='UTF-8')
    else:
        handle = open(output, 'w', encoding='UTF-8', newline='')
        writer = csv.writer(handle)

    previous_epoch = int(start_datetime.timestamp())
    break_out = False
    index = 1
    columns = ['index', 'id', 'day', 'utc', 'title', 'author','author_fullname','author_created_utc', 'url', 'score']
    writer.writerow(columns)
    while True:
        new_url = url_base + str(previous_epoch)
        json_text = requests.get(new_url, headers={'User-Agent': "user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36", 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'})
        time.sleep(3.5) 
        try:
            json_data = json_text.json()
        except json.decoder.JSONDecodeError:
            time.sleep(2)
            continue

        if 'data' not in json_data:
            break
        objects = json_data['data']
        if len(objects) == 0:
            break

        for object in objects:
            previous_epoch = object['created_utc'] - 1
            if end_datetime is not None and datetime.utcfromtimestamp(previous_epoch) < end_datetime:
                break_out = True
                break
            count += 1
            try:
                write_csv_line(writer, object, data_category, index)
                index += 1
            except Exception as err:
                print(f"Couldn't print object: https://www.reddit.com{object['permalink']}")
                print(traceback.format_exc())
        if break_out:
            break

        print(f"Saved {count} through {datetime.fromtimestamp(previous_epoch).strftime('%Y-%m-%d')}")

    print(f"Saved {count}")
    handle.close()


reddit_client_id = client_info['reddit_client_id']
reddit_client_secret = client_info['reddit_client_secret']

username = "" 
thread_id = ""
output_format = "csv"
start_time = datetime.strptime("06/01/2021", "%m/%d/%Y")
end_time = datetime.strptime("06/01/2020", "%m/%d/%Y")

convert_to_ascii = False  # don't touch this unless you know what you're doing
convert_thread_id_to_base_ten = True  # don't touch this unless you know what
filter_string = None

print(filter_string)
url_template = "https://api.pushshift.io/reddit/{}/search?limit=1000&{}&before="
#for subreddit in ["SHIBArmy", "dogecoin"]:
for subreddit in ["SHIBArmy"]:
    filters = []
    if username:
        filters.append(f"author={username}")
    if subreddit:
        filters.append(f"subreddit={subreddit}")
    if thread_id:
        if convert_thread_id_to_base_ten:
            filters.append(f"link_id={int(thread_id, 36)}")
        else:
            filters.append(f"link_id=t3_{thread_id}")
    filter_string = '&'.join(filters)
    for category in ['submission', 'comment']:   
        data_download(f"./{subreddit}_{category}.csv", category, url_template.format(category, filter_string), output_format, start_time, end_time, convert_to_ascii)

