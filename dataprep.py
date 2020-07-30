import os
import glob
import concurrent.futures
import urllib
import urllib.request
import json
import base64
import pandas as pd
import config.local as local_config
from requests.auth import HTTPBasicAuth

GITHUB_REPO_PREFIX = 'https://api.github.com/repos/'

__location__ = os.path.realpath(os.path.join(
    os.getcwd(), os.path.dirname(__file__)))  # 1. /Users/qiandan/pythonProject/imlproject

# # 2. set resources as working directory
os.chdir("/Users/qiandan/pythonProject/imlproject/resources")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
print(all_filenames)


# # 1. combine two files
# combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
# export to csv
combined_csv.to_csv("combined_20200101_20200401.csv",
                    index=False, encoding='utf-8-sig')


# 2. add missing columns: stars, folks, watches
df = pd.read_csv("combined_20200101_20200401.csv")
urls = [repo_name for repo_name in [
    content for label, content in df.items()][0]]

URLs = urls[15000:20000]
json_list = []
print("start: " + str(len(json_list)))


def load_url(url, timeout):
    with requests.get(GITHUB_REPO_PREFIX+url,
                      auth=HTTPBasicAuth(local_config.git_auth.username, ocal_config.git_auth.pw)) as res:
        return json.loads(res.content)


with open('10000-15000.json', 'a+', encoding='utf-8') as f:
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(
            load_url, url, 60): url for url in URLs}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                json_list.append(
                    {"repo_name": data['full_name'], "watch": data['subscribers_count'], "fork": data['forks_count'], "star": data['stargazers_count']})
            except Exception as exc:
                json.dump(json_list, f, ensure_ascii=False, indent=4)
                json_list = []
                print('%r generated an exception: %s' % (url, exc))

    json.dump(json_list, f, ensure_ascii=False, indent=4)


# json to csv
df = pd.read_json(r'10000-15000.json')
df.to_csv(r'10000-15000.csv', index=None)

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
print(all_filenames)


# 1. combine two files
# combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
# export to csv
combined_csv.to_csv("combined.csv",
                    index=False, encoding='utf-8-sig')
