import requests
from bs4 import BeautifulSoup

# 1. 发送请求获取网页内容
url = 'https://ehall.xjtu.edu.cn/jwapp/sys/kcbcx/*default/index.do?amp_sec_version_=1&gid_=QkFSMHA0V2tVc1FHMm4rZHpXNUtlRFptSFdPV21rMjdKQ1ZlRmg3SGhRU1pCRCsvcC9kUjQ5NjEvaWpCUDg5cFA3anJCbFlrQlVnQ1E0ZXZsTjR3MlE9PQ&EMAP_LANG=zh&THEME=indigo#/qxkcb'
response = requests.get(url)
# 确认请求成功
if response.status_code == 200:
    web_content = response.text
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
    exit()

# 2. 解析网页内容
soup = BeautifulSoup(web_content, 'html.parser')
with open('output.html', 'w', encoding='utf-8') as file:
    file.write(web_content)
# 3. 提取所需的数据（例如，提取所有标题为 <h1> 的文本）


# 4. 将数据保存到文件（例如，保存到 CSV 文件）
# import csv

# with open('titles.csv', 'w', newline='') as csvfile:
#     csvwriter = csv.writer(csvfile)
#     csvwriter.writerow(['Title'])
#     for title in titles:
#         csvwriter.writerow([title.text])