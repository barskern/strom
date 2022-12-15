import logging

import requests
import requests_cache
import pandas as pd
import matplotlib.pyplot as plt
import pendulum
import seaborn as sns
from tqdm import tqdm

logger = logging.getLogger(__name__)

BASE_URL = "https://www.hvakosterstrommen.no/api/v1/prices/{aar}/{maaned:0>2}-{dag:0>2}_{prisomrade}.json"

requests_cache.install_cache("strom")

start_date = pendulum.datetime(2022, 11, 1)
end_date = start_date.end_of('month')

datas = []
date_range = pendulum.period(start_date, end_date).range('days')
for date in tqdm(date_range, unit='day'):
    url = BASE_URL.format(aar=date.year, maaned=date.month, dag=date.day, prisomrade="NO1")
    try:
        res = requests.get(str(url), json=True)
        data = res.json()
        datas.extend(data)
    except Exception as e:
        logger.error(f"Unable to download data for day '{date}': {e}")

df = pd.DataFrame.from_records(datas)
df['time_start'] = pd.to_datetime(df['time_start'])
df['time_end'] = pd.to_datetime(df['time_end'])

print(df.describe(include='all', datetime_is_numeric=True))

g = sns.relplot(df, x='time_start', y="NOK_per_kWh", kind='line')

plt.axhline(df['NOK_per_kWh'].mean(), label='mean')
print(df['NOK_per_kWh'].mean())

plt.show()
