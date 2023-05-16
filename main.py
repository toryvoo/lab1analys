import pandas
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import urllib
import urllib.request
from datetime import datetime
import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
import re


class Dataframes:

    def __init__(self, path):
        for n in range(1, 29):
            exec("self.df{} = None".format(n))
            exec("self.tdf{} = None".format(n))
        self.path = path
        self.merged = 0

    def downloadfiles(self):
        for i in range(1, 29):
            url = 'https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/get_TS_admin.php?country=UKR&provinceID={}&year1=1981&year2=2020&type=Mean'.format(i)
            wp = urllib.request.urlopen(url)
            text = wp.read()
            now = datetime.now()
            date_and_time_time = now.strftime("%d%m%Y%H%M%S")
            out = open('NOAA_ID' + str(i) + '_' + date_and_time_time + '.csv', 'wb')
            out.write(text)
            out.close()


    def readdata(self):
        global df24
        headers = ['Year', 'Week', 'SMN', 'SMT', 'VCI', 'TCI', 'VHI', 'empty']
        onlyfiles = [f for f in listdir(self.path) if isfile(join(self.path, f))]
        for n in range(1, 29):
            for i in onlyfiles:
                if i.find('NOAA_ID' + str(n) + '_') != -1:
                    exec("self.df{} = pd.read_csv(i, header=1, names=headers)".format(n))
                    exec("self.df{} = pd.read_csv(i, header=1, names=headers)".format(n))

    def changeindexes(self):
        self.tdf1 = self.df24
        self.tdf1['area'] = 1
        self.tdf2 = self.df25
        self.tdf2['area'] = 2
        self.tdf3 = self.df5
        self.tdf3['area'] = 3
        self.tdf4 = self.df6
        self.tdf4['area'] = 4
        self.tdf5 = self.df27
        self.tdf5['area'] = 5
        self.tdf6 = self.df23
        self.tdf6['area'] = 6
        self.tdf7 = self.df26
        self.tdf7['area'] = 7
        self.tdf8 = self.df7
        self.tdf8['area'] = 8
        self.tdf9 = self.df11
        self.tdf9['area'] = 9
        self.tdf10 = self.df13
        self.tdf10['area'] = 10
        self.tdf11 = self.df14
        self.tdf11['area'] = 11
        self.tdf12 = self.df15
        self.tdf12['area'] = 12
        self.tdf13 = self.df16
        self.tdf13['area'] = 13
        self.tdf14 = self.df17
        self.tdf14['area'] = 14
        self.tdf15 = self.df18
        self.tdf15['area'] = 15
        self.tdf16 = self.df19
        self.tdf16['area'] = 16
        self.tdf17 = self.df21
        self.tdf17['area'] = 17
        self.tdf18 = self.df22
        self.tdf18['area'] = 18
        self.tdf19 = self.df8
        self.tdf19['area'] = 19
        self.tdf20 = self.df9
        self.tdf20['area'] = 20
        self.tdf21 = self.df10
        self.tdf21['area'] = 21
        self.tdf22 = self.df1
        self.tdf22['area'] = 22
        self.tdf23 = self.df3
        self.tdf23['area'] = 23
        self.tdf24 = self.df2
        self.tdf24['area'] = 24
        self.tdf25 = self.df4
        self.tdf25['area'] = 25
        q = []
        for i in range(1, 26):
            exec('q.append(self.tdf{})'.format(i))
        self.merged = pd.concat(q)
        self.merged['Year'].astype(str)
        self.merged['empty'] = 'empty'
        self.merged.dropna(inplace=True)
        self.merged = self.merged.drop(self.merged.loc[self.merged['VHI'] == -1].index)
        self.merged.replace('<tt><pre>1982', '1982', regex=False, inplace=True)
        self.merged['Year'] = self.merged['Year'].astype(int)

    def yearandprovince(self, area, year):
        # print(self.merged[(self.merged["area"] == area) & (self.merged["Year"] == year)])
        # print(self.merged.loc[(self.merged['area'] == 6) & (self.merged['Year'] == 2020)])
        # print(self.merged.dtypes)
        # print(self.merged[self.merged['Year'].str.contains('1982')])
        print(self.merged[(self.merged["area"] == area) & (self.merged["Year"] == year)]['VHI'])
        print("Екстремуми:", end=' ')
        print(self.merged[(self.merged["area"] == area) & (self.merged["Year"] == year)]['VHI'].max(), end=' ')
        print(self.merged[(self.merged["area"] == area) & (self.merged["Year"] == year)]['VHI'].min())

    def allyearsanddroughts(self, area):
        print(self.merged[self.merged['area'] == area][['VHI', 'Year']])
        qq = self.merged[self.merged['area'] == area][['VHI', 'Year']]
        print(qq[(qq.VHI <= 15)])

    def allyearsanddroughts2(self, area):
        print(self.merged[self.merged['area'] == area][['VHI', 'Year']])
        qq = self.merged[self.merged['area'] == area][['VHI', 'Year']]
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(qq[(qq.VHI <= 30)])


if __name__ == '__main__':
    ddd = Dataframes('/Users/user/Desktop/temp')
    ddd.downloadfiles()
    ddd.readdata()
    ddd.changeindexes()
    ddd.yearandprovince(11, 2007)
    ddd.allyearsanddroughts2(7)

