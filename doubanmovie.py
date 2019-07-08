import requests
from lxml import etree
import time
import pymysql
import json

def getUrl(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
    }
    time.sleep(3)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        #print(response.text)
        return response.text
    return None
def parseurlContent(urlhtml):
    html = etree.HTML(urlhtml)
    type = html.xpath('//div[@id="info"]//span[@property="v:genre"]/text()')
    typeStr = ",".join(type)
    #print(typeStr)
    time = html.xpath('//div[@id="info"]//span[@property="v:initialReleaseDate"]/text()')
    timeStr = ",".join(time)
    #print(timeStr)
    return typeStr, timeStr
def getJson(offset):
    url = 'https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=%E7%94%B5%E5%BD%B1&start='+str(offset)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
    }
    time.sleep(2)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        #print(response.text)
        return response.text
    return None

def parseJson(Jsondata):
    dicts = json.loads(Jsondata)
    L = []
    for i in range(20):
        #print(dicts['data'][i]['directors'])#导演
        directorslist = dicts['data'][i]['directors']
        directorstr = ",".join(directorslist)
        #print(directorstr)
        #print(dicts['data'][i]['rate'])#评分
        rate = dicts['data'][i]['rate']
        #print(dicts['data'][i]['cover_x'])
        #print(dicts['data'][i]['star'])
        #print(dicts['data'][i]['title'])#名字
        title = dicts['data'][i]['title']
        #print(dicts['data'][i]['url'])#详情
        url = dicts['data'][i]['url']

        #print(dicts['data'][i]['casts'])#演员
        castlist = dicts['data'][i]['casts']
        castStr = ",".join(castlist)
        #print(",".join(castlist))
        #print(dicts['data'][i]['cover'])#封面
        cover = dicts['data'][i]['cover']
        #print(dicts['data'][i]['id'])
        movieid = dicts['data'][i]['id']

        urlContent = getUrl(url)
        #print(type(urlContent))
        #parseurlContent(urlContent)
        type = parseurlContent(urlContent)[0]
        time = parseurlContent(urlContent)[1]

        t = (title, cover, directorstr, rate, castStr, url, movieid, type, time)
        L.append(t)
    print(L)
    insertTable(L)
def connectMysql():
    db = pymysql.connect(host='localhost', user ='root', password='root', port=3306)
    cursor = db.cursor()
    cursor.execute('select version()')
    data = cursor.fetchall()
    print('Database version:',data)
    cursor.execute("create database doubanmovie1 default character set utf8")
    db.close()
def createTable():
    db = pymysql.connect(host='localhost', user='root', password='root', port=3306 , db='doubanmovie1')
    cursor = db.cursor()
    sql = 'create table if not exists movietable1 (id int not null primary key auto_increment,title varchar(255) not null,cover varchar(255) not null,directors varchar(255) not null,rate varchar(255) not null,casts varchar(255) not null,url varchar(255) not null,movieid varchar(255) not null,type varchar(255) not null,time varchar(255) not null)'
    cursor.execute(sql)
    print('Table create successfully')
    db.close()
def insertTable(List):
    db = pymysql.connect(host='localhost', user='root', password='root', port=3306, db='doubanmovie1')
    cursor = db.cursor()
    sql = 'insert into movietable1(title,cover,directors,rate,casts,url,movieid,type,time) value(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    try:
        cursor.executemany(sql, List)
        db.commit()
    except:
        db.rollback()
    db.close()
#connectMysql()连接数据库
#createTable()创建表
for i in range(500):#to 500
    if i==499:
        jsondata = getJson(9979)
        dicts = json.loads(jsondata)
        List = []
        for j in range(1,20):
            #print(dicts['data'][j]['directors'])  # 导演
            directorslist = dicts['data'][j]['directors']
            directorstr = ",".join(directorslist)
            #print(dicts['data'][j]['rate'])  # 评分
            rate = dicts['data'][j]['rate']
            # print(dicts['data'][i]['cover_x'])
            # print(dicts['data'][i]['star'])
            #print(dicts['data'][j]['title'])  # 名字
            title = dicts['data'][j]['title']
            #print(dicts['data'][j]['url'])  # 详情
            url = dicts['data'][j]['url']

            #print(dicts['data'][j]['casts'])  # 演员
            castlist = dicts['data'][j]['casts']
            castStr = ",".join(castlist)
            #print(dicts['data'][j]['cover'])  # 封面
            cover = dicts['data'][j]['cover']
            #print(dicts['data'][j]['id'])
            movieid = dicts['data'][j]['id']
            urlContent = getUrl(url)
            # print(type(urlContent))
            parseurlContent(urlContent)
            type = parseurlContent(urlContent)[0]
            time = parseurlContent(urlContent)[1]
            t = (title, cover, directorstr, rate, castStr, url, movieid, type, time)
            List.append(t)
        print(List)
        insertTable(List)
        break
    else:
        jsondata = getJson(i * 20)
        parseJson(jsondata)

