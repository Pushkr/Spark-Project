from pyspark import SparkContext, SparkConf
import sys
import requests
from requests.exceptions import ChunkedEncodingError
from bs4 import BeautifulSoup
from urllib.parse import urlparse
# import urlparse
from pyspark.sql import HiveContext, Row

url = "https://newyork.craigslist.org/search/sss?sort=rel&query=nissan+2016"


def pagefetcher(temp_url):
    temp_rows = []
    if temp_url is not None:
        try:
            webdata_full = requests.get(temp_url)
            soup = BeautifulSoup(webdata_full.text, "html.parser")
            result1 = soup.find("div", {"class": "content"})
            temp_rows = result1.find_all("li", {"class": "result-row"})

        except (ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError) as err:
            print("\nError connecting site : {0} \n".format(err))
            return None
        except ChunkedEncodingError:
            print("\nSite response delayed, skipping retrieval..: {0}".format(sys.exc_info()[0]))
            return None
        finally:
            if len(temp_rows) > 0:
                return temp_rows
            else:
                return None


def rowfetcher(row):
    id_url = row.find("a", {"class": "hdrlnk"})
    lurl = (id_url.get("href"))
    posting = urlparse(lurl)
    if posting.netloc == '':
        post_url = urlparse(url).scheme + "://" + urlparse(url).netloc + posting.path
    else:
        post_url = urlparse(url).scheme + "://" + posting.netloc + posting.path

    title = id_url.text if id_url is not None else "No Title"

    span = row.find("span", {"class": "result-price"})
    price = (span.text if span is not None else "Not Listed")

    # Retrieve post details
    try:
        post_data = requests.get(post_url)
        post_soup = BeautifulSoup(post_data.text, "html.parser")
        pspan = post_soup.find("span", {"class": "postingtitletext"})
        pbody = post_soup.find("section", {"id": "postingbody"})
    except (ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError) as err:
        print("\nError connecting site : {0} \n".format(err))
        return
    except ChunkedEncodingError:
        print("\nSite response delayed, skipping retrieval..: {0}".format(sys.exc_info()[0]))

    # Find location of posting
    location = "Not Listed"
    try:
        location = pspan.small.text if pspan is not None else "Not Listed"
    except AttributeError:
        pass

    # Description of post
    body_text = pbody.text if pbody is not None else "Not Listed"

    pbody = post_soup.find_all("p", {"class": "postinginfo"})

    post_time, upd_time = ["N/A", "N/A"]
    try:
        if pbody[2].find("time", {"class": "timeago"}) is not None:
            post_time = (pbody[2].find("time", {"class": "timeago"}))['datetime'].split("T")
        if pbody[3].find("time", {"class": "timeago"}) is not None:
            upd_time = (pbody[3].find("time", {"class": "timeago"}))['datetime'].split("T")
    except:
        pass

    items = [title, post_url, price, location, post_time[0], post_time[1][:-5],
             upd_time[0], upd_time[1][:-5], body_text.replace("\n", "##")]
    items = [x.strip().replace("\n"," ") for x in items]

    return items


def main():
    # setup SparkContext
    conf = SparkConf().setAppName("Craigslist_1.0")
    sc = SparkContext(conf=conf)
    hc = HiveContext(sc)
    s_page = "&s="

    totalCount = 0
    pages = {}

    try:
        # make http request to get search result from craiglist
        webdata = requests.get(url)
        # use soup
        soup = BeautifulSoup(webdata.text, "html.parser")
        # find number of search items returned
        pages = soup.find("span", {"class": "button pagenum"})

    except IOError:
        print("\n Unexpected error : {0}".format(sys.exc_info()[0]))
        return
    except:
        print("\nError connecting site")
        return
    

    if pages is not None and pages.text != 'no results':
        totalCount = int(pages.find("span", {"class": "totalcount"}).text)
    else:
        totalCount = 0
            
    urlList = []

    print ("\n\n\n\n found {} results \n\n\n\n".format(totalCount))

    if totalCount ==0 :
        print(soup)


    if 0 < totalCount < 100 :
        urlList.append((url + s_page))
    elif totalCount >= 100 :
        for i in range(0, totalCount // 100):
            urlList.append((url + s_page + str(i * 100)))

    if totalCount > 0:        
        # Parallelize collection
        urlListRDD = sc.parallelize(urlList)

        # Collect posting abstract from each result page using map
        pagesRDD = urlListRDD.flatMap(pagefetcher).filter(lambda row: row is not None)
        print("Found total : %d" % (pagesRDD.count()))

        # Retrieve each post's data
        postsRDD = pagesRDD.map(rowfetcher)

        #Save data with \001 as field delimiter 
        postsRDD.map(lambda row: "\001".join(row)).saveAsTextFile("craigslist/rawdata")

        avgPrice = postsRDD.filter(lambda row : row[2] != "Not Listed") \
                    .map(lambda row: (int(row[2].strip("$").strip(",")))) \
                    .aggregate((0.0,0),lambda x,y: (x[0]+y,x[1]+1),lambda x,y: (x[0]+y[0],x[1]+y[1])) 

        print("average price is {}".format(avgPrice[0]/avgPrice[1]))

        wordCount= postsRDD.flatMap(lambda row:row[8].replace("##","\n")). \
            map(lambda row : row.split(" ")). \
            map(lambda word: (word,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: -x[1])

        wordCount.saveAsTextFile("craigslist/wordcount/nissan2015")

    else:
        pass

    sc.stop()


# if __name__ = '__main__':
main()




