### Analysis of compact segament SUVs 
Aim of this project is to - 
- analyse selling trends of compact sengment SUVs based on data collected from craigslist.com
- to demostrate data processing capabilities of Apache Spark using python
- Visiually represent the data
- develop a prediction model to guess prices



#### Data Collection :
 
`Disclaimer`: Data collected from craigslist in this project is by no means is a 'Big Data' but since the comodity hardware on which this project is built is of limited capability and resources, this collected data is used as just a sliced representation of actual data which might be available on craigslist servers.

`DataCollector.py` is entry point of this project. This code essentially contains lot of reused code from  a [my own web scraper](https://github.com/Pushkr/CraigslistToCsv) which essentially started as fun project but which later turned in to 
a complete API development work. 

This module starts with sending a single `request` to craigslist server along with search string. The response from server contains the number of craigslist posts currently available for the search string i.e `totalCount`.

`totalCount` is good indication of how many page requests that need to be sent to collect all postings. A typical page contains 100 posts. A parallelize collection of URLs is created based on `totalCount` and `pagefetcher` method uses this RDD to collect page data using `flatMap()`

Another method `rowfetcher` uses the RDD created in previous step to send thousands of http requests to gather data from each individual craigslist post. 

#### Data cleansing & transformation :
Data is striped of empty spaces, and all the new lines characters in `description` fields are replaced by dummy characters.
All the fields are joined by unicode null character ("\001")

Ultimately data is dumped in HDFS using `saveAsTextFile()`.

#### Loading data in hive metastore:
`dbloader.hql` is simple HiveQL script which loads the data collected in previous steps into `craigslist` database.
Tables are created each time a new type of search item is encountered. 
This tables are partitioned based on the date and month when the craigslist post was added.


Sample results :
Data collection APIs were ran on three most popular brands and model of compact SUVs (see code : `Graph.py`) - 

1. Honda CRV
2. Toyota Rav4
3. Nissan Rogue

Data collected in this stage is separated by year and model for detailed analysis. 

![ScreenShot](https://github.com/Pushkr/Spark-Project/blob/master/images/image1.png)

Following observations can be made from this graph: 

1. Graph line for honda brand is at relatively lower position i.e. mostly under 100. This is indication that people generally hold on to hondas rather than sell it.

2. On the other hand, there are overall higher number of nissan suvs are for resale. 

3. For both nissan and toyota, most number of cars available today (as of december 2016) are between 2012 - 2014 models. This is approximately 3 years old models that are getting resold. Three year is a typical lease aggreement, so this seems to indicate that the people who buy/lease nissan and toyota are more likely return their vehicle to dealer rather than keep it.

4. Highest number of hondas avaiable are from year 2008, this seems to reconfirm point #1 and indicates longer ownership period for hondas

5. sharp incline at the begining for nissan is due to the fact that nissan rogue came into market in 2007 while toyota and Hondas were in this segment since 1994. 


Future enhancements/work in progress :

- [ ] plot price graphs
- [ ] TBD
