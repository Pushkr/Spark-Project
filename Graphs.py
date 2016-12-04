from pyspark import SparkContext, SparkConf
import matplotlib.pyplot as plt
 

# create spark context
conf = SparkConf().setAppName("Craigslist_1.0: Data Analysis")

sc = SparkContext(conf=conf)

# read data from HDFS
nissanFiles = sc.textFile("craigslist/nissanrogue").cache()

hondaFiles = sc.textFile("craigslist/hondacrv").cache()

toyotaFiles = sc.textFile("craigslist/toyotarav4").cache()


# Data cleansing : remove duplicates, remove empty lines
nissanRDD = nissanFiles.distinct().filter(lambda line : len(line.strip()) > 0)  

hondaRDD = hondaFiles.distinct().filter(lambda line : len(line.strip()) > 0)  

toyotaRDD = toyotaFiles.distinct().filter(lambda line : len(line.strip()) > 0)  



nissanCnt = [] # List for total number of nissan rogue for sale 

hondaCnt = [] # List for total number of hondas CRVs for sale

toyotaCnt = [] # List for total number toyota RAV4s for sale

nissanByYear =[] # List to hold intermidiate rdds 

hondaByYear = []

toyotaByYear =[]

car_year =[] 


# process data for past ten years 
for year in range(2007,2017):
	
      # Check post title for mention of car year
      
      tempRdd1 = nissanRDD.filter(lambda line : str(year) in line.split(",")[0].lower())

      nissanByYear.append(tempRdd1)
      
      nissanCnt.append(tempRdd1.count())

      tempRdd2 = hondaRDD.filter(lambda line : str(year) in line.split(",")[0].lower())
      
      hondaByYear.append(tempRdd2)
      
      hondaCnt.append(tempRdd2.count())

      tempRdd3 = toyotaRDD.filter(lambda line : str(year) in line.split(",")[0].lower())
      
      toyotaByYear.append(tempRdd3)
      
      toyotaCnt.append(tempRdd3.count())

      car_year.append(year)


# create dictonary for each brand by year

nissanDict = dict(zip(car_year,nissanByYear))

hondaDict = dict(zip(car_year,hondaByYear))

toyotaDict = dict(zip(car_year,toyotaByYear))



# Plot data 
plt.plot(car_year,nissanCnt) 

plt.plot(car_year,hondaCnt)

plt.plot(car_year,toyotaCnt)

axes = plt.axes()

axes.set_xlim([2007,2017])

axes.set_ylim([0, 500])

axes.set_xticks([x for x in range(2007,2017)])

axes.set_yticks([x for x in range(0,500,50)])

plt.xlabel("Car Year")

plt.ylabel("Number of cars for sale")

plt.legend(["nissan rogue","Honda CRV","Toyota Rav4"])

plt.savefig("/home/cloudera/Desktop/image1.png")

sc.stop()
