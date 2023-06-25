#rm(list = ls())
library(dplyr)
library(tidyr)
library(lubridate)
library(tictoc)
library(dbscan)
library(rgdal)
library(OpenStreetMap)
library(ggplot2)
library(gridExtra)
library(sqldf)
source("libraries.R")

tic()
#1. Load data
data <- getRemoteData(users=10) 
#data <- getData(limit=1000000,users=10) #call once
#data <- getDataByUser(limit=1000000,userid = "imendoza") 
#data <- getDataByRemoteUser(limit=1000000,userid = "galvarezmci",deviceid = "iPhone15-3-16-5") #call once

subdata <- data %>% filter(accuracy<1000)
#data %>% filter(accuracy<1000 & activity_type!="unknown") %>% select(device_model,battery_level,is_moving,speed,heading,activity_type,recorded_at,latitude,longitude,company_token) ->subdata
save(subdata,file="subdata10users.R")
#load(file="subdata.R") #800000 datos
write.csv(data,"10newuserspoints.csv",row.names = FALSE)


#2. Track points stats
glimpse(subdata)
boxplot(speed~activity_type,subdata)

#3. Trip records stats (bounding box from Cuenca)
minlat <- -2.9736; maxlat <- -2.7946; minlon <- -79.0911; maxlon <- -78.7716
#subdata %>% filter(latitude> minlat & latitude <  maxlat & longitude > minlon & longitude < maxlon) -> subdata#bounding box Cuenca
datapoints <- changecoordsystem(subdata,longlabel = "longitude", latlabel = "latitude", targetproj = "+init=epsg:31992" )
plotmap(minlon,maxlon,minlat,maxlat,datapoints[1:min(10000,nrow(datapoints)),],"+init=epsg:31992")

#4 Trip segmentation
alltrips <- segmentTrips2(datapoints) #get trips 10 minutes < x < 1 hour
backuptrips <- alltrips
alltrips <- alltrips[alltrips$tdistance>100,]
alltrips$date <- ymd_hms(alltrips$starttime) 
alltrips$adate <- ymd_hms(alltrips$endtime) 
alltrips$wday <- wday(alltrips$date)
alltrips$hour <- hour(alltrips$endtime) #arrival hour
#alltrips$day <- format(ymd_hms(alltrips$date),'%Y-%m-%d')
alltrips$day <- format(ymd_hms(alltrips$adate),'%Y-%m-%d')
alltrips <- populateStayTime(alltrips)
alltrips <- labelHomes2(alltrips)
save(alltrips,file="10newusertrips.R")
write.csv(x = alltrips, file = "10newusertrips.csv",row.names = F)
#load(file="alltrips.R")
barplot(table(alltrips$wday))
barplot(table(alltrips$mode))


#5. stats for week day trips, from home, >5min and weekly (>=16 visits in semester)
alltrips %>% filter(home=="Y") -> hometrips
#alltrips %>% filter(wday >=2 & wday <= 6 & home!="Y") -> nohometrips
#tripdata %>% filter(mode %in% c("in_vehicle")) -> tripdata
# par(mfrow=c(1,2))
# hist(hour(nohometrips$endtime),breaks=c(0,6,12,18,24))
# hist(hour(hometrips$endtime)) #only home trips
# hist((nohometrips$stayindestination))
# hist((hometrips$stayindestination)) #only home trips

g1 <- ggplot(alltrips[alltrips$stayindestination<10000,], aes(x=stayindestination, fill=home) )+ geom_density(alpha=0.4)
g2 <- ggplot(alltrips, aes(x=hour(endtime), fill=home) )+ geom_density(alpha=0.4)
g3 <- ggplot(alltrips, aes(x=dtrips, fill=home) )+ geom_density(alpha=0.4)
grid.arrange(g1,g2,g3,ncol=2, nrow=2)

#plot homes
par(mfrow=c(1,1))
#nighttrips <- alltrips[alltrips$hour>=0 & alltrips$hour<=5,]

tripdata = data.frame(x =hometrips$dx, y=hometrips$dy)

plotmap(minlon,maxlon,minlat,maxlat,tripdata,"+init=epsg:31992")

#c(mean(hometrips$dlat),mean(hometrips$dlong))

#6. Encontrar zonas densas del mapa de calor
#load(file="pois.R")
data <- getAttractors(tripdata, eps=100, minPts=2)
tripdata <- data[[1]]
data[[2]] %>% filter(users>1) -> centroids #at least 2 users
plotmap(minlon,maxlon,minlat,maxlat,centroids,"+init=epsg:31992")
save(data,file="pois.R")
write.csv(x = centroids, file = "pois.csv",row.names = F)
toc()
#1142.73 sec con 800000 datos

#Contestar Preguntas de Investigación
#7. Especificar atractores para una hora en particular
# tripdata %>% filter(hour == 21) -> hourlytrips
# centroids %>% filter(cluster %in% as.numeric(unique(hourlytrips$cluster))) -> hourlycentroids
# plotmap(minlon,maxlon,minlat,maxlat,hourlycentroids,"+init=epsg:31992")

#Para TICEC, calcular tiempo de permanencia en destino, filtrar viajes con corta pemanencia
#Verificar que hogares son lugares donde se sale de mañana y se llega de noche.

#DESPUES: Buscar zonas atractoras de estudiantes para planificar transporte público universitario.
#Completar viajes en base a rutinas de cada persona (viajes repertitivos que permitan ver la trayectoria)