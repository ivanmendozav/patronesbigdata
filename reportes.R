#rm(list = ls())
library(dplyr)
library(tidyr)
library(lubridate)
library(tictoc)
library(dbscan)
library(rgdal)
library(OpenStreetMap)
library(ggplot2)
source("libraries.R")

tic()
#1. Load data
data <- getData(limit=100000,users=5) #call once
data %>% filter(accuracy<1000 & activity_type!="unknown") %>% select(device_model,battery_level,is_moving,speed,heading,activity_type,recorded_at,latitude,longitude,company_token) ->subdata
save(subdata,file="subdata.R")
#load(file="subdata.R") #800000 datos

#2. Track points stats
glimpse(subdata)
boxplot(speed~activity_type,subdata)

#3. Trip records stats (bounding box from Cuenca)
minlat <- -2.9736; maxlat <- -2.7946; minlon <- -79.0911; maxlon <- -78.7716
subdata %>% filter(latitude> minlat & latitude <  maxlat & longitude > minlon & longitude < maxlon) -> subdata#bounding box Cuenca
datapoints <- changecoordsystem(subdata,longlabel = "longitude", latlabel = "latitude", targetproj = "+init=epsg:31992" )
plotmap(minlon,maxlon,minlat,maxlat,datapoints[1:min(10000,nrow(datapoints)),],"+init=epsg:31992")

#4 Trip segmentation
alltrips <- segmentTrips(datapoints) #get trips 10 minutes < x < 1 hour
alltrips$date <- ymd_hms(alltrips$starttime) 
alltrips$wday <- wday(alltrips$date)
alltrips$hour <- hour(alltrips$endtime) #arrival hour
alltrips <- populateStayTime(alltrips)
alltrips %>% filter(ttime>300 & ttime<3600) -> alltrips
alltrips <- labelHomes(alltrips,fromhour = 18,minstay = 3600)
save(alltrips,file="alltrips.R")
write.csv(x = alltrips, file = "trips.csv",row.names = F)
#load(file="alltrips.R")
barplot(table(alltrips$wday))
barplot(table(alltrips$mode))


#5. stats for week day trips, from home, >5min and weekly (>=16 visits in semester)
alltrips %>% filter(wday >=2 & wday <= 6 & home=="Y") -> hometrips
alltrips %>% filter(wday >=2 & wday <= 6 & home!="Y") -> nohometrips
#tripdata %>% filter(mode %in% c("in_vehicle")) -> tripdata
par(mfrow=c(1,2))
hist(hour(nohometrips$endtime),breaks=c(0,6,12,18,24))
hist(hour(hometrips$endtime),breaks=c(0,6,12,18,24)) #only home trips
hist((nohometrips$stayindestination))
hist((hometrips$stayindestination)) #only home trips

par(mfrow=c(1,1))

tripdata$x = tripdata$dx; tripdata$y = tripdata$dy; 
plotmap(minlon,maxlon,minlat,maxlat,tripdata[sample(1:nrow(tripdata),100),],"+init=epsg:31992")

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