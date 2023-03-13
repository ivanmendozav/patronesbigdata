#rm(list = ls())
library(dplyr)
library(lubridate)
library(tictoc)
library(dbscan)
library(rgdal)
library(OpenStreetMap)
library(ggplot2)
source("libraries.R")

tic()
#1. Load data
data <- getData(limit=1000000) #call once
data %>% filter(accuracy<1000 & activity_type!="unknown") %>% select(device_model,event,battery_level,is_moving,speed,heading,activity_type,recorded_at,latitude,longitude,company_token) ->subdata
save(subdata,file="subdata.R")
#load(file="subdata.R") #800000 datos

#2. Track points stats
glimpse(subdata)
boxplot(speed~activity_type,subdata)

#3. Trip records stats (bounding box from Cuenca)
minlat <- -2.9736; maxlat <- -2.7946; minlon <- -79.0911; maxlon <- -78.7716
subdata %>% filter(latitude> minlat & latitude <  maxlat & longitude > minlon & longitude < maxlon) -> subdata#bounding box Cuenca
datapoints <- changecoordsystem(subdata,longlabel = "longitude", latlabel = "latitude", targetproj = "+init=epsg:31992" )
plotmap(minlon,maxlon,minlat,maxlat,datapoints[1:10000,],"+init=epsg:31992")

#load(file="tripdata.R")
tripdata <- segmentTrips(datapoints, minttime = 600, maxtime = 3600) #get trips 10 minutes < x < 1 hour
tripdata$date <- ymd_hms(tripdata$starttime) 
tripdata$wday <- wday(tripdata$date)
tripdata$hour <- hour(tripdata$date)
tripdata <- labelHomes(tripdata)
save(tripdata,file="tripdata.R")
prop.table(table(tripdata$wday))

#stats for week day trips
tripdata %>% filter(wday == 6 & hour>20 & home=="N") -> tripdata
tripdata %>% filter(mode %in% c("on_bicycle","in_vehicle","walking")) -> tripdata
hist(tripdata$ttime)
table(tripdata$mode)
boxplot(ttime ~ mode, tripdata)
hist(tripdata$tdistance)
boxplot(tdistance ~ mode, tripdata)
tripdata$x = tripdata$dx; tripdata$y = tripdata$dy; 
plotmap(minlon,maxlon,minlat,maxlat,tripdata[sample(1:nrow(tripdata),10000),],"+init=epsg:31992")

#4. Encontrar zonas densas del mapa de calor
#load(file="pois.R")
data <- getAttractors(tripdata, eps=22, minPts=3)
tripdata <- data[[1]]
centroids <- data[[2]]
plotmap(minlon,maxlon,minlat,maxlat,centroids,"+init=epsg:4326")
save(data,file="pois.R")
toc()
#1142.73 sec con 800000 datos

#5. Especificar atractores para una hora en particular
tripdata %>% filter(hour == 21) -> hourlytrips
centroids %>% filter(cluster %in% as.numeric(unique(hourlytrips$cluster))) -> hourlycentroids
plotmap(minlon,maxlon,minlat,maxlat,hourlycentroids,"+init=epsg:4326")
#Descartar domicilios
#Filtrar por minimo de estudiantes
#DESPUES: Buscar zonas atractoras de estudiantes para planificar transporte público universitario.
#Completar viajes en base a rutinas de cada persona (viajes repertitivos que permitan ver la trayectoria)