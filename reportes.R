rm(list = ls())
library(dplyr)
library(tictoc)
source("libraries.R")

tic()
#1. Load data
# data <- getData() #call once
# data %>% filter(accuracy<1000 & activity_type!="unknown") %>% select(device_model,event,battery_level,is_moving,speed,heading,activity_type,recorded_at,latitude,longitude,company_token) ->subdata
# save(subdata,file="subdata.R")
load(file="subdata.R")

#2. Track points stats
glimpse(subdata)
table(subdata$is_moving, subdata$activity_type)
boxplot(speed~activity_type,subdata)

#3. Trip records stats
subdata %>% filter(latitude>-3 & latitude < -2 & longitude > -79 & longitude < -78) -> subdata#bounding box Cuenca
datapoints <- changecoordsystem(subdata,longlabel = "longitude", latlabel = "latitude", targetproj = "+init=epsg:31992" )
plotmap(min(datapoints$longitude)-0.002,max(datapoints$longitude)+0.002,min(datapoints$latitude)-0.007,max(datapoints$latitude)+0.007,datapoints,"+init=epsg:31992")

tripdata <- segmentTrips(datapoints, minttime = 600, maxtime = 3600) #get trips 10 minutes < x < 1 hour
hist(tripdata$ttime)
table(tripdata$mode)
boxplot(ttime ~ mode, tripdata)
plot(tripdata$dx, tripdata$dy,main = "User destinations",xlab = "x", ylab="y",cex.axis=.95)
tripdata$x = tripdata$dx; tripdata$y = tripdata$dy; 
plotmap(min(tripdata$dlong)-0.002,max(tripdata$dlong)+0.002,min(tripdata$dlat)-0.007,max(tripdata$dlat)+0.007,tripdata,"+init=epsg:31992")


#4. Encontrar zonas densas del mapa de calor
tripdata <- getAttractors(tripdata, eps=22, minPts=10)
save(tripdata,file="tripdata.R")
toc()

#DESPUES: Buscar zonas atractoras de estudiantes para planificar transporte público universitario.
