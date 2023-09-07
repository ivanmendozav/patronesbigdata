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
library(descriptr)
library(digest)

connect_pgsql <- function(database="bici2023"){
  require("RPostgreSQL")
  drv <- dbDriver("PostgreSQL")
  pw <- {  "postgres" }
  con_m <- dbConnect(drv, dbname = database, host = "localhost", port = 5432, user = "postgres", password = pw)
  rm(pw) # removes the password
  return(con_m)
}

connect_remotepgsql <- function(host="172.16.1.214",database="bici",user="bici",pwd="6di2NseuN9r8"){
  require("RPostgreSQL")
  drv <- dbDriver("PostgreSQL")
  con_m <- dbConnect(drv, dbname = database, host = host, port = 5432, user = user, password = pwd)
  return(con_m)
}
 
getData <- function(limit=10000, users=5){
  require("RPostgreSQL")
  con_t <- connect_pgsql("bici2023")
  query <- paste("SELECT distinct(company_token) user,count(company_token) datos FROM locations where (recorded_at between '2019-10-01' and '2019-12-01') group by (company_token) order by datos desc limit ",users, sep="")
  df <- dbGetQuery(con_t, query)
  users <- paste(paste0("'", df$user,"'"),collapse = ",")
  
  query <- paste("select id,accuracy,activity_type,altitude,device_model,event,battery_level,is_moving,speed,heading,recorded_at,latitude,longitude,company_token from locations where (recorded_at between '2019-10-01' and '2019-12-01') and company_token IN (",users,") order by recorded_at limit ",limit, sep="")
  df <- dbGetQuery(con_t, query)
  dbDisconnect(con_t)
  return(df)
}


getRemoteData <- function(limit=1000000, users=5){
  require("RPostgreSQL")
  con_t <- connect_remotepgsql()
  query <- paste("SELECT distinct(company_token)||(device_id) as user,count(*) datos
FROM locations where recorded_at  > '2023-01-01'
               group by (company_token)||(device_id)
               order by datos desc limit ",users, sep="")
  df <- dbGetQuery(con_t, query)
  users <- paste(paste0("'", df$user,"'"),collapse = ",")
  
  query <- paste("select id,accuracy,activity_type,(company_token)||(device_id) as company_token,device_model,event,battery_level,
       is_moving,speed,heading,recorded_at,latitude,longitude,altitude
from locations where recorded_at > '2023-01-01' and (company_token)||(device_id)
IN (",users,") order by (company_token)||(device_id),recorded_at limit ",limit, sep="")
  df <- dbGetQuery(con_t, query)
  dbDisconnect(con_t)
  return(df)
}


getDataByUser <- function(limit=100000, userid="galvarezc"){
  require("RPostgreSQL")
  con_t <- connect_pgsql("bici2023")
  query <- paste("select id,accuracy,activity_type,altitude,device_model,event,battery_level,is_moving,speed,heading,recorded_at,latitude,longitude,company_token from locations where company_token = '",userid,"' order by recorded_at limit ",limit, sep="")
  df <- dbGetQuery(con_t, query)
  dbDisconnect(con_t)
  df <- df[order(df$recorded_at),]
  return(df)
}

getDataByRemoteUser <- function(limit=100000, userid="fsacoto", deviceid=""){
  require("RPostgreSQL")
  con_t <- connect_remotepgsql()
  query <- paste("select id,accuracy,activity_type,(company_token)||(device_id) as company_token,device_model,event,battery_level,
       is_moving,speed,heading,recorded_at,latitude,longitude,altitude from locations where recorded_at>'2023-01-01' and company_token = '",userid,"' and device_id = '",deviceid,"' order by recorded_at limit ",limit, sep="")
  df <- dbGetQuery(con_t, query)
  dbDisconnect(con_t)
  df <- df[order(df$recorded_at),]
  return(df)
}

#Using activity type STILL
segmentTrips <- function(data){
  #preprocessing
  subdata <- data %>% select(company_token,x,y,latitude,longitude,recorded_at,activity_type)
  subdata$activity_type <- factor(subdata$activity_type)
  subdata$date <- lubridate::ymd_hms(subdata$recorded_at)
  #segmentation
  
  allusertrips <- data.frame() #data frame to return
  users <- unique(subdata$company_token) #list of unique users
  for (user in users){ #for each user
    id <- 1
    subdata %>% filter(company_token==user ) %>% drop_na() -> userpoints #user points
    userpoints <- userpoints[order(userpoints$date),] #sort trips by datetime
    print(paste(user,":",nrow(userpoints)))
    if (nrow(userpoints)>1 & nrow(userpoints %>% filter(activity_type=="still"))>0){ #if stops are found
      movingpoints <- data.frame() #each trips points
      sumdistance <- 0
      next_origin = userpoints[1,] #first point is first origin
      for (pointindex in 2:nrow(userpoints)){ #for each data point
        point <- userpoints[pointindex,]
        if(nrow(movingpoints)>1){
          last_point <- tail(movingpoints,1)
          distance <- sqrt((point$x-last_point$x)^2+(point$y-last_point$y)^2)
          sumdistance <- sumdistance + distance
        }
        if (point$activity_type!="still"){ #merge moving points
          movingpoints <- rbind(movingpoints,point)
        } else{ #when stop add to all trips array
          #if(nrow(movingpoints)>1){ #only if there are enough points (At least origin and destination)
            #create trip object
            #distance <- sqrt((point$x-last_point$x)^2+(point$y-last_point$y)^2)
            #sumdistance <- sumdistance + distance
          if(nrow(movingpoints)>1){
            trip <- data.frame(user=user, tripid = id, ox=next_origin$x, oy=next_origin$y, dx=point$x, dy=point$y, olong=next_origin$longitude, olat=next_origin$latitude, dlong=point$longitud, dlat=point$latitude, starttime=next_origin$recorded_at, endtime=last_point$recorded_at, mode=labelmode(movingpoints), points=nrow(movingpoints), tdistance=sumdistance)
            allusertrips <- rbind(allusertrips,trip)
            id <- id + 1 #new trip id
          }
          movingpoints <- data.frame() #new trip
          sumdistance <- 0
          next_origin = point #next origin is previous destination
          #}
        }
      }
    }
  }
  #postprocessing
  allusertrips$ttime <- as.numeric(as.POSIXct(allusertrips$endtime))-as.numeric(as.POSIXct(allusertrips$starttime))
  #allusertrips %>% filter(ttime>=minttime & ttime<=maxtime) ->allusertrips
  return(allusertrips)
}


#Using is_moving FALSE
segmentTrips2 <- function(data){
  #preprocessing
  subdata <- data %>% select(company_token,x,y,latitude,longitude,recorded_at,activity_type,is_moving) %>% drop_na()
  subdata$activity_type <- factor(subdata$activity_type)
  subdata$is_moving <- factor(subdata$is_moving)
  subdata$date <- lubridate::ymd_hms(subdata$recorded_at)
  #segmentation
  
  allusertrips <- data.frame() #data frame to return
  users <- unique(subdata$company_token) #list of unique users
  for (user in users){ #for each user
    id <- 1
    subdata %>% filter(company_token==user ) -> userpoints #user points
    userpoints <- userpoints[order(userpoints$date),] #sort trips by datetime
    if (nrow(userpoints)>1 & nrow(userpoints %>% filter(is_moving=="FALSE"))>0){ #if stops are found
      movingpoints <- data.frame() #each trips points
      sumdistance <- 0
      next_origin = userpoints[1,] #first point is first origin
      for (pointindex in 2:nrow(userpoints)){ #for each data point
        point <- userpoints[pointindex,]
        if(nrow(movingpoints)>1){
          last_point <- tail(movingpoints,1)
          distance <- sqrt((point$x-last_point$x)^2+(point$y-last_point$y)^2)
          sumdistance <- sumdistance + distance
        }
        if (point$is_moving!="FALSE"){ #merge moving points
          movingpoints <- rbind(movingpoints,point)
        } else{ #when stop add to all trips array
          #if(nrow(movingpoints)>1){ #only if there are enough points (At least origin and destination)
          #create trip object
          #distance <- sqrt((point$x-last_point$x)^2+(point$y-last_point$y)^2)
          #sumdistance <- sumdistance + distance
          if(nrow(movingpoints)>1){
            trip <- data.frame(user=user, tripid = id, ox=next_origin$x, oy=next_origin$y, dx=point$x, dy=point$y, olong=next_origin$longitude, olat=next_origin$latitude, dlong=point$longitud, dlat=point$latitude, starttime=next_origin$recorded_at, endtime=point$recorded_at, mode=labelmode(movingpoints), points=nrow(movingpoints), tdistance=sumdistance)
            allusertrips <- rbind(allusertrips,trip)
            id <- id + 1 #new trip id
          }
          movingpoints <- data.frame() #new trip
          sumdistance <- 0
          next_origin = point #next origin is previous destination
          #}
        }
      }
    }
  }
  #postprocessing
  allusertrips$ttime <- as.numeric(as.POSIXct(allusertrips$endtime))-as.numeric(as.POSIXct(allusertrips$starttime))
  #allusertrips %>% filter(ttime>=minttime & ttime<=maxtime) ->allusertrips
  return(allusertrips)
}


#Using speed below threshold
segmentTrips3 <- function(fsubdata, epsilon){
  fallusertrips <- data.frame() #data frame to return
  users <- unique(fsubdata$uid) #list of unique users
  for (u in users){ #for each user
    id <- 1
    fsubdata %>% filter(uid==u ) -> userpoints #user points
    if (nrow(userpoints)>1 & nrow(userpoints[userpoints$speed<epsilon,])>0){ #if stops are found
      movingpoints <- data.frame() #each trips points
      sumdistance <- 0
      sumvelocity <- 0
      origin = userpoints[1,]
      for (pointindex in 1:nrow(userpoints)){ #for each data point
        point <- userpoints[pointindex,]
        if (point$speed>=epsilon){ #when moving
          if(nrow(movingpoints)==0){
            origin = point #first point moving
          }
          movingpoints <- rbind(movingpoints,point)
          sumdistance <- sumdistance + point$distance
          sumvelocity <- sumvelocity + point$speed
        } else{ #when stop add to all trips array
          if(nrow(movingpoints)>0){
              destination <- point
              avg_vel <- sumvelocity/nrow(movingpoints)
              trip <- data.frame(user=u, tripid = id, dx=destination$x, dy=destination$y, starttime=origin$t, endtime=destination$t, tdistance=sumdistance)
              fallusertrips <- rbind(fallusertrips,trip)
              id <- id + 1 #new trip id
              movingpoints <- data.frame() #new trip
              sumdistance <- 0
              sumvelocity <- 0
            }
          }
        }
      }
    }
  #postprocessing
  fallusertrips$ttime <- as.numeric(fallusertrips$endtime - fallusertrips$starttime)
  fallusertrips[fallusertrips$ttime<0,] <- abs(24-fallusertrips$starttime)+fallusertrips$endtime
  return(fallusertrips)
}


addSpeeds <- function(set){
  datapoints <- set
  datapoints <- transform(datapoints,Lag_x=lag(x,n = 1,default = 0))
  datapoints <- transform(datapoints,Lag_y=lag(y,n = 1,default = 0))
  datapoints <- transform(datapoints,Lag_t=lag(timestamp,n = 1,default = 0))
  datapoints$distance <- sqrt((datapoints$Lag_x-datapoints$x)^2 + (datapoints$Lag_y-datapoints$y)^2)/1000
  datapoints$dt <- (datapoints$timestamp - datapoints$Lag_t)/3600
  datapoints$speed <- datapoints$distance/datapoints$dt
  datapoints <- datapoints[datapoints$dt>0,]
  datapoints <- datapoints[2:nrow(datapoints),]
  datapoints <- datapoints[datapoints$speed<130,]
  datapoints %>% select(-c(Lag_x,Lag_y,Lag_t)) -> datapoints
  datapoints <- datapoints[2:nrow(datapoints),]
  return  (datapoints)
}


#time-based
segmentTrips4 <- function(datapoints, tmin=600, vmax=2, minPts=2){
  populated <- data.frame()
  for (uid in unique(datapoints$uid)){
    subsetone <- datapoints[datapoints$uid==uid,]
    # subsetone <- transform(subsetone,Lag_x=lag(x,n = 1,default = 0))
    # subsetone <- transform(subsetone,Lag_y=lag(y,n = 1,default = 0))
    # subsetone <- transform(subsetone,Lag_t=lag(timestamp,n = 1,default = 0))
    # subsetone$distance <- sqrt((subsetone$Lag_x-subsetone$x)^2 + (subsetone$Lag_y-subsetone$y)^2)/1000
    # subsetone$dt <- (subsetone$timestamp - subsetone$Lag_t)/3600
    # subsetone$speed <- subsetone$distance/subsetone$dt
    # subsetone <- subsetone[subsetone$dt>0,]
    # subsetone <- subsetone[subsetone$speed<130,]
    # subsetone <-  subsetone %>% select(-c(Lag_x,Lag_y,Lag_t))
    # subsetone <- subsetone[2:nrow(subsetone),]
    subsetone$id <- 1:nrow(subsetone)
    subsetone %>% filter(speed>vmax) -> validspeeds
    validspeeds  %>% select(timestamp) ->dataframe
    clusters <- dbscan(dataframe,eps = tmin, minPts = minPts)
    validspeeds$tripid <- factor(clusters$cluster)
    query <- "select subsetone.*,validspeeds.tripid from subsetone left join validspeeds on validspeeds.id = subsetone.id"
    subsetone <- sqldf(query)
    subsetone[is.na(subsetone)] <- 0
    subsetone$tripid <- factor(subsetone$tripid)
    query <- "select t.tripid,min(t.id) origin_id,max(t.id) destination_id,min(t) departure, max(t) arrival,sum(t.distance) travel_distance, max(t)-min(t) travel_time from subsetone as t where t.tripid!=0 group by t.tripid order by t.tripid"
    trippoints <- sqldf(query)
    query <- "select t.*,origin.x origin_x, origin.y origin_y,origin.z origin_z, dest.x dest_x, dest.y dest_y,dest.z dest_z,dest.timestamp,dest.dow from trippoints as t left join subsetone as origin on origin.id=t.origin_id left join subsetone as dest on dest.id=t.destination_id order by t.tripid"
    trippoints <- sqldf(query)
    trips <- trippoints %>% select(tripid,origin_x,origin_y,origin_z,departure,dest_x,dest_y, dest_z,arrival,travel_distance,travel_time,dow,timestamp)
    colnames(trips) <- c("tripid","ox","oy","oz","departure","dx","dy","dz","arrival","tdistance","ttime","dow","timestamp")
    trips$uid <- uid
    populated <- rbind(populated,trips)
  }
  populated$id <- 1:nrow(populated)
  return (populated)
}


#time-based
segmentTrips5 <- function(datapoints, tmin=600, vmax=2, minPts=2){
  populated <- data.frame()
  for (uid in unique(datapoints$uid)){
    subsetone <- datapoints[datapoints$uid==uid,]
    subsetone$id <- 1:nrow(subsetone)
    #filter
    subsetone %>% filter(speed>vmax) -> validspeeds
    validspeeds  %>% select(timestamp) ->dataframe
    #cluster by time
    clusters <- dbscan(dataframe,eps = tmin, minPts = minPts)
    validspeeds$tripid <- factor(clusters$cluster)
    
    #label trip id
    jointables <- left_join(subsetone, validspeeds, by=c('id'='id'))
    subsetone$tripid <- factor(jointables$tripid) 
    subsetone <- subsetone %>% tidyr::drop_na()
    
    #aggregate to find endpoints
    subsetone %>%
      group_by(tripid) %>%
      summarise(
        origin_id = max(id, na.rm = T),
        destination_id = min(id, na.rm = T),
        points = n(),
        travel_distance = sum(distance),
        departure = min(timestamp),
        arrival = max(timestamp),
        travel_time = max(timestamp)-min(timestamp)
      ) %>%
      arrange(tripid) -> trippoints
    
    #bring location and time of endpoints
    d <- inner_join(trippoints, subsetone, by=c('origin_id'='id'))
    d <- select(d,c(tripid.x,destination_id,points,travel_distance,departure,arrival,travel_time,uid,x,y))
    d <- inner_join(d, subsetone, by=c('destination_id'='id'))
    trips <- select(d,c(tripid.x,points,travel_distance,departure,arrival,travel_time,uid.x,x.x,y.x,x.y,y.y))
    colnames(trips) <- c("tripid","points","travel_distance","departure","arrival","travel_time","uid","origin_x","origin_y","destination_x","destination_y")
    trips$departure <- (as_datetime(trips$departure ))
    trips$arrival <- (as_datetime(trips$arrival ))
    populated <- rbind(populated,trips)
  }
  return (tail(populated,-1))
}

whereAtTime <- function(alltrips,time=22){
    alltrips %>% filter(hour(starttime)>=time & hour(endtime)<=time) -> onroad
    alltrips %>% filter(hour(endtime)>=time & hour(endtime)+floor(stayindestination/3600)) -> ondestination
    t <-rbind(onroad,ondestination)
    return(t)
}

populateStayTime <- function(data, mindistance=0){
  #populate stay time
  alltrips <- data
  alltrips <- alltrips[alltrips$tdistance>=mindistance,]
  arrivalseconds <- as.numeric(as.POSIXct(alltrips$endtime))
  departureseconds <- as.numeric(as.POSIXct(alltrips$starttime))
  alltrips$departureseconds <- departureseconds
  alltrips$arrivalseconds <- arrivalseconds
  alltrips$stayindestination <- 0
  
  users <- unique(alltrips$user) #list of unique users
  newtripdata <- data.frame()
  for (u in users){
    alltrips %>% filter(user==u ) -> usertrips
    usertrips <- usertrips[order(usertrips$date),] 
    for (i in 1:(nrow(usertrips)-1)){
      usertrips[i,]$stayindestination <-usertrips[i+1,]$departureseconds - usertrips[i,]$arrivalseconds 
    }
    newtripdata <- rbind(newtripdata,usertrips)
  }
  return(newtripdata)
}

#for paper
populateStayTime2 <- function(data){
  #populate stay time
  alltrips <- data
  alltrips$departureseconds <- alltrips$arrival*3600
  alltrips$arrivalseconds <- alltrips$departure*3600
  alltrips$stayindestination <- 0
  
  users <- unique(alltrips$uid) #list of unique users
  newtripdata <- data.frame()
  for (u in users){
    alltrips %>% filter(uid==u ) -> usertrips
    usertrips <- usertrips[order(usertrips$tripid),] 
    for (i in 1:(nrow(usertrips)-1)){
      if (usertrips[i+1,]$departureseconds > usertrips[i,]$arrivalseconds){
        usertrips[i,]$stayindestination <-usertrips[i+1,]$departureseconds - usertrips[i,]$arrivalseconds 
      }
      else{
        usertrips[i,]$stayindestination <- (((24*3600)-usertrips[i,]$arrivalseconds)+usertrips[i+1,]$departureseconds)/3600
      }
    }
    newtripdata <- rbind(newtripdata,usertrips)
  }
  newtripdata <- newtripdata %>% select(-c(departureseconds, arrivalseconds))
  return(newtripdata)
}


labelHomes <- function(tripdata, fromhour=20){
  #home travels are approx 25% of total travels
  users <- unique(tripdata$user) #list of unique users
  newtripdata <- data.frame()
  for (ui in users){
    usertrips <- tripdata[tripdata$user == ui,]
    usertrips$dtrips <- 0
    usertrips$home <- "N"
    #print(ui)
    clust <- dbscan(data.frame(x=usertrips$dx , y=usertrips$dy),eps = 50,minPts = 2)
    usertrips$cluster <- clust$cluster
    
    # add trips number to destination
    dclust <- as.data.frame(table(usertrips$cluster))
    for (ucluster in dclust$Var1){
      dtrips <- as.numeric(dclust[dclust$Var1==ucluster,]$Freq)
      usertrips[usertrips$cluster == ucluster,]$dtrips = dtrips
    }
    
    #focus on night trips for home
    t <- whereAtTime(usertrips,fromhour)
    if (length(unique(clust$cluster))>1 & nrow(t)>0){
      dclust <- as.data.frame(table(t$cluster))
      clust <- dclust[dclust$Var1!="0",]
      clust <- clust[order(clust$Freq,decreasing = T),]
      home <- as.numeric(clust[1,]$Var1)
      usertrips$home <- ifelse(usertrips$cluster == home, "Y","N")
      
    }else{usertrips$home <-"N"}
    usertrips$cluster <- NULL  
    newtripdata <- rbind(newtripdata,usertrips)
  }
  return(newtripdata)
}


labelHomes2 <- function(tripdata){
  #home travels are approx 25% of total travels
  users <- unique(tripdata$user) #list of unique users
  newtripdata <- data.frame()
  for (ui in users){
    usertrips <- tripdata[tripdata$user == ui,]
    usertrips$dtrips <- 0
    usertrips$home <- "N"
    #print(ui)
    clust <- dbscan(data.frame(x=usertrips$dx , y=usertrips$dy),eps = 20,minPts = 2)
    usertrips$cluster <- clust$cluster
    
    # add trips number to destination
    dclust <- as.data.frame(table(usertrips$cluster))
    for (ucluster in dclust$Var1){
      dtrips <- as.numeric(dclust[dclust$Var1==ucluster,]$Freq)
      usertrips[usertrips$cluster == ucluster,]$dtrips = dtrips
    }
    
    #focus last day location
    if (length(unique(clust$cluster))>1 ){
      query <- "select distinct cluster,dtrips from usertrips where cluster!=0 and tripid IN (select max(tripid) last_trip from usertrips group by day) order by dtrips desc"
      last_trip_clusters <- sqldf(query)
      home <- as.numeric(last_trip_clusters[1,]$cluster)
      usertrips$home <- ifelse(usertrips$cluster == home, "Y","N")
      
    }else{usertrips$home <-"N"}
    usertrips$cluster <- NULL  
    newtripdata <- rbind(newtripdata,usertrips)
  }
  return(newtripdata)
}


labelHomes3 <- function(tripdata){
  #home travels are approx 25% of total travels
  users <- unique(tripdata$uid) #list of unique users
  newtripdata <- data.frame()
  
  for (ui in users){
    usertrips <- tripdata[tripdata$uid == ui,]
    #arrival day (last completed trip of day)
    usertrips$day <- format(ymd_hms(as_datetime(usertrips$timestamp)),'%Y-%m-%d')
    usertrips$dtrips <- 0
    usertrips$home <- "N"
    clust <- dbscan(data.frame(x=usertrips$dx , y=usertrips$dy),eps = 50,minPts = 5)
    usertrips$cluster <- clust$cluster
    
    # add trips number to destination
    dclust <- as.data.frame(table(usertrips$cluster))
    for (ucluster in dclust$Var1){
      dtrips <- as.numeric(dclust[dclust$Var1==ucluster,]$Freq)
      usertrips[usertrips$cluster == ucluster,]$dtrips = dtrips
    }
    
    #focus on last day location
    if (length(unique(clust$cluster))>1 ){
      query <- "select distinct cluster,dtrips from usertrips where arrival > 20 and cluster!=0 and tripid IN (select max(tripid) last_trip from usertrips group by day) order by dtrips desc"
      last_trip_clusters <- sqldf(query)
      home <- as.numeric(last_trip_clusters[1,]$cluster)
      usertrips$home <- ifelse(usertrips$cluster == home, "Y","N")
      
    }else{usertrips$home <-"N"}
    usertrips$cluster <- NULL  
    usertrips$day <- NULL
    newtripdata <- rbind(newtripdata,usertrips)
  }
  return(newtripdata)
}


#label travel mode
labelmode <- function(movingpoints){
  modes <- prop.table(table(movingpoints$activity_type)) #get modes per trip
  t <- (as.data.frame(modes))
  t <- t[order(t$Freq,decreasing = TRUE),]
  return(head(t,1)$Var) #sort by most frequent modes
}

changecoordsystem <- function(tripdata, longlabel="dlong", latlabel="dlat", sourceproj="+proj=longlat +datum=WGS84", targetproj="+proj=utm +zone=17 +south  ellps=WGS84"){
  library(rgdal)
  data <- tripdata
  coordinates(data) <- c(longlabel, latlabel) #columns with points
  proj4string(data) <- CRS(sourceproj) # Current projection: WGS 84
  converted <- spTransform(data,  CRS(targetproj)) # target projection: 31992 for Ecuador
  finalcoords <- coordinates(converted)
  finaldata <- tripdata
  finaldata$x <- finalcoords[,1]
  finaldata$y <- finalcoords[,2]
  return(finaldata) #coords in new projection
}

getAttractors <- function(tripdata, eps=25, minPts=10){
  library(dplyr)
  library(dbscan)
  library(sqldf)
  clust <- dbscan(data.frame(x=tripdata$dx, y=tripdata$dy),eps = eps,minPts = minPts)
  tripdata$cluster<-as.integer(clust$cluster)
  centroids <- sqldf("select cluster,count(distinct(user)) users,count(*) trips,avg(dx) centroid_x,avg(dy) centroid_y from tripdata where cluster<>0 group by cluster order by cluster limit 10",dbname = "bici2023")
  wgscent <- changecoordsystem(centroids,longlabel = "centroid_x", latlabel = "centroid_y", sourceproj = "+init=epsg:31992",  targetproj="+init=epsg:4326")
  colnames(wgscent) <- c("cluster","users","trips","x","y","long","lat")
  return (list(tripdata, wgscent))
}

plotmap <- function(minlon,maxlon,minlat,maxlat,points,projection="+init=epsg:4326"){
  library(OpenStreetMap)
  library(ggplot2)
  tol = 0
  #points <- points[points$x >= minlon & points$x <= maxlon & points$y >= minlat & points$y <= maxlat,]
  sa_map <- openmap(c(maxlat+tol, minlon+tol), c(minlat, maxlon), 
                    type = "opencyclemap")
  sa_map2 <- openproj(sa_map, projection = projection)
  sa_map2_plt <- OpenStreetMap::autoplot.OpenStreetMap(sa_map2) + 
        geom_point(data = points,
               aes(x = x, y = y), # slightly shift the points
               colour = "red", size =  1) +
    xlab("Longitude (°E)") + ylab("Latitude (°S)")
  plot(sa_map2_plt)
}
