connect_pgsql <- function(database="bici2023"){
  require("RPostgreSQL")
  drv <- dbDriver("PostgreSQL")
  pw <- {  "postgres" }
  con_m <- dbConnect(drv, dbname = database, host = "localhost", port = 5432, user = "postgres", password = pw)
  rm(pw) # removes the password
  return(con_m)
}
 
getData <- function(limit=10000, users=5){
  require("RPostgreSQL")
  con_t <- connect_pgsql("bici2023")
  query <- paste("SELECT distinct(company_token) user,count(company_token) datos FROM locations where (recorded_at between '2019-10-01' and '2019-12-01') group by (company_token) order by datos desc limit ",users, sep="")
  df <- dbGetQuery(con_t, query)
  users <- paste(paste0("'", df$user,"'"),collapse = ",")
  
  query <- paste("select accuracy,activity_type,device_model,event,battery_level,is_moving,speed,heading,recorded_at,latitude,longitude,company_token from locations where (recorded_at between '2019-10-01' and '2019-12-01') and company_token IN (",users,") order by recorded_at limit ",limit, sep="")
  df <- dbGetQuery(con_t, query)
  dbDisconnect(con_t)
  return(df)
}

segmentTrips <- function(subdata){
  library(lubridate)
  library(dplyr)
  
  #preprocessing
  subdata$activity_type <- factor(subdata$activity_type)
  subdata$device_model <- factor(subdata$device_model)
  subdata$is_moving <- factor(subdata$is_moving)
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

populateStayTime <- function(data){
  #populate stay time
  alltrips <- data
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

labelHomes <- function(tripdata, fromhour=20, minstay=3600){
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
    usertrips %>% filter(hour>fromhour & stayindestination>minstay & dtrips>1) -> t
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

#label travel mode
labelmode <- function(movingpoints){
  modes <- prop.table(table(movingpoints$activity_type)) #get modes per trip
  t <- (as.data.frame(modes))
  t <- t[order(t$Freq,decreasing = TRUE),]
  return(head(t,1)$Var) #sort by most frequent modes
}

changecoordsystem <- function(tripdata, longlabel="dlong", latlabel="dlat", sourceproj="+init=epsg:4326", targetproj="+init=epsg:31992"){
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
