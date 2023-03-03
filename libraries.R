connect_pgsql <- function(database="bici2023"){
  require("RPostgreSQL")
  drv <- dbDriver("PostgreSQL")
  pw <- {  "postgres" }
  con_m <- dbConnect(drv, dbname = database, host = "localhost", port = 5432, user = "postgres", password = pw)
  rm(pw) # removes the password
  return(con_m)
}

getData <- function(limit=100000){
  require("RPostgreSQL")
  con_t <- connect_pgsql("bici2023")
  query <- paste("select * from locations where recorded_at between '2019-10-01' and '2019-12-01' limit ",limit, sep="")
  df <- dbGetQuery(con_t, query)
  dbDisconnect(con_t)
  return(df)
}

segmentTrips <- function(subdata, minttime=300, maxtime=3600){
  library(lubridate)
  library(dplyr)
  sumdistance <- 0
  #preprocessing
  subdata$activity_type <- factor(subdata$activity_type)
  subdata$device_model <- factor(subdata$device_model)
  subdata$event <- factor(subdata$event)
  subdata$is_moving <- factor(subdata$is_moving)
  subdata$date <- lubridate::ymd_hms(subdata$recorded_at)
  #segmentation
  movingpoints <- data.frame() #each trips points
  allusertrips <- data.frame() #data frame to return
  users <- unique(subdata$company_token) #list of unique users
  for (user in users){ #for each user
    id <- 1
    subdata %>% filter(company_token==user ) -> userpoints #user points
    if (nrow(userpoints %>% filter(activity_type=="still"))>0){ #if stops are found
      next_origin = userpoints[1,] #first origin is first point
      for (pointindex in 1:nrow(userpoints)){ #for each data point
        point <- userpoints[pointindex,]
        if (point$activity_type!="still"){ #merge moving points
          if(nrow(movingpoints)>1){
            last_point <- tail(movingpoints,1)
            distance <- sqrt((point$x-last_point$x)^2+(point$y-last_point$y)^2)
            sumdistance <- sumdistance + distance
          }  
          movingpoints <- rbind(movingpoints,point)
        } else{ #when stop add to all trips array
          if(nrow(movingpoints)>1){ #only if there are enough points (At least origin and destination)
            #create trip object
            distance <- sqrt((point$x-last_point$x)^2+(point$y-last_point$y)^2)
            sumdistance <- sumdistance + distance
            trip <- data.frame(user=user, tripid = id, ox=next_origin$x, oy=next_origin$y, dx=point$x, dy=point$y, olong=next_origin$longitude, olat=next_origin$latitude, dlong=point$longitud, dlat=point$latitude, starttime=head(movingpoints,1)$recorded_at, endtime=tail(movingpoints,1)$recorded_at, mode=labelmode(movingpoints), points=nrow(movingpoints), tdistance=sumdistance)
            allusertrips <- rbind(allusertrips,trip)
            movingpoints <- data.frame() #new trip
            sumdistance <- 0
            id <- id + 1 #new trip id
            next_origin = point #next origin is previous destination
          }
        }
      }
    }
  }
  #postprocessing
  allusertrips$ttime <- as.numeric(allusertrips$endtime-allusertrips$starttime)
  allusertrips %>% filter(ttime>=minttime & ttime<=maxtime) ->allusertrips
  return(allusertrips)
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
  clust <- dbscan(data.frame(x=tripdata$dx, y=tripdata$dy),eps = eps,minPts = minPts)
  tripdata$cluster<-factor(clust$cluster)
  tripdata %>% filter(cluster!="0") %>% group_by(cluster) %>% summarise(points=n(),centroid_x = mean(dx),centroid_y = mean(dy)) -> centroids
  centroids <- as.data.frame(centroids)
  print(centroids)
  wgscent <- changecoordsystem(centroids,longlabel = "centroid_x", latlabel = "centroid_y", sourceproj = "+init=epsg:31992",  targetproj="+init=epsg:4326")
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
