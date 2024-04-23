library(mongolite)
library(ggplot2)

connection_string = 'mongodb+srv://<username>:<password>@cluster0.rfklo8t.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'

hrly = mongo(collection="rain", db="Forecasts", url=connection_string)

pipeMonthlyWindow <- function(length,var='total_precip',windowFun='sum') {
  post <- 0 # zero means window ends at current document timestamp
  pre <- post - (length+1)
  paste0(
'[
  {
    "$setWindowFields": {
      "sortBy": {
        "timestamp": 1
      }, 
      "partitionBy": "$station_id", 
      "output": {
        "maxAcrossRange": {
          "$',windowFun,'": "$',var,'", 
          "window": {
            "range": [
              ',pre,',',post,'
            ], 
            "unit": "hour"
          }
        }
      }
    }
  }, {
    "$group": {
      "_id": {
        "station_id": "$station_id", 
        "month": {
          "$dateToString": {
            "date": "$timestamp", 
            "format": "%Y-%m"
          }
        }
      }, 
      "maxForMonth": {
        "$max": "$maxAcrossRange"
      },
      "count": { "$sum": 1 }
    }
  }, {
    "$addFields": {
      "station_id": "$_id.station_id", 
      "month": "$_id.month",
      "duration": ',length,'
    }
  }, {
    "$replaceWith": {
      "$unsetField": {
        "field": "_id",
        "input": "$$ROOT"
      }
    }
  }, {
    "$sort": {
      "station_id": 1, 
      "month": 1
    }
  }
]'
  )
}


monmax <- c()
for (dur in c(3,6,12,24)) {
  tmpmax <- hrly$aggregate(pipeline=pipeMonthlyWindow(dur,var='total_precip',windowFun = 'sum'))
  tmpmax$p <- NA
  # add non-exceedance probability
  tmpmax$p[order(tmpmax$maxForMonth)] <- (seq_len(nrow(tmpmax))/(nrow(tmpmax)+1))
  monmax<-rbind(monmax,tmpmax)
}

# remove months with less than 26 days of data
monmax <- subset(monmax,count>26*24)

# Gumbel Reduced Variate
redvar <- function(p) -log(-log(p))
monmax$x <- redvar(monmax$p)

# x-axis label locations
px <- c(.2,.5,.75,.9,.95,.975,.99,.995,.9975)
plt <- ggplot(monmax,aes(x=x,y=maxForMonth)) + geom_point() + ylab('Monthly Maximum') +
  scale_x_continuous(name='Monthly Exceed Prob (%)',breaks=redvar(px),labels=round((1-px)*100,2)) +
  facet_wrap(.~duration)

plt

