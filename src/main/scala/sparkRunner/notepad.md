## Notepad.exe

This is mostly random snippets saved 'just in case'.

Useful learnings aggregated at [GitHub](https://github.com/dannyhlee/DanMan)

#### Code snippets
```
    val rootObjectDS = rawDF.select("as_of", "created_at", "locations", "trends").as[RootObject]
    rootObjectDS.show(truncate=false)
    
    rootObjectDS.select("as_of").show()
    rootObjectDS.select("created_at").show()
    rootObjectDS.select("locations").show()
    rootObjectDS.select("trends").show()
    
    val location = rootObjectDS.select("locations.name")
    location.show(truncate = false)
    
    val trends = rootObjectDS.select("trends.element")
    trends.show(truncate = false)

    val trendList = trends.select(explode(trends("trends")))
    trendList.show(truncate = false)

    val trendSingle = trendList.select(explode(trendList("col"))).toDF()
      .select("name", "url", "promoted_content", "query", "tweet_volume").as[TrendItem]
    trendSingle.show(truncate = false)`
```
#### Schemas
```
    val singleTrendSchema = new StructType()
      .add("trends", ArrayType(new StructType()
        .add("name", StringType)
        .add("promoted_content", StringType)
        .add("query", StringType)
        .add("tweet_volume", LongType)
        .add("url", StringType)
      ))
```

#### Case classes
```
    case class TrendItem(name: String, url: String, promoted_content: String,
                      query: String, tweet_volume: Double)
    
    case class Locations(name: String, woeid: Double)
    
    case class RootObject(trends: List[TrendItem], as_of: String,
                          created_at: String, locations: List[Locations])
```