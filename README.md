## pj2 SparkRunner

#### I. Description
A spark runner file

#### II. Done
1. Read in json
2. Apply nested schema
3. Read and write to HDFS

figure a:
```
          root
      |-- as_of: string (nullable = true)
      |-- created_at: string (nullable = true)
      |-- locations: array (nullable = true)
      |    |-- element: struct (containsNull = true)
      |    |    |-- name: string (nullable = true)
      |    |    |-- woeid: long (nullable = true)
      |-- trends: array (nullable = true)
      |    |-- element: struct (containsNull = true)
      |    |    |-- name: string (nullable = true)
      |    |    |-- promoted_content: string (nullable = true)
      |    |    |-- query: string (nullable = true)
      |    |    |-- tweet_volume: long (nullable = true)
      |    |    |-- url: string (nullable = true)
```

#### III. Doing
1. SQL queries
2. Process data, produce single trend object (IV.1)
3. Generate SQL - single topic rank change over time in a specific geo
4. Generate SQL - location change of a topic over time
5. Generate index measuring proximity between geos

#### IV. Doable
1. Produce (date, location, single-trend, rank) objects 
  a. Analyze change of rank over time
  b. Analyze rank and rank change between locations over time
2. Chart change graphically per topic
3. Chart location list that changes over time
