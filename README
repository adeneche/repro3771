To reproduce [DRILL-3771](https://issues.apache.org/jira/browse/DRILL-3771) do the following:

- start a Drillbit cluster, or even a single Drillbit in embedded mode. You need to know how to connect to the cluster, e.g.

```
  jdbc:drill:drillbit=localhost:31010
```
- build this project

```
  mvn package
```

- you need a long running query (it will be canceled after fetching 10K rows, so make sure it returns more than that). e.g.

```
  SELECT * FROM bigtable LIMIT 1000000
```

- run the client passing the connection string and the query

```
  java -ea -jar target/repro3771-1.0-SNAPSHOT-jar-with-dependencies.jar -u jdbc:drill:drillbit=localhost:31010 -q "SELECT * FROM bigtable LIMIT 1000000"
```