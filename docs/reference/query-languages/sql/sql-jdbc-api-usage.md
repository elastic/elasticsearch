---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/_api_usage.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# API usage [_api_usage]

One can use JDBC through the official `java.sql` and `javax.sql` packages:

## `java.sql` [java-sql]

The former through `java.sql.Driver` and `DriverManager`:

```java
String address = "jdbc:es://" + elasticsearchAddress;     <1>
Properties connectionProperties = connectionProperties(); <2>
Connection connection =
    DriverManager.getConnection(address, connectionProperties);
```

1. The server and port on which Elasticsearch is listening for HTTP traffic. The port is by default 9200.
2. Properties for connecting to Elasticsearch. An empty `Properties` instance is fine for unsecured Elasticsearch.



## `javax.sql` [javax-sql]

Accessible through the `javax.sql.DataSource` API:

```java
EsDataSource dataSource = new EsDataSource();
String address = "jdbc:es://" + elasticsearchAddress;     <1>
dataSource.setUrl(address);
Properties connectionProperties = connectionProperties(); <2>
dataSource.setProperties(connectionProperties);
Connection connection = dataSource.getConnection();
```

1. The server and port on which Elasticsearch is listening for HTTP traffic. By default 9200.
2. Properties for connecting to Elasticsearch. An empty `Properties` instance is fine for unsecured Elasticsearch.


Which one to use? Typically client applications that provide most configuration properties in the URL rely on the `DriverManager`-style while `DataSource` is preferred when being *passed* around since it can be configured in one place and the consumer only has to call `getConnection` without having to worry about any other properties.

To connect to a secured Elasticsearch server the `Properties` should look like:

```java
Properties properties = new Properties();
properties.put("user", "test_admin");
properties.put("password", "x-pack-test-password");
```

Once you have the connection you can use it like any other JDBC connection. For example:

```java
try (Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(
              " SELECT name, page_count"
            + "    FROM library"
            + " ORDER BY page_count DESC"
            + " LIMIT 1")) {
    assertTrue(results.next());
    assertEquals("Don Quixote", results.getString(1));
    assertEquals(1072, results.getInt(2));
    SQLException e = expectThrows(SQLException.class, () ->
        results.getInt(1));
    assertThat(e.getMessage(), containsString("Unable to convert "
            + "value [Don Quixote] of type [TEXT] to [Integer]"));
    assertFalse(results.next());
}
```

::::{note} 
Elasticsearch SQL doesn’t provide a connection pooling mechanism, thus the connections the JDBC driver creates are not pooled. In order to achieve pooled connections, a third-party connection pooling mechanism is required. Configuring and setting up the third-party provider is outside the scope of this documentation.
::::



