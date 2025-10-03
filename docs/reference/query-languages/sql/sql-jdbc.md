---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-jdbc.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# SQL JDBC [sql-jdbc]

{{es}}'s SQL jdbc driver is a rich, fully featured JDBC driver for {{es}}. It is Type 4 driver, meaning it is a platform independent, stand-alone, Direct to Database, pure Java driver that converts JDBC calls to {{es}} SQL.


## Installation [sql-jdbc-installation]

The JDBC driver can be obtained from:

Dedicated page
:   [elastic.co](https://www.elastic.co/downloads/jdbc-client) provides links, typically for manual downloads.

Maven dependency
:   [Maven](https://maven.apache.org/)-compatible tools can retrieve it automatically as a dependency:

```xml subs=true
<dependency>
  <groupId>org.elasticsearch.plugin</groupId>
  <artifactId>x-pack-sql-jdbc</artifactId>
  <version>{{version.stack}}</version>
</dependency>
```

from [Maven Central Repository](https://search.maven.org/artifact/org.elasticsearch.plugin/x-pack-sql-jdbc), or from `artifacts.elastic.co/maven` by adding it to the repositories list:

```xml
<repositories>
  <repository>
    <id>elastic.co</id>
    <url>https://artifacts.elastic.co/maven</url>
  </repository>
</repositories>
```


## Version compatibility [jdbc-compatibility]

Your driver must be compatible with your {{es}} version.

::::{important}
The driver version cannot be newer than the {{es}} version. For example, {{es}} version 7.10.0 is not compatible with {{version.stack}} drivers.
::::


| {{es}} version | Compatible driver versions | Example |
| --- | --- | --- |
| 7.7.0 and earlier versions | * The same version.<br> | {{es}} 7.6.1 is only compatible with 7.6.1 drivers. |


## Setup [jdbc-setup]

The driver main class is `org.elasticsearch.xpack.sql.jdbc.EsDriver`. Note the driver implements the JDBC 4.0 `Service Provider` mechanism meaning it is registered automatically as long as it is available in the classpath.

Once registered, the driver understands the following syntax as an URL:

```text
jdbc:[es|elasticsearch]://[[http|https]://]?[host[:port]]?/[prefix]?[\?[option=value]&]*
```

`jdbc:[es|elasticsearch]://`
:   Prefix. Mandatory.

`[[http|https]://]`
:   Type of HTTP connection to make. Possible values are `http` (default) or `https`. Optional.

`[host[:port]]`
:   Host (`localhost` by default) and port (`9200` by default). Optional.

`[prefix]`
:   Prefix (empty by default). Typically used when hosting {{es}} under a certain path. Optional.

`[option=value]`
:   Properties for the JDBC driver. Empty by default. Optional.

The driver recognized the following properties:


#### Essential [jdbc-cfg]

$$$jdbc-cfg-timezone$$$

`timezone` (default JVM timezone)
:   Timezone used by the driver *per connection* indicated by its `ID`. **Highly** recommended to set it (to, say, `UTC`) as the JVM timezone can vary, is global for the entire JVM and can’t be changed easily when running under a security manager.


#### Network [jdbc-cfg-network]

`connect.timeout` (default `30000`)
:   Connection timeout (in milliseconds). That is the maximum amount of time waiting to make a connection to the server.

`network.timeout` (default `60000`)
:   Network timeout (in milliseconds). That is the maximum amount of time waiting for the network.

`page.size` (default `1000`)
:   Page size (in entries). The number of results returned per page by the server.

`page.timeout` (default `45000`)
:   Page timeout (in milliseconds). Minimum retention period for the scroll cursor on the server. Queries that require a stateful scroll cursor on the server side might fail after this timeout. Hence, when scrolling through large result sets, processing `page.size` records should not take longer than `page.timeout` milliseconds.

`query.timeout` (default `90000`)
:   Query timeout (in milliseconds). That is the maximum amount of time waiting for a query to return.


### Basic Authentication [jdbc-cfg-auth]

`user`
:   Basic Authentication user name

`password`
:   Basic Authentication password


### SSL [jdbc-cfg-ssl]

`ssl` (default `false`)
:   Enable SSL

`ssl.keystore.location`
:   key store (if used) location

`ssl.keystore.pass`
:   key store password

`ssl.keystore.type` (default `JKS`)
:   key store type. `PKCS12` is a common, alternative format

`ssl.truststore.location`
:   trust store location

`ssl.truststore.pass`
:   trust store password

`ssl.truststore.type` (default `JKS`)
:   trust store type. `PKCS12` is a common, alternative format

`ssl.protocol`(default `TLS`)
:   SSL protocol to be used


### Proxy [_proxy]

`proxy.http`
:   Http proxy host name

`proxy.socks`
:   SOCKS proxy host name


### Mapping [_mapping]

`field.multi.value.leniency` (default `true`)
:   Whether to be lenient and return the first value (without any guarantees of what that will be - typically the first in natural ascending order) for fields with multiple values (true) or throw an exception.


### Index [_index]

`index.include.frozen` (default `false`)
:   Whether to include frozen indices in the query execution or not (default).


### Cluster [_cluster]

`catalog`
:   Default catalog (cluster) for queries. If unspecified, the queries execute on the data in the local cluster only.

    See [{{ccs}}](docs-content://solutions/search/cross-cluster-search.md).



### Error handling [_error_handling]

`allow.partial.search.results` (default `false`)
:   Whether to return partial results in case of shard failure or fail the query throwing the underlying exception (default).


### Troubleshooting [_troubleshooting]

`debug` (default `false`)
:   Setting it to `true` will enable the debug logging.

`debug.output` (default `err`)
:   The destination of the debug logs. By default, they are sent to standard error. Value `out` will redirect the logging to standard output. A file path can also be specified.


### Additional [_additional]

`validate.properties` (default `true`)
:   If disabled, it will ignore any misspellings or unrecognizable properties. When enabled, an exception will be thrown if the provided property cannot be recognized.

To put all of it together, the following URL:

```text
jdbc:es://http://server:3456/?timezone=UTC&page.size=250
```

opens up a Elasticsearch SQL connection to `server` on port `3456`, setting the JDBC connection timezone to `UTC` and its pagesize to `250` entries.
