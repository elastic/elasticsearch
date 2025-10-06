---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-cli.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# SQL CLI [sql-cli]

Elasticsearch ships with a script to run the SQL CLI in its `bin` directory:

```bash
$ ./bin/elasticsearch-sql-cli
```

You can pass the URL of the Elasticsearch instance to connect to as the first parameter:

```bash
$ ./bin/elasticsearch-sql-cli https://some.server:9200
```

If security is enabled on your cluster, you can pass the username and password in the form `username:password@host_name:port` to the SQL CLI:

```bash
$ ./bin/elasticsearch-sql-cli https://sql_user:strongpassword@some.server:9200
```

Once the CLI is running you can use any [query](elasticsearch://reference/query-languages/sql/sql-spec.md) that Elasticsearch supports:

```sql
sql> SELECT * FROM library WHERE page_count > 500 ORDER BY page_count DESC;
     author      |        name        |  page_count   | release_date
-----------------+--------------------+---------------+---------------
Peter F. Hamilton|Pandora's Star      |768            |1078185600000
Vernor Vinge     |A Fire Upon the Deep|613            |707356800000
Frank Herbert    |Dune                |604            |-144720000000
Alastair Reynolds|Revelation Space    |585            |953078400000
James S.A. Corey |Leviathan Wakes     |561            |1306972800000
```

The jar containing the SQL CLI is a stand alone Java application and the scripts just launch it. You can move it around to other machines without having to install Elasticsearch on them. Without the already provided script files, you can use a command similar to the following to start the SQL CLI:

```bash
$ ./java -jar [PATH_TO_CLI_JAR]/elasticsearch-sql-cli-[VERSION].jar https://some.server:9200
```

or

```bash
$ ./java -cp [PATH_TO_CLI_JAR]/elasticsearch-sql-cli-[VERSION].jar org.elasticsearch.xpack.sql.cli.Cli https://some.server:9200
```

The jar name will be different for each Elasticsearch version (for example `elasticsearch-sql-cli-7.3.2.jar`), thus the generic `VERSION` specified in the example above. Furthermore, if not running the command from the folder where the SQL CLI jar resides, you’d have to provide the full path, as well.


## CLI commands [cli-commands]

Apart from SQL queries, CLI can also execute some specific commands:

`allow_partial_search_results = <boolean>` (default `false`)
:   If `true`, returns partial results if there are shard request timeouts or [shard failures](docs-content://deploy-manage/distributed-architecture/reading-and-writing-documents.md#shard-failures). If `false`, returns an error with no partial results.

```sql
sql> allow_partial_search_results = true;
allow_partial_search_results set to true
```

`fetch_size = <number>` (default `1000`)
:   Allows to change the size of fetches for query execution. Each fetch is delimited by fetch separator (if explicitly set).

```sql
sql> fetch_size = 2000;
fetch size set to 2000
```

`fetch_separator = <string>` (empty string by default)
:   Allows to change the separator string between fetches.

```sql
sql> fetch_separator = "---------------------";
fetch separator set to "---------------------"
```

`lenient = <boolean>` (default `false`)
:   If `false`, Elasticsearch SQL returns an error for fields containing [array values](elasticsearch://reference/elasticsearch/mapping-reference/array.md). If `true`, Elasticsearch SQL returns the first value from the array with no guarantee of consistent results.

```sql
sql> lenient = true;
lenient set to true
```

`info`
:   Returns server information.

```sql
sql> info;
Node:mynode Cluster:elasticsearch Version:8.3
```

`exit`
:   Closes the CLI.

```sql
sql> exit;
Bye!
```

`cls`
:   Clears the screen.

```sql
sql> cls;
```

`logo`
:   Prints Elastic logo.

```sql
sql> logo;

                       asticElasticE
                     ElasticE  sticEla
          sticEl  ticEl            Elast
        lasti Elasti                   tic
      cEl       ast                     icE
     icE        as                       cEl
     icE        as                       cEl
     icEla     las                        El
   sticElasticElast                     icElas
 las           last                    ticElast
El              asti                 asti    stic
El              asticEla           Elas        icE
El            Elas  cElasticE   ticEl           cE
Ela        ticEl         ticElasti              cE
 las     astic               last              icE
   sticElas                   asti           stic
     icEl                      sticElasticElast
     icE                       sticE   ticEla
     icE                       sti       cEla
     icEl                      sti        Ela
      cEl                      sti       cEl
       Ela                    astic    ticE
         asti               ElasticElasti
           ticElasti  lasticElas
              ElasticElast

                       SQL
                      8.3.0
```

