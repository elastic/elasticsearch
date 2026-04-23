---
navigation_title: Getting started
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-getting-started.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Getting started with SQL [sql-getting-started]

To start using {{es}} SQL, create an index with some data to experiment with:

```console
PUT /library/_bulk?refresh
{"index":{"_id": "Leviathan Wakes"}}
{"name": "Leviathan Wakes", "author": "James S.A. Corey", "release_date": "2011-06-02", "page_count": 561}
{"index":{"_id": "Hyperion"}}
{"name": "Hyperion", "author": "Dan Simmons", "release_date": "1989-05-26", "page_count": 482}
{"index":{"_id": "Dune"}}
{"name": "Dune", "author": "Frank Herbert", "release_date": "1965-06-01", "page_count": 604}
```

And now you can execute SQL using the [SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query):

```console
POST /_sql?format=txt
{
  "query": "SELECT * FROM library WHERE release_date < '2000-01-01'"
}
```
% TEST[continued]

Which should return something along the lines of:

```text
    author     |     name      |  page_count   | release_date
---------------+---------------+---------------+------------------------
Dan Simmons    |Hyperion       |482            |1989-05-26T00:00:00.000Z
Frank Herbert  |Dune           |604            |1965-06-01T00:00:00.000Z
```
% TESTRESPONSE[s/\|/\\|/ s/\+/\\+/]
% TESTRESPONSE[non_json]

You can also use the [*SQL CLI*](sql-cli.md). There is a script to start it shipped in the Elasticsearch `bin` directory:

```bash
$ ./bin/elasticsearch-sql-cli
```

From there you can run the same query:

```sql
sql> SELECT * FROM library WHERE release_date < '2000-01-01';
    author     |     name      |  page_count   | release_date
---------------+---------------+---------------+------------------------
Dan Simmons    |Hyperion       |482            |1989-05-26T00:00:00.000Z
Frank Herbert  |Dune           |604            |1965-06-01T00:00:00.000Z
