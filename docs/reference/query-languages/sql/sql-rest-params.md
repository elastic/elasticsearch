---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-rest-params.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Passing parameters to a query [sql-rest-params]

Using values in a query condition, for example, or in a `HAVING` statement can be done "inline", by integrating the value in the query string itself:

```console
POST /_sql?format=txt
{
  "query": "SELECT YEAR(release_date) AS year FROM library WHERE page_count > 300 AND author = 'Frank Herbert' GROUP BY year HAVING COUNT(*) > 0"
}
```
% TEST[setup:library]

or it can be done by extracting the values in a separate list of parameters and using question mark placeholders (`?`) in the query string:

```console
POST /_sql?format=txt
{
  "query": "SELECT YEAR(release_date) AS year FROM library WHERE page_count > ? AND author = ? GROUP BY year HAVING COUNT(*) > ?",
  "params": [300, "Frank Herbert", 0]
}
```
% TEST[setup:library]

::::{important}
The recommended way of passing values to a query is with question mark placeholders, to avoid any attempts of hacking or SQL injection.
::::


