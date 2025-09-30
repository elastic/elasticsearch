---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-odbc.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# SQL ODBC [sql-odbc]

## Overview [sql-odbc-overview]

Elasticsearch SQL ODBC Driver is a 3.80 compliant ODBC driver for {{es}}. It is a core level driver, exposing all of the functionality accessible through the {{es}}'s SQL API, converting ODBC calls into Elasticsearch SQL.

In order to make use of the driver, the server must have Elasticsearch SQL installed and running with the valid license.

* [Driver installation](sql-odbc-installation.md)
* [Configuration](sql-odbc-setup.md)
