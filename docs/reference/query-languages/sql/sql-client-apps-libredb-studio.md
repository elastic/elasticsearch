---
applies_to:
  stack: ga
products:
  - id: elasticsearch
---

# LibreDB Studio [sql-client-apps-libredb-studio]

You can use the {{es}} SQL [REST API](sql-rest.md) to access {{es}} data from [LibreDB Studio](https://github.com/libredb/libredb-studio), an open source, web-based database IDE. LibreDB Studio sends each statement to the `_sql` endpoint over HTTP, so no JDBC or ODBC driver is needed.

::::{important}
Elastic does not endorse, promote or provide support for this application. For native {{es}} integration in LibreDB Studio, reach out to its vendor.
::::


## Prerequisites [sql-client-apps-libredb-studio-prerequisites]

* A running [LibreDB Studio](https://github.com/libredb/libredb-studio) server, started from the container image `ghcr.io/libredb/libredb-studio:latest` or with `npx @libredb/studio`
* Network access from the LibreDB Studio server to the {{es}} HTTP port


## New connection [sql-client-apps-libredb-studio-new-connection]

Create a new connection and select **Elasticsearch** as the database type. Under **Host & Instance**, enter the {{es}} host and its HTTP port, which defaults to `9200`.

For a cluster with security enabled, enter either a **Username** and **Password**, which are sent as HTTP Basic authentication, or an **API Key ID** and **API Key Secret**, which are sent in an `ApiKey` authorization header.

For a cluster that serves HTTPS with a publicly trusted certificate, open **SSL / TLS** and select the `verify-system` SSL mode. The port stays the same.


## Query and browse [sql-client-apps-libredb-studio-query]

The object tree lists the indices of the cluster, with the fields of each index read from its mapping. The editor runs {{es}} SQL statements, and when {{es}} returns a result in pages, LibreDB Studio follows the returned [cursor](sql-pagination.md) to read the following pages.
