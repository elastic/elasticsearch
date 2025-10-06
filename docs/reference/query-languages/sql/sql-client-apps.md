---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-client-apps.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# SQL client applications [sql-client-apps]

Thanks to its [JDBC](sql-jdbc.md) and [ODBC](sql-odbc.md) interfaces, a broad range of third-party applications can use {{es}}'s SQL capabilities. This section lists, in alphabetical order, a number of them and their respective configuration - the list however is by no means comprehensive (feel free to [submit a PR](https://www.elastic.co/blog/art-of-pull-request) to improve it): as long as the app can use the {{es}} SQL driver, it can use
{{es}} SQL.

* [DBeaver](sql-client-apps-dbeaver.md)
* [DbVisualizer](sql-client-apps-dbvis.md)
* [Microsoft Excel](sql-client-apps-excel.md)
* [Microsoft Power BI Desktop](sql-client-apps-powerbi.md)
* [Microsoft PowerShell](sql-client-apps-ps1.md)
* [MicroStrategy Desktop](sql-client-apps-microstrat.md)
* [Qlik Sense Desktop](sql-client-apps-qlik.md)
* [SQuirreL SQL](sql-client-apps-squirrel.md)
* [SQL Workbench](sql-client-apps-workbench.md)
* [Tableau Desktop](sql-client-apps-tableau-desktop.md)
* [Tableau Server](sql-client-apps-tableau-server.md)

::::{important}
Elastic does not endorse, promote or provide support for any of the applications listed. For native {{es}} integration in these products, reach out to their respective vendor.
::::


::::{note}
Each application has its own requirements and license these are outside the scope of this documentation which covers only the configuration aspect with {{es}} SQL.
::::


::::{warning}
The support for applications implementing the ODBC 2.x standard and prior is currently limited.
::::













