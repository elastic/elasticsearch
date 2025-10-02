---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-client-apps-dbvis.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# DbVisualizer [sql-client-apps-dbvis]

You can use the {{es}} JDBC driver to access {{es}} data from DbVisualizer.

::::{important}
Elastic does not endorse, promote or provide support for this application.
::::


## Prerequisites [_prerequisites_3]

* [DbVisualizer](https://www.dbvis.com/) 13.0 or higher
* Elasticsearch SQL [JDBC driver](sql-jdbc.md)

  Note
  :   Pre 13.0 versions of DbVisualizer can still connect to {{es}} by having the [JDBC driver](sql-jdbc.md) set up from the generic **Custom** template.



## Setup the {{es}} JDBC driver [_setup_the_es_jdbc_driver]

Setup the {{es}} JDBC driver through **Tools** > **Driver Manager**:

![dbvis 1 driver manager](../images/elasticsearch-reference-dbvis-1-driver-manager.png "")

Select **Elasticsearch** driver template from the left sidebar to create a new user driver:

![dbvis 2 driver manager elasticsearch](../images/elasticsearch-reference-dbvis-2-driver-manager-elasticsearch.png "")

Download the driver locally:

![dbvis 3 driver manager download](../images/elasticsearch-reference-dbvis-3-driver-manager-download.png "")

and check its availability status:

![dbvis 4 driver manager ready](../images/elasticsearch-reference-dbvis-4-driver-manager-ready.png "")


## Create a new connection [_create_a_new_connection]

Once the {{es}} driver is in place, create a new connection:

![dbvis 5 new conn](../images/elasticsearch-reference-dbvis-5-new-conn.png "")

by double-clicking the {{es}} entry in the list of available drivers:

![dbvis 6 new conn elasticsearch](../images/elasticsearch-reference-dbvis-6-new-conn-elasticsearch.png "")

Enter the connection details, then press **Connect** and the driver version (as that of the cluster) should show up under **Connection Message**.

![dbvis 7 new conn connect](../images/elasticsearch-reference-dbvis-7-new-conn-connect.png "")


## Execute SQL queries [_execute_sql_queries]

The setup is done. DbVisualizer can be used to run queries against {{es}} and explore its content:

![dbvis 8 data](../images/elasticsearch-reference-dbvis-8-data.png "")


