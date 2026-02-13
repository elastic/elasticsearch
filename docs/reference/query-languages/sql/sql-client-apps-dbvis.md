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

![dbvis driver manager](../images/elasticsearch-reference-dbvis_open_driver_manager.png "")

Select **Elasticsearch** driver template from the left sidebar to create a new user driver:

![dbvis driver manager elasticsearch](../images/elasticsearch-reference-dbvis_new_driver_done.png "")

Download the driver locally:

![dbvis driver manager download](../images/elasticsearch-reference-dbvis_new_driver_start.png "")

and check its availability status:

![dbvis driver manager ready](../images/elasticsearch-reference-dbvis_new_driver_refresh.png "")


## Create a new connection [_create_a_new_connection]

Once the {{es}} driver is in place, create a new connection:

![dbvis new connection](../images/elasticsearch-reference-dbvis_add_db_connection.png "")

by double-clicking the {{es}} entry in the list of available drivers:

![dbvis new elasticsearch connection](../images/elasticsearch-reference-dbvis_add_connection.png "")

Enter the connection details, then press **Connect** and the driver version (as that of the cluster) should show up under **Connection Message**.

![dbvis enter connection details](../images/elasticsearch-reference-dbvis_connection_details.png "")


## Execute SQL queries [_execute_sql_queries]

The setup is done. DbVisualizer can be used to run queries against {{es}} and explore its content:

![dbvis running queries](../images/elasticsearch-reference-dbvis_hero.png "")


