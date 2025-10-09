---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-client-apps-ps1.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Microsoft PowerShell [sql-client-apps-ps1]

You can use the {{es}} ODBC driver to access {{es}} data from Microsoft PowerShell.

::::{important}
Elastic does not endorse, promote or provide support for this application; for native {{es}} integration in this product, reach out to its vendor.
::::


## Prerequisites [_prerequisites_6]

* [Microsoft PowerShell](https://docs.microsoft.com/en-us/powershell/)
* {{es}} SQL [ODBC driver](sql-odbc.md)
* A preconfigured User or System DSN (see [Configuration](sql-odbc-setup.md#dsn-configuration) section on how to configure a DSN).


## Writing a script [_writing_a_script]

While putting the following instructions into a script file is not an absolute requirement, doing so will make it easier to extend and reuse. The following instructions exemplify how to execute a simple SELECT query from an existing index in your {{es}} instance, using a DSN configured in advance. Open a new file, `select.ps1`, and place the following instructions in it:

```powershell
$connectstring = "DSN=Local Elasticsearch;"
$sql = "SELECT * FROM library"

$conn = New-Object System.Data.Odbc.OdbcConnection($connectstring)
$conn.open()
$cmd = New-Object system.Data.Odbc.OdbcCommand($sql,$conn)
$da = New-Object system.Data.Odbc.OdbcDataAdapter($cmd)
$dt = New-Object system.Data.datatable
$null = $da.fill($dt)
$conn.close()
$dt
```

Now open a PowerShell shell and simply execute the script:

$$$apps_excel_exed$$$
![apps ps exed](../images/elasticsearch-reference-apps_ps_exed.png "")


