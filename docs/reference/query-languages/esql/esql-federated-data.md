---
navigation_title: "Federated data"
description: "Overview of querying data stored outside Elasticsearch using ES|QL, including key concepts, supported data sources, and file formats."
applies_to:
  stack: preview =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# {{esql}} federated data

A **data source** is a connection to an external system, such as S3, GCS, or Azure, that {{esql}} can read data from.
A **dataset** references data from a data source and is exposed under a name that you can query with `FROM <dataset>`.

## Security [esql-federated-data-security]

Access to data sources and datasets is controlled with dedicated role privileges.

### Data source privileges [esql-federated-data-datasource-privileges]

Grant access to data sources with the `data_source` privilege under `global` in a role definition. This privilege takes a list of groups, each pairing a `names` pattern (matching one or more data source names) with a list of `privileges`:

`create`
:   Create a data source with a matching name.

`read_metadata`
:   Retrieve information about data sources with a matching name.

`delete`
:   Delete a data source with a matching name.

`read`
:   Reference a data source with a matching name when creating or updating a dataset.

`manage`
:   All operations on a data source with a matching name: `create`, `read_metadata`, `delete`, and `read`.

For example, the following role can fully manage data sources named `sales_*`, and can reference the `logs_s3` data source when creating datasets:

```yaml
data_source_admin:
  global:
    data_source:
      - names: [ "sales_*" ]
        privileges: [ "manage" ]
      - names: [ "logs_s3" ]
        privileges: [ "read" ]
```

### Dataset privileges [esql-federated-data-dataset-privileges]

Datasets are secured like indices, using the following index privileges:

`create_dataset`
:   Create a dataset with a matching name. Creating a dataset that references a data source also requires the `read` data source privilege for that data source; the two are authorized independently.

`read_dataset_metadata`
:   Retrieve information about datasets with a matching name.

`delete_dataset`
:   Delete a dataset with a matching name.

`manage_dataset`
:   All operations on a dataset with a matching name: `create_dataset`, `read_dataset_metadata`, and `delete_dataset`.

Querying a dataset with `FROM <dataset>` only requires the ordinary `read` index privilege on the dataset name; no privilege on the underlying data source is needed to query it.

The `read` grant on a dataset name must not carry document-level or field-level security; `FROM <dataset>` is rejected if it does.

For example, the following role can create datasets named `sales_ds_*` that reference `sales_*` data sources, and query them:

```yaml
dataset_writer:
  global:
    data_source:
      - names: [ "sales_*" ]
        privileges: [ "read" ]
  indices:
    - names: [ "sales_ds_*" ]
      privileges: [ "create_dataset", "read" ]
```
