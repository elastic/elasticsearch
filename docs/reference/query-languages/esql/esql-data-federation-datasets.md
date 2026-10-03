---
navigation_title: "Datasets"
description: "Learn what an ES|QL Data Federation dataset defines and find the references for defining, creating, and querying datasets."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Datasets in {{esql}} Data Federation

A dataset makes a named collection of files in external storage available to {{esql}}. It records which connected data source and files to use, together with any settings or schema definitions needed to interpret them. You can query the dataset by name without ingesting its data into {{es}}.

For the overall mental model, from connecting external storage to querying a dataset, refer to [how {{esql}} Data Federation works](esql-data-federation.md#how-it-works).

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Define a dataset

To define a dataset, work through the following decisions:

1. **Select a data source.** Use a [connected data source](esql-data-federation-sources.md) that provides access to the external storage. One data source can serve multiple datasets.
2. **Name the dataset.** The name identifies the dataset in an {{esql}} query. Dataset names share a namespace with indices, data streams, aliases, and views, so a dataset cannot use the name of any of these existing objects.
3. **Select the files.** Use a storage URI and [resource pattern](esql-data-federation-patterns.md) to select files in one [supported file format](esql-data-federation-file-formats.md).
4. **Determine the schema.** Let {{es}} infer and reconcile the schema, or [declare column names and data types](esql-data-federation-schema.md) explicitly.
5. **Adjust dataset behavior.** Add [dataset settings](esql-data-federation-dataset-settings.md) when you need to change the defaults for file discovery, parsing, error handling, schema resolution, or query parallelism.
6. **Describe the dataset.** Add an optional description to explain what the dataset contains or how it is used.

## Create and manage a dataset

After defining what the dataset reads and how to interpret it, [create and manage the dataset](esql-data-federation-manage-datasets.md) in {{kib}} or with the `/_query/dataset` API.

## Query a dataset

Reference the dataset by name in the {{esql}} `FROM` command. To learn how queries discover files and reduce the amount of external data read, refer to [Query data with {{esql}} Data Federation](esql-data-federation-querying.md).
