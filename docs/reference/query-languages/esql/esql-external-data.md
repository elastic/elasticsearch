---
navigation_title: "Query external data"
description: "Overview of querying data stored outside Elasticsearch using {{esql}}, including key concepts, supported data sources, and file formats."
applies_to:
  stack: preview =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# {{esql}} external data sources

{{esql}} external data sources enable you to query data stored outside {{es}} without ingesting it first. You register a connection to an external system, define which data to query, and access it with `FROM`, the same syntax used for native indices. External data and indexed data can be queried side by side in a single workflow.

## Why use external data sources

Many organizations store large volumes of data in cloud object storage for cost and compliance reasons. Querying that data typically requires a separate tool like Apache Spark, Amazon Athena, or Trino, which means managing extra infrastructure and switching between query languages.

{{esql}} external data sources enable you to query this data directly from {{es}}, with several advantages:

- **One language for all your data.** Use the same {{esql}} syntax for both indexed data and external data. No context-switching, no second query engine.
- **No extra infrastructure.** Query external data natively in {{es}} without deploying or managing additional compute services, catalogs, or connectors.
- **Progressive acceleration.** Start by querying raw data directly in object storage. When specific datasets need faster performance, promote them into {{es}} for indexed search. Both tiers stay queryable with the same `FROM` syntax.

## How it works

External data sources use two building blocks:

- A **[data source](esql-external-data-reference.md#data-source-api)** is a named connection to an external system. It stores the connection type, region, and credentials. A data source defines how to connect, not what data to query.
- A **[dataset](esql-external-data-reference.md#dataset-api)** is a virtual index that references a data source and specifies what to query. For file-based data sources, this is a resource path or glob pattern over files. A dataset is the entity you query by name, just like a regular index.

Datasets share the same namespace as indices, aliases, and views. A dataset cannot have the same name as an existing index.

One data source can serve many datasets. When credentials rotate, you update the data source in one place without touching the datasets that reference it.

Once a dataset exists, you query it with `FROM`:

```esql
FROM access_logs
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
| SORT error_count DESC
| LIMIT 20
```

There is no special syntax for external data. `FROM access_logs` works the same way whether `access_logs` is a native {{es}} index or a dataset pointing at external storage.

## Supported data source types

The following data source types are supported:

:::{include} _snippets/external-data/supported-data-source-types.md
:::

## Supported file formats

External data sources can read the following file formats:

:::{include} _snippets/external-data/supported-file-formats.md
:::

The format is detected automatically from the file extension. You can override this in the dataset settings if needed.

For details on type-specific settings and format options, refer to the [external data source reference](esql-external-data-reference.md).

## Capabilities and limitations

Most {{esql}} processing commands and functions work on external datasets. The execution engine is the same one used for native indices. Some capabilities that depend on Lucene or specialized data structures are not currently available.

For the full list, refer to the [external data source reference](esql-external-data-reference.md#functionality-that-does-not-work-or-works-differently-on-datasets).

## Next steps

- To set up and query your first external data source, refer to [](esql-external-data-quickstart.md).
- For detailed settings, format options, and the full capabilities list, refer to the [external data source reference](esql-external-data-reference.md).
