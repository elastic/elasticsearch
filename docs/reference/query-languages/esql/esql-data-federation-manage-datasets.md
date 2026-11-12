---
navigation_title: "Create and manage"
description: "Create, inspect, update, and delete ES|QL Data Federation datasets in Kibana or with the Elasticsearch API."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Create and manage datasets for {{esql}} Data Federation

:::{include} _snippets/data-federation/experimental-warning.md
:::

Create and manage {{esql}} Data Federation datasets in {{kib}} or with the {{es}} API. Before
creating a dataset, [connect a data source](esql-data-federation-sources.md) and review how to
[define a dataset](esql-data-federation-datasets.md#define-a-dataset). Creating a dataset doesn't check that the data source can read its resource, so also [grant read access](esql-data-federation-sources.md#grant-read-access) to the data in your cloud provider.

You can manage datasets in two ways:

[Kibana UI](esql-data-federation-manage-datasets-ui.md)
:   Add datasets with a step-by-step wizard, and view, edit, and delete them from the **Datasets** tab.

[API](esql-data-federation-manage-datasets-api.md)
:   Create, get, list, and delete datasets with the `/_query/dataset` endpoint.

## Audit logging

With [audit logging enabled](docs-content://deploy-manage/security/logging-configuration/enabling-audit-logs.md),
creating, updating, and deleting datasets generates `security_config_change` events. Refer to
[put_dataset](../../elasticsearch/elasticsearch-audit-events.md#event-put-dataset) and
[delete_dataset](../../elasticsearch/elasticsearch-audit-events.md#event-delete-dataset).
