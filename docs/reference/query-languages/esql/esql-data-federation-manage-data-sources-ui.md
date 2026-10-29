---
navigation_title: "Manage in UI"
description: "Connect, view, edit, and delete ES|QL Data Federation data sources in Kibana."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Connect and manage data sources in the {{kib}} UI

:::{include} _snippets/data-federation/experimental-warning.md
:::

In the {{kib}} UI, you connect and manage data sources from the **Data sources** tab under **Data management** > **{{esql}} Data Federation**. To use the UI, [enable it in {{kib}}](esql-data-federation.md#enable-the-feature) first.

## View data sources

The **Data sources** tab lists each data source with its type, its description, and the number of datasets that read from it. You can search your data sources and connect new ones. Each row has the following actions:

- **Edit** ({icon}`pencil`): Opens the data source's connection settings.
- **Delete** ({icon}`trash`): Available only when no datasets read from the data source.

In the following example, `ookla_speedtest` can't be deleted because a dataset reads from it:

:::{image} images/data-federation/data-sources-tab.png
:alt: The Data sources tab listing two Amazon S3 data sources, with the delete action unavailable for ookla_speedtest because one dataset reads from it
:width: 800px
:::

## Connect a new data source

To connect a data source:

1. Click **Connect data source** to open the **Connect external data source** flyout.
2. Enter a **Name** and an optional **Description**.
3. In **Preferred method**, choose how {{es}} [authenticates](esql-data-federation-sources.md#authentication) to your storage:
   - Your bucket is public: select **Anonymous**. No credentials are stored.
   - Your bucket is private and your deployment runs on {{ech}}: select **Federated Identity**. It's the recommended method because no keys are stored, and it's the default when it's available. Refer to [connect with federated identity](esql-data-federation-federated-identity.md).
   - Your bucket is private and federated identity isn't available: select **Access and Secret Keys**, then enter the key pair. Refer to [connect with static credentials](esql-data-federation-static-credentials.md).
4. Click **Connect**.

:::{note}
Some connection options are available only with the [API](esql-data-federation-manage-data-sources-api.md):

- A custom Amazon S3 [`endpoint`](esql-data-federation-data-source-settings.md#endpoint)
- [Managed identity](esql-data-federation-sources.md#authentication) authentication
:::

### Check the connection

Connecting a data source doesn't test whether it can read your files. To check the connection, [create a dataset](esql-data-federation-manage-datasets.md) that reads from it and query the dataset.

{applies_to}`stack: preview 9.6+` You can also check the connection with the [test a connection](esql-data-federation-manage-data-sources-api.md#test-a-connection) API.

## Edit a data source

To change a data source:

1. Click **Edit** ({icon}`pencil`) in the data source's row.
2. Update its settings. You can't change the data source type or name.
3. Click **Save**.

Credential fields that you leave unchanged keep their stored values. When credentials rotate, update them here. Datasets that read from the data source use the new credentials without any changes.

## Delete a data source

A data source can't be deleted while datasets read from it. To delete a data source:

1. [Delete the datasets](esql-data-federation-manage-datasets.md) that read from it.
2. Click **Delete** ({icon}`trash`) in the data source's row.

To delete several unused data sources at once, select them, then click **Delete**.

## Next steps

:::{include} _snippets/data-federation/data-sources-next-steps.md
:::
