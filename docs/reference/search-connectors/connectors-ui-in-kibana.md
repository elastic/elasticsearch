---
applies_to:
  stack: ga
  serverless:
    elasticsearch: ga
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-usage.html
---

# Connectors UI [es-connectors-usage]

This document describes operations available to connectors using the UI.

In the Kibana or Serverless UI, find Connectors using the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects). Here, you can view a summary of all your connectors and sync jobs, and to create new connectors.

::::{tip}
In 8.12 we introduced a set of [Connector APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create and manage Elastic connectors and sync jobs, along with a [CLI tool](https://github.com/elastic/connectors/blob/main/docs/CLI.md). Use these tools if you’d like to work with connectors and sync jobs programmatically, without using the UI.

::::



## Create and configure connectors [es-connectors-usage-index-create]

You connector writes data to an {{es}} index.

To create self-managed [**self-managed connector**](/reference/search-connectors/self-managed-connectors.md), use the buttons under **Search > Content > Connectors**. Once you’ve chosen the data source type you’d like to sync, you’ll be prompted to create an {{es}} index.

## Manage connector indices [es-connectors-usage-indices]

View and manage all Elasticsearch indices managed by connectors.

In the {{kib}} UI, navigate to **Search > Content > Connectors** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects). Here, you can view a list of connector indices and their attributes, including connector type health and ingestion status.

Within this interface, you can choose to view the details for each existing index or delete an index. Or, you can [create a new connector index](#es-connectors-usage-index-create).

These operations require access to Kibana and additional index privileges.


## Customize connector index mappings and settings [es-connectors-usage-index-create-configure-existing-index]

{{es}} stores your data as documents in an index. Each index is made up of a set of fields and each field has a type (such as `keyword`, `boolean`, or `date`).

**Mapping** is the process of defining how a document, and the fields it contains, are stored and indexed. Connectors use [dynamic mapping](docs-content://manage-data/data-store/mapping/dynamic-field-mapping.md) to automatically create mappings based on the data fetched from the source.

Index **settings** are configurations that can be adjusted on a per-index basis. They control things like the index’s performance, the resources it uses, and how it should handle operations.

When you create an index with a connector, the index is created with *default* search-optimized field template mappings and index settings. Mappings for specific fields are then dynamically created based on the data fetched from the source.

You can inspect your index mappings in the following ways:

* **In the {{kib}} UI**: Navigate to **Search > Content > Indices > *YOUR-INDEX* > Index Mappings**
* **By API**: Use the [Get mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-mapping)

You can manually **edit** the mappings and settings via the {{es}} APIs:

* Use the [Put mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) to update index mappings.
* Use the [Update index settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings) to update index settings.

It’s important to note that these updates are more complex when the index already contains data.

Refer to the following sections for more information.


### Customize mappings and settings before syncing data [es-connectors-usage-index-create-configure-existing-index-no-data]

Updating mappings and settings is simpler when your index has no data. If you create and attach a *new* index while setting up a connector, you can customize the mappings and settings before syncing data, using the APIs mentioned earlier.


### Customize mappings and settings after syncing data [es-connectors-usage-index-create-configure-existing-index-have-data]

Once data has been added to {{es}} using dynamic mappings, you can’t directly update existing field mappings. If you’ve already synced data into an index and want to change the mappings, you’ll need to [reindex your data](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex).

The workflow for these updates is as follows:

1. [Create](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) a new index with the desired mappings and settings.
2. [Reindex](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) your data from the old index into this new index.
3. Delete the old index.
4. (Optional) Use an [alias](docs-content://manage-data/data-store/aliases.md), if you want to retain the old index name.
5. Attach your connector to the new index or alias.


## Manage recurring syncs [es-connectors-usage-syncs-recurring]

After creating an index to be managed by a connector, you can configure automatic, recurring syncs.

In the {{kib}} UI, navigate to **Search > Content > Connectors** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).

Choose the index to configure, and then choose the **Scheduling** tab.

Within this interface, you can enable or disable scheduled:

1. Full content syncs
2. Incremental content syncs (if supported)
3. Access control syncs (if supported)

When enabled, you can additionally manage the sync schedule.

This operation requires access to Kibana and the `write` [indices privilege^](/reference/elasticsearch/security-privileges.md) for the `.elastic-connectors` index.

Alternatively, you can [sync once](#es-connectors-usage-syncs-manual).

After you enable recurring syncs or sync once, the first sync will begin. (There may be a short delay before the connector service begins the first sync.) You may want to [view the index details](#es-connectors-usage-index-view) to see the status or errors, or [view the synced documents](#es-connectors-usage-documents).


## Sync once [es-connectors-usage-syncs-manual]

After creating the index to be managed by a connector, you can request a single sync at any time.

In the {{kib}} UI, navigate to **Search > Content > Elasticsearch indices** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).

Then choose the index to sync.

Regardless of which tab is active, the **Sync** button is always visible in the top right. Choose this button to reveal sync options:

1. Full content
2. Incremental content (if supported)
3. Access control (if supported)

Choose one of the options to request a sync. (There may be a short delay before the connector service begins the sync.)

This operation requires access to Kibana and the `write` [indices privilege^](/reference/elasticsearch/security-privileges.md) for the `.elastic-connectors` index.


## Cancel sync [es-connectors-usage-syncs-cancel]

After a sync has started, you can cancel the sync before it completes.

In the {{kib}} UI, navigate to **Search > Content > Elasticsearch indices** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).

Then choose the index with the running sync.

Regardless of which tab is active, the **Sync** button is always visible in the top right. Choose this button to reveal sync options, and choose **Cancel Syncs** to cancel active syncs. This will cancel the running job, and marks all *pending* and *suspended* jobs as canceled as well. (There may be a short delay before the connector service cancels the syncs.)

This operation requires access to Kibana and the `write` [indices privilege^](/reference/elasticsearch/security-privileges.md) for the `.elastic-connectors` and `.elastic-connectors-sync-jobs` index.


## View status [es-connectors-usage-index-view]

View the index details to see a variety of information that communicate the status of the index and connector.

In the {{kib}} UI, navigate to **Search > Content > Elasticsearch indices** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).

Then choose the index to view.

The **Overview** tab presents a variety of information, including:

* General information about the connector index, for example: name, description, ingestion type, connector type, and language analyzer.
* Any errors affecting the connector or sync process.
* The current ingestion status (see below for possible values).
* The current document count.

Possible values of ingestion status:

* Incomplete - A connector that is not configured yet.
* Configured - A connector that is configured.
* Connected - A connector that can successfully connect to a data source.
* Error - A connector that failed to connect to the data source.
* Connector failure - A connector that has not seen any update for more than 30 minutes.
* Sync failure - A connector that failed in the last sync job.

This tab also displays the recent sync history, including sync status (see below for possible values).

Possible values of sync status:

* Sync pending - The initial job status, the job is pending to be picked up.
* Sync in progress - The job is running.
* Canceling sync - Cancelation of the job has been requested.
* Sync canceled - The job was canceled
* Sync suspended - The job was suspended due to service shutdown, and it can be resumed when the service restarts.
* Sync complete - The job completed successfully.
* Sync failure - The job failed.

For each sync, choose the `view` button to display the job details, including:

* The job ID
* Document stats, including: number of documents added/deleted, total number of documents, and volume of documented added
* Event logs
* Sync rules that were active when the sync was requested
* Pipelines that were active when the sync was requested

This operation requires access to Kibana and the `read` [indices privilege^](/reference/elasticsearch/security-privileges.md) for the `.elastic-connectors` index.


## View documents [es-connectors-usage-documents]

View the documents the connector has synced from the data. Additionally view the index mappings to determine the current document schema.

In the {{kib}} UI, navigate to **Search > Content > Elasticsearch indices** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).

Then choose the index to view.

Choose the **Documents** tab to view the synced documents. Choose the **Index Mappings** tab to view the index mappings that were created by the connector.

When setting up a new connector, ensure you are getting the documents and fields you were expecting from the data source. If not, see [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md) for help.

These operations require access to Kibana and the `read` and `manage` [indices privileges^](/reference/elasticsearch/security-privileges.md) for the index containing the documents.

See [Security](/reference/search-connectors/es-connectors-security.md) for security details.


## Manage sync rules [es-connectors-usage-sync-rules]

Use [sync rules](/reference/search-connectors/es-sync-rules.md) to limit which documents are fetched from the data source, or limit which fetched documents are stored in Elastic.

In the {{kib}} UI, navigate to **Search > Content > Elasticsearch indices** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).

Then choose the index to manage and choose the **Sync rules** tab.


## Manage ingest pipelines [es-connectors-usage-pipelines]

Use [ingest pipelines](docs-content://solutions/search/ingest-for-search.md) to transform fetched data before it is stored in Elastic.

In the {{kib}} UI, navigate to **Search > Content > Elasticsearch indices** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).

Then choose the index to manage and choose the **Pipelines** tab.

