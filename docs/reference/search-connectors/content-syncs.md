---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-sync-types.html
---

# Content syncs [es-connectors-sync-types]

Elastic connectors have two types of content syncs:

* [Full syncs](#es-connectors-sync-types-full)
* [Incremental syncs](#es-connectors-sync-types-incremental)


## Full syncs [es-connectors-sync-types-full]

::::{note}
We recommend running a full sync whenever [Sync rules](/reference/search-connectors/es-sync-rules.md) are modified

::::


A full sync syncs all documents in the third-party data source into {{es}}.

It also deletes any documents in {{es}}, which no longer exist in the third-party data source.

A full sync, by definition, takes longer than an incremental sync but it ensures full data consistency.

A full sync is available for all connectors.

You can [schedule](/reference/search-connectors/connectors-ui-in-kibana.md#es-connectors-usage-syncs-recurring) or [manually trigger](/reference/search-connectors/connectors-ui-in-kibana.md#es-connectors-usage-syncs-manual) a full sync job.


## Incremental syncs [es-connectors-sync-types-incremental]

An incremental sync only syncs data changes since the last full or incremental sync.

Incremental syncs are only available after an initial full sync has successfully completed. Otherwise the incremental sync will fail.

You can [schedule](/reference/search-connectors/connectors-ui-in-kibana.md#es-connectors-usage-syncs-recurring) or [manually trigger](/reference/search-connectors/connectors-ui-in-kibana.md#es-connectors-usage-syncs-manual) an incremental sync job.


### Incremental sync performance [es-connectors-sync-types-incremental-performance]

During an incremental sync your connector will still *fetch* all data from the third-party data source. If data contains timestamps, the connector framework compares document ids and timestamps. If a document already exists in {{es}} with the same timestamp, then this document does not need updating and will not be sent to {{es}}.

The determining factor in incremental sync performance is the raw volume of data ingested. For small volumes of data, the performance improvement using incremental syncs will be negligible. For large volumes of data, the performance impact can be huge. Additionally, an incremental sync is less likely to be throttled by {{es}}, making it more performant than a full sync when {{es}} is under heavy load.

A third-party data source that has throttling and low throughput, but stores very little data in Elasticsearch, such as GitHub, Jira, or Confluence, won’t see a significant performance improvement from incremental syncs.

However, a fast, accessible third-party data source that stores huge amounts of data in {{es}}, such as Azure Blob Storage, Google Drive, or S3, can lead to a significant performance improvement from incremental syncs.

::::{note}
Incremental syncs for [SharePoint Online](/reference/search-connectors/es-connectors-sharepoint-online.md) and [Google Drive](/reference/search-connectors/es-connectors-google-drive.md) connectors use specific logic. All other connectors use the same shared connector framework logic for incremental syncs.

::::



### Incremental sync availability [es-connectors-sync-types-incremental-supported]

Incremental syncs are available for the following connectors:

* [Azure Blob Storage](/reference/search-connectors/es-connectors-azure-blob.md)
* [Box](/reference/search-connectors/es-connectors-box.md)
* [Confluence](/reference/search-connectors/es-connectors-confluence.md)
* [Dropbox](/reference/search-connectors/es-connectors-dropbox.md)
* [GitHub](/reference/search-connectors/es-connectors-github.md)
* [Gmail](/reference/search-connectors/es-connectors-gmail.md)
* [Google Cloud Storage](/reference/search-connectors/es-connectors-google-cloud.md)
* [Google Drive](/reference/search-connectors/es-connectors-google-drive.md)
* [Jira](/reference/search-connectors/es-connectors-jira.md)
* [Network drive](/reference/search-connectors/es-connectors-network-drive.md)
* [Notion](/reference/search-connectors/es-connectors-notion.md)
* [OneDrive](/reference/search-connectors/es-connectors-onedrive.md)
* [Outlook](/reference/search-connectors/es-connectors-outlook.md)
* [Salesforce](/reference/search-connectors/es-connectors-salesforce.md)
* [ServiceNow](/reference/search-connectors/es-connectors-servicenow.md)
* [SharePoint Online](/reference/search-connectors/es-connectors-sharepoint-online.md)
* [SharePoint Server](/reference/search-connectors/es-connectors-sharepoint.md)
* [Teams](/reference/search-connectors/es-connectors-teams.md)
* [Zoom](/reference/search-connectors/es-connectors-zoom.md)

