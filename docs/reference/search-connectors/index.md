---
applies_to:
  stack: ga
  serverless: ga
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors.html
  - https://www.elastic.co/guide/en/serverless/current/elasticsearch-ingest-data-through-integrations-connector-client.html
  - https://www.elastic.co/guide/en/enterprise-search/current/connectors.html
---

# Search connectors

$$$es-connectors-native$$$


:::{note}
This page is about Search connectors that synchronize third-party data into {{es}}. If you’re looking for Kibana connectors to integrate with services like generative AI model providers, refer to [Kibana Connectors](docs-content://deploy-manage/manage-connectors.md).
:::

A _connector_ is an Elastic integration that syncs data from an original data source to {{es}}. Use connectors to create searchable, read-only replicas of your data in {{es}}.

Each connector extracts the original files, records, or objects; and transforms them into documents within {{es}}.

These connectors are written in Python and the source code is available in the [`elastic/connectors`](https://github.com/elastic/connectors/tree/main/connectors/sources) repo.

## Available connectors


::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::


Connectors are available for the following third-party data sources:

- [Azure Blob Storage](/reference/search-connectors/es-connectors-azure-blob.md)
- [Box](/reference/search-connectors/es-connectors-box.md)
- [Confluence](/reference/search-connectors/es-connectors-confluence.md)
- [Dropbox](/reference/search-connectors/es-connectors-dropbox.md)
- [GitHub](/reference/search-connectors/es-connectors-github.md)
- [Gmail](/reference/search-connectors/es-connectors-gmail.md)
- [Google Cloud Storage](/reference/search-connectors/es-connectors-google-cloud.md)
- [Google Drive](/reference/search-connectors/es-connectors-google-drive.md)
- [GraphQL](/reference/search-connectors/es-connectors-graphql.md)
- [Jira](/reference/search-connectors/es-connectors-jira.md)
- [MicrosoftSQL](/reference/search-connectors/es-connectors-ms-sql.md)
- [MongoDB](/reference/search-connectors/es-connectors-mongodb.md)
- [MySQL](/reference/search-connectors/es-connectors-mysql.md)
- [Network drive](/reference/search-connectors/es-connectors-network-drive.md)
- [Notion](/reference/search-connectors/es-connectors-notion.md)
- [OneDrive](/reference/search-connectors/es-connectors-onedrive.md)
- [OpenText Documentum](/reference/search-connectors/es-connectors-opentext.md)
- [Oracle](/reference/search-connectors/es-connectors-oracle.md)
- [Outlook](/reference/search-connectors/es-connectors-outlook.md)
- [PostgreSQL](/reference/search-connectors/es-connectors-postgresql.md)
- [Redis](/reference/search-connectors/es-connectors-redis.md)
- [S3](/reference/search-connectors/es-connectors-s3.md)
- [Salesforce](/reference/search-connectors/es-connectors-salesforce.md)
- [ServiceNow](/reference/search-connectors/es-connectors-servicenow.md)
- [SharePoint Online](/reference/search-connectors/es-connectors-sharepoint-online.md)
- [SharePoint Server](/reference/search-connectors/es-connectors-sharepoint.md)
- [Slack](/reference/search-connectors/es-connectors-slack.md)
- [Teams](/reference/search-connectors/es-connectors-teams.md)
- [Zoom](/reference/search-connectors/es-connectors-zoom.md)

:::{tip}
Because prerequisites and configuration details vary by data source, you’ll need to refer to the individual connector references for specific details.
:::

## Overview


Because connectors are self-managed on your own infrastructure, they run outside of your Elastic deployment.

You can run them from source or in a Docker container.

## Workflow

In order to set up, configure, and run a connector you’ll be moving between your third-party service, the Elastic UI, and your terminal. At a high-level, the workflow looks like this:

1. Satisfy any data source prerequisites (e.g., create an OAuth application).
2. Create a connector in the UI (or via the API).
3. Deploy the connector service:
    - [Option 1: Run with Docker](es-connectors-run-from-docker.md) (recommended)
    - [Option 2: Run from source](es-connectors-run-from-source.md)
4. Enter data source configuration details in the UI.

### Data source prerequisites

The first decision you need to make before deploying a connector is which third party service (data source) you want to sync to {{es}}. See the list of [available connectors](#available-connectors).

Note that each data source will have specific prerequisites you’ll need to meet to authorize the connector to access its data. For example, certain data sources may require you to create an OAuth application, or create a service account. You’ll need to check the [individual connector documentation](connector-reference.md) for these details.