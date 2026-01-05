---
applies_to:
  stack: ga
  serverless: ga
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors.html
  - https://www.elastic.co/guide/en/serverless/current/elasticsearch-ingest-data-through-integrations-connector-client.html
  - https://www.elastic.co/guide/en/enterprise-search/current/connectors.html
---

# Content connectors

$$$es-connectors-native$$$


:::{note}
This page is about Content connectors that synchronize third-party data into {{es}}. If you’re looking for Kibana connectors to integrate with services like generative AI model providers, refer to [Kibana Connectors](docs-content://deploy-manage/manage-connectors.md).
:::

A _connector_ is an Elastic integration that syncs data from an original data source to {{es}}. Use connectors to create searchable, read-only replicas of your data in {{es}}.

Each connector extracts the original files, records, or objects; and transforms them into documents within {{es}}.

These connectors are written in Python and the source code is available in the [`elastic/connectors`](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources) repo.

## Available connectors


::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

This table provides an overview of our available connectors, their current support status, and the features they support.

The columns provide specific information about each connector:

- **Status**: Indicates whether the connector is in General Availability (GA), Technical Preview, Beta, or is an Example connector.
- **Advanced sync rules**: Specifies the versions in which advanced sync rules are supported, if applicable.
- **Local binary extraction service**: Specifies the versions in which the local binary extraction service is supported, if applicable.
- **Incremental syncs**: Specifies the version in which incremental syncs are supported, if applicable.
- **Document level security**: Specifies the version in which document level security is supported, if applicable.



| Connector | Status | [Advanced sync rules](./es-sync-rules.md#es-sync-rules-advanced) | [Local binary extraction service](./es-connectors-content-extraction.md#es-connectors-content-extraction-local) | [Incremental syncs](./content-syncs.md#es-connectors-sync-types-incremental) | [Document level security](./document-level-security.md) | Source code                                                                                                                |
| ------- | --------------- | -- | -- | -- | -- |----------------------------------------------------------------------------------------------------------------------------|
| [Azure Blob](/reference/search-connectors/es-connectors-azure-blob.md) | **GA** | - | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/azure_blob_storage/) |
| [Box](/reference/search-connectors/es-connectors-box.md)  | **Preview** | - | - | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/box)                                     |
| [Confluence Cloud](/reference/search-connectors/es-connectors-confluence.md) | **GA** | 8.9+ | 8.11+ | 8.13+ | 8.10 | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/atlassian/confluence)                              |
| [Confluence Data Center](/reference/search-connectors/es-connectors-confluence.md) | **Preview** | 8.13+ | 8.13+ | 8.13+ | 8.14+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/atlassian/confluence)                              |
| [Confluence Server](/reference/search-connectors/es-connectors-confluence.md)| **GA** | 8.9+ | 8.11+ | 8.13+ | 8.14+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/atlassian/confluence)                              |
| [Dropbox](/reference/search-connectors/es-connectors-dropbox.md)| **GA** | - | 8.11+ | 8.13+ | 8.12+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/dropbox)                                 |
| [GitHub](/reference/search-connectors/es-connectors-github.md)| **GA** | 8.10+ | 8.11+ | 8.13+ | 8.12+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/github)                                  |
| [Gmail](/reference/search-connectors/es-connectors-gmail.md)| **GA** | - | - | 8.13+ | 8.10+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/gmail)                                   |
| [Google Cloud Storage](/reference/search-connectors/es-connectors-google-cloud.md)| **GA** | - | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/google_cloud_storage)                    |
| [Google Drive](/reference/search-connectors/es-connectors-google-drive.md)| **GA** | - | 8.11+ | 8.13+ | 8.10+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/google_drive)                            |
| [GraphQL](/reference/search-connectors/es-connectors-graphql.md)| **Preview** | - | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/graphql)                                 |
| [Jira Cloud](/reference/search-connectors/es-connectors-jira.md)| **GA** | 8.9+ | 8.11+ | 8.13+ | 8.10+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/atlassian/jira)                                    |
| [Jira Data Center](/reference/search-connectors/es-connectors-jira.md)| **Preview** | 8.13+ | 8.13+ | 8.13+ | 8.13+*| [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/atlassian/jira)                                    |
| [Jira Server](/reference/search-connectors/es-connectors-jira.md)| **GA** | 8.9+ | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/atlassian/jira)                                    |
| [Microsoft SQL Server](/reference/search-connectors/es-connectors-ms-sql.md)| **GA** | 8.11+ | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/mssql)                                   |
| [MongoDB](/reference/search-connectors/es-connectors-mongodb.md)| **GA** | 8.8 native/ 8.12 self-managed | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/mongo)                                   |
| [MySQL](/reference/search-connectors/es-connectors-mysql.md)| **GA** | 8.8+ | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/mysql)                                   |
| [Network drive](/reference/search-connectors/es-connectors-network-drive.md)| **GA** | 8.10+ | 8.14+ | 8.13+ | 8.11+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/network_drive)                           |
| [Notion](/reference/search-connectors/es-connectors-notion.md)| **GA** | 8.14+ | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/notion)                                  |
| [OneDrive](/reference/search-connectors/es-connectors-onedrive.md)| **GA** | 8.11+ | 8.11+ | 8.13+ | 8.11+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/onedrive)                                |
| [Oracle](/reference/search-connectors/es-connectors-oracle.md)| **GA** | - | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/oracle)                                  |
| [Outlook](/reference/search-connectors/es-connectors-outlook.md)| **GA** | - | 8.11+ | 8.13+ | 8.14+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/outlook)                                 |
| [PostgreSQL](/reference/search-connectors/es-connectors-postgresql.md)| **GA** | 8.11+ | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/postgresql)                              |
| [Redis](/reference/search-connectors/es-connectors-redis.md)| **Preview** | - | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/redis)                                   |
| [Amazon S3](/reference/search-connectors/es-connectors-s3.md)| **GA** | 8.12+ | 8.11+ | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/s3)                                      |
| [Salesforce](/reference/search-connectors/es-connectors-salesforce.md)| **GA** | 8.12+ | 8.11+ | 8.13+ | 8.13+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/salesforce)                              |
| [ServiceNow](/reference/search-connectors/es-connectors-servicenow.md)| **GA** | 8.10+ | 8.11+ | 8.13+ | 8.13+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/servicenow)                              |
| [Sharepoint Online](/reference/search-connectors/es-connectors-sharepoint-online.md)| **GA** | 8.9+ | 8.9+ | 8.9+ | 8.9+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/sharepoint/sharepoint_online)                       |
| [Sharepoint Server](/reference/search-connectors/es-connectors-sharepoint.md)| **Beta** | - | 8.11+ | 8.13+ | 8.15+ | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/sharepoint/sharepoint_server)                       |
| [Slack](/reference/search-connectors/es-connectors-slack.md)| **Preview** | - | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/slack)                                   |
| [Teams](/reference/search-connectors/es-connectors-teams.md)| **Preview** | - | - | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/microsoft_teams)                                   |
| [Zoom](/reference/search-connectors/es-connectors-zoom.md)| **Preview** | - | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/zoom)                                    |

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

The first decision you need to make before deploying a connector is which third party service (data source) you want to sync to {{es}}. See the list of [available connectors](#available-connectors).

Note that each data source will have specific prerequisites you’ll need to meet to authorize the connector to access its data. For example, certain data sources may require you to create an OAuth application, or create a service account. You’ll need to check the [individual connector documentation](connector-reference.md) for these details.
