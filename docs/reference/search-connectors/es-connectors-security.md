---
navigation_title: "Security"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-security.html
---

# Connectors security [es-connectors-security]


This document describes security considerations for connectors.

Self-managed deployments require more upfront work to ensure strong security. Refer to [Secure the Elastic Stack^](docs-content://deploy-manage/security.md) in the Elasticsearch documentation for more information.


## Access to credentials [es-native-connectors-security-connections]

Credentials for the data source — such as API keys or username/password pair— are stored in your deployment’s `.elastic-connectors` Elasticsearch index. Therefore, the credentials are visible to all Elastic users with the `read` [indices privilege^](/reference/elasticsearch/security-privileges.md) for that index. By default, the following Elastic users have this privilege: the `elastic` superuser and the `kibana_system` user. Enterprise Search service account tokens can also read the `.elastic-connectors` index.


% ## Access to internally stored API keys [es-native-connectors-security-api-key]

% API keys for Elastic managed connectors are stored in the internal system index `.connector-secrets`. Access to this index is restricted to authorized API calls only. The cluster privilege `write_connector_secrets` is required to store or update secrets through the API. Only the Enterprise Search instance has permission to read from this index.

### Document-level security [es-native-connectors-security-dls]

Document-level security is available for a subset of connectors. DLS is available by default for the following connectors:

* [Confluence](/reference/search-connectors/es-connectors-confluence.md)
* [Dropbox](/reference/search-connectors/es-connectors-dropbox.md)
* [Jira](/reference/search-connectors/es-connectors-jira.md) (including Jira Data Center)
* [GitHub](/reference/search-connectors/es-connectors-github.md)
* [Gmail](/reference/search-connectors/es-connectors-gmail.md)
* [Google Drive](/reference/search-connectors/es-connectors-google-drive.md)
* [Network Drive](/reference/search-connectors/es-connectors-network-drive.md)
* [OneDrive](/reference/search-connectors/es-connectors-onedrive.md)
* [Outlook](/reference/search-connectors/es-connectors-outlook.md)
* [Salesforce](/reference/search-connectors/es-connectors-salesforce.md)
* [SharePoint Online](/reference/search-connectors/es-connectors-sharepoint-online.md)
* [SharePoint Server](/reference/search-connectors/es-connectors-sharepoint.md)
* [ServiceNow](/reference/search-connectors/es-connectors-servicenow.md)

Learn more about this feature in [*Document level security*](/reference/search-connectors/document-level-security.md), including availability and prerequisites.


## Access to documents [es-native-connectors-security-deployment]

Data synced from your data source are stored as documents in the Elasticsearch index you created. This data is visible to all Elastic users with the `read` [indices privilege^](/reference/elasticsearch/security-privileges.md) for that index. Be careful to ensure that access to this index is *at least* as restrictive as access to the original data source.


## Encryption [es-native-connectors-security-encryption]

Elastic Cloud automatically encrypts data at rest. Data in transit is automatically encrypted using `https`.

Self-managed deployments must implement encryption at rest. See [Configure security for the Elastic Stack](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md) in the Elasticsearch documentation for more information.

