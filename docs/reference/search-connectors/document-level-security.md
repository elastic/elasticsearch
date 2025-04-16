---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-dls.html
---

# Document level security [es-dls]

Document level security (DLS) enables you to restrict access to documents in your Elasticsearch indices according to user and group permissions. This ensures search results only return authorized information for users, based on their permissions.


## Availability & prerequisites [es-dls-availability-prerequisites]

Support for DLS in Elastic connectors was introduced in version **8.9.0**.

::::{note}
This feature is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

::::


This feature is not available for all Elastic subscription levels. Refer to the subscriptions pages for [Elastic Cloud](https://www.elastic.co/subscriptions/cloud) and [Elastic Stack](https://www.elastic.co/subscriptions).

DLS is available by default when using the following Elastic connectors:

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

Note that our standalone products (App Search and Workplace Search) do not use this feature. Workplace Search has its own permissions management system.


## Learn more [es-dls-learn-more]

DLS documentation:

* [How DLS works](/reference/search-connectors/es-dls-overview.md)
* [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md)
* [DLS for SharePoint Online connector](/reference/search-connectors/es-connectors-sharepoint-online.md#es-connectors-sharepoint-online-client-configuration)



