---
navigation_title: "Release notes"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/8.18/es-connectors-release-notes.html
---

# Connector release notes

:::{admonition} Enterprise Search is discontinued in Elastic 9.0.0
Please note that Enterprise Search is not available in Elastic 9.0+, including App Search, Workplace Search, the Elastic Web Crawler, and Elastic managed connectors.

If you are an Enterprise Search user and want to upgrade to Elastic 9.0, refer to [our Enterprise Search FAQ](https://www.elastic.co/resources/search/enterprise-search-faq#what-features-are-impacted-by-this-announcement).
It includes detailed steps, tooling, and resources to help you transition to supported alternatives in 9.x, such as Elasticsearch, the Open Web Crawler, and self-managed connectors.
:::

## 9.4.6 [connectors-9.4.6-release-notes]

### Fixes [connectors-9.4.6-fixes]
* Fix long-running syncs being falsely marked as idle when Elasticsearch was temporarily slow or refresh calls timed out. The connector service no longer forces an index refresh on every job status check, keeps the ingestion heartbeat alive through transient errors, and retries job status checks during active syncs. [#4368](https://github.com/elastic/connectors/pull/4368), [#4311](https://github.com/elastic/connectors/issues/4311)
* Fix long-running syncs failing permanently on transient non-JSON Elasticsearch bulk responses, such as `Client Closed Request`. Failed concurrent bulk tasks are no longer silently dropped. [#4385](https://github.com/elastic/connectors/pull/4385)
* Fix connectors running under Elastic Agent ignoring the Elasticsearch output `ssl.verification_mode` policy setting, which meant certificate verification was always enforced. [#4393](https://github.com/elastic/connectors/pull/4393), [#4084](https://github.com/elastic/connectors/issues/4084)

## 9.5.2 [connectors-9.5.2-release-notes]

### Fixes [connectors-9.5.2-fixes]
* Fix long-running syncs being falsely marked as idle when Elasticsearch was temporarily slow or refresh calls timed out. The connector service no longer forces an index refresh on every job status check, keeps the ingestion heartbeat alive through transient errors, and retries job status checks during active syncs. [#4367](https://github.com/elastic/connectors/pull/4367), [#4311](https://github.com/elastic/connectors/issues/4311)

## 9.5.1 [connectors-9.5.1-release-notes]

### Fixes [connectors-9.5.1-fixes]
* Fix Document Level Security for the Outlook connector, where content documents were indexed with identities that did not match the ones granted by the access control documents, so owners could not retrieve their own synced documents. [#4313](https://github.com/elastic/connectors/pull/4313), [#4290](https://github.com/elastic/connectors/issues/4290)
* Fix the SharePoint Online connector to skip the system list `SharePointHomeCacheList`, so syncs are no longer aborted by Unauthorized responses when fetching its attachments. [#4308](https://github.com/elastic/connectors/pull/4308)
* Fix Confluence connector Document Level Security to index effective page view restrictions by intersecting the child's and all ancestors' read restrictions, instead of over-granting through space-level permissions. [#4303](https://github.com/elastic/connectors/pull/4303), [#4095](https://github.com/elastic/connectors/issues/4095)

## 9.5.0 [connectors-9.5.0-release-notes]

### Features and enhancements [connectors-9.5.0-features-enhancements]
* Added the `elasticsearch.bulk.max_text_document_size` setting, a per-document size cap for non-binary documents sent to the Elasticsearch bulk sink (default 3 MiB; set to `0` to disable). Oversized text documents are skipped and logged instead of overwhelming the cluster. [#4013](https://github.com/elastic/connectors/pull/4013), [#14454](https://github.com/elastic/search-team/issues/14454)
* Trimmed Gmail messages to their body and a minimal set of headers before indexing, reducing indexed payload size and noise from raw message content. A new `include_full_raw_message` toggle (default `false`) restores the previous full-raw behavior. [#4031](https://github.com/elastic/connectors/pull/4031), [#1369](https://github.com/elastic/connectors/issues/1369)

### Fixes [connectors-9.5.0-fixes]
* Fixed the SharePoint Online connector to surface a clear, actionable error when role assignments are unauthorized while Document Level Security is enabled, naming the affected site and explaining how to grant `Sites.FullControl.All` or disable DLS. [#4266](https://github.com/elastic/connectors/pull/4266), [#3293](https://github.com/elastic/connectors/issues/3293)
* Fixed the Outlook connector to skip unexpected Exchange item types, unresolvable or inaccessible folders, and related edge cases with a warning instead of aborting the sync. [#4158](https://github.com/elastic/connectors/pull/4158)
* Fixed the Outlook connector aborting an entire sync when Exchange returned an unrecognised EWS element (for example a stray `EndTimeZone` alongside a calendar item). Such elements are now skipped with a warning and the rest of the mailbox continues to sync. [#4287](https://github.com/elastic/connectors/pull/4287)

## 9.4.5 [connectors-9.4.5-release-notes]

### Fixes [connectors-9.4.5-fixes]
* Fix Document Level Security for the Outlook connector, where content documents were indexed with identities that did not match the ones granted by the access control documents, so owners could not retrieve their own synced documents. [#4312](https://github.com/elastic/connectors/pull/4312), [#4290](https://github.com/elastic/connectors/issues/4290)
* Fix the SharePoint Online connector to skip the system list `SharePointHomeCacheList`, so syncs are no longer aborted by Unauthorized responses when fetching its attachments. [#4307](https://github.com/elastic/connectors/pull/4307)
* Fix Confluence connector Document Level Security to index effective page view restrictions by intersecting the child's and all ancestors' read restrictions, instead of over-granting through space-level permissions. [#4302](https://github.com/elastic/connectors/pull/4302), [#4095](https://github.com/elastic/connectors/issues/4095)
* Fix the Outlook connector aborting a sync when Exchange returned an unrecognised EWS element; such elements are now skipped with a warning and the rest of the mailbox continues to sync. [#4292](https://github.com/elastic/connectors/pull/4292)
* Fix the SharePoint Online connector to surface a clear, actionable error when role assignments are unauthorized while Document Level Security is enabled. [#4268](https://github.com/elastic/connectors/pull/4268), [#3293](https://github.com/elastic/connectors/issues/3293)
* Fix the Outlook connector to skip unexpected Exchange item types, unresolvable or inaccessible folders, and related edge cases with a warning instead of aborting the sync. [#4177](https://github.com/elastic/connectors/pull/4177), [#4158](https://github.com/elastic/connectors/pull/4158)

## 9.4.4 [connectors-9.4.4-release-notes]

### Fixes [connectors-9.4.4-fixes]
* Fix Slack connector float timestamp causing messages to be deleted on scheduled syncs. [#4168](https://github.com/elastic/connectors/pull/4168)
* Handle out-of-range BSON datetimes in the MongoDB connector to prevent sync failures. [#4155](https://github.com/elastic/connectors/pull/4155)
* Fix Outlook connector to dispatch Contacts by item type and harden folder and field assumptions. [#4151](https://github.com/elastic/connectors/pull/4151)
* Fix GitHub connector to propagate fetch errors instead of swallowing them. [#4135](https://github.com/elastic/connectors/pull/4135)
* Fix Outlook connector to harden sync against missing Exchange field values. [#4132](https://github.com/elastic/connectors/pull/4132)
* Drop unused `space.permissions` from content query in the Confluence Data Center and Server connector. [#4121](https://github.com/elastic/connectors/pull/4121)
* Fix Outlook connector to verify Exchange TLS with an in-memory CA, removing a cert-file race condition. [#4115](https://github.com/elastic/connectors/pull/4115)
* Fix Outlook connector to skip mailbox-less accounts and prevent SSL misconfiguration from aborting sync. [#4092](https://github.com/elastic/connectors/pull/4092)

## 9.3.8 [connectors-9.3.8-release-notes]

### Fixes [connectors-9.3.8-fixes]
* Fix Slack connector float timestamp causing messages to be deleted on scheduled syncs. [#4167](https://github.com/elastic/connectors/pull/4167)
* Handle out-of-range BSON datetimes in the MongoDB connector to prevent sync failures. [#4166](https://github.com/elastic/connectors/pull/4166)
* Fix Outlook connector to dispatch Contacts by item type and harden folder and field assumptions. [#4152](https://github.com/elastic/connectors/pull/4152)
* Fix GitHub connector to propagate fetch errors instead of swallowing them. [#4134](https://github.com/elastic/connectors/pull/4134)
* Fix Outlook connector to harden sync against missing Exchange field values. [#4131](https://github.com/elastic/connectors/pull/4131)
* Drop unused `space.permissions` from content query in the Confluence Data Center and Server connector. [#4120](https://github.com/elastic/connectors/pull/4120)
* Fix Outlook connector to verify Exchange TLS with an in-memory CA, removing a cert-file race condition. [#4114](https://github.com/elastic/connectors/pull/4114)
* Fix Outlook connector to skip mailbox-less accounts and prevent SSL misconfiguration from aborting sync. [#4091](https://github.com/elastic/connectors/pull/4091)

## 9.4.3 [connectors-9.4.3-release-notes]

### Fixes [connectors-9.4.3-fixes]
* Fix Confluence connector failing against Confluence Data Center / Server with HTTP 500 from `/rest/api/space?expand=permissions,history`. [#4049](https://github.com/elastic/connectors/pull/4049), [#4041](https://github.com/elastic/connectors/pull/4041)
* Fix sync jobs incorrectly reporting `indexed_document_count`, `indexed_document_volume`, and `deleted_document_count` as `0` despite successful ingestion. [#4055](https://github.com/elastic/connectors/pull/4055), [#4047](https://github.com/elastic/connectors/pull/4047)
* Fix the Jira connector failing to sync issues on Jira Server and Data Center deployments older than v10. [#4060](https://github.com/elastic/connectors/pull/4060), [#4059](https://github.com/elastic/connectors/pull/4059)
* Reduce Jira connector memory usage during full syncs and fix a case where a failed fetch could leave a sync waiting indefinitely. [#4077](https://github.com/elastic/connectors/pull/4077), [#4062](https://github.com/elastic/connectors/pull/4062)
* Fix Outlook connector crashing on localized Exchange servers. [#4068](https://github.com/elastic/connectors/pull/4068), [#4065](https://github.com/elastic/connectors/pull/4065)
* Fix Outlook connector failing on on-prem Exchange when AD users have no mail attribute. [#4080](https://github.com/elastic/connectors/pull/4080), [#4078](https://github.com/elastic/connectors/pull/4078)

## 9.3.7 [connectors-9.3.7-release-notes]

### Fixes [connectors-9.3.7-fixes]
* Fix Outlook Server connector aborting sync when an Active Directory user has a valid SMTP address but no Exchange mailbox (`ErrorNonExistentMailbox`); the account is now skipped with a warning and the sync continues. Also fix a crash (`NO_CERTIFICATE_OR_CRL_FOUND`) when SSL is enabled but no certificate is provided; the connector now falls back to unverified connections and logs a clear warning. [#4091](https://github.com/elastic/connectors/pull/4091), [#4085](https://github.com/elastic/connectors/pull/4085)

## 9.3.6 [connectors-9.3.6-release-notes]

### Fixes [connectors-9.3.6-fixes]
* The Jira connector now falls back to the deprecated `rest/api/2/search` endpoint for Jira Server and Data Center deployments, fixing syncs against versions older than v10 that do not support the newer `rest/api/3/search/jql` endpoint. [#4059](https://github.com/elastic/connectors/pull/4059), [#4058](https://github.com/elastic/connectors/issues/4058)
* Reduced the Jira connector's memory usage during full syncs and fixed a case where a failed fetch could leave a sync waiting indefinitely, both of which contributed to growing memory consumption in agentless deployments. [#4062](https://github.com/elastic/connectors/pull/4062), [#3914](https://github.com/elastic/connectors/issues/3914)
* Fixed sync jobs reporting an indexed document count of `0` even when documents were successfully ingested; the counts are now updated correctly when the bulk error monitor triggers mid-batch, and user-supplied `elasticsearch.bulk.error_monitor.*` settings are now honored. [#4047](https://github.com/elastic/connectors/pull/4047), [#3736](https://github.com/elastic/connectors/issues/3736)
* Fixed the Confluence connector failing against Confluence Data Center and Server with an HTTP 500 from `/rest/api/space`; the connector no longer requests the unused `permissions` and `history` expansions on Data Center and Server. [#4041](https://github.com/elastic/connectors/pull/4041)
* Fixed an Outlook connector crash on localized (non-English) Exchange servers by resolving the contacts and archive folders through locale-agnostic distinguished folder IDs instead of English display names. [#4065](https://github.com/elastic/connectors/pull/4065), [#4064](https://github.com/elastic/connectors/issues/4064)
* Fixed an Outlook connector crash on on-premises Exchange when Active Directory contained users without a mail attribute; those users are now skipped with a warning instead of aborting the sync. [#4078](https://github.com/elastic/connectors/pull/4078)

## 9.4.2 [connectors-9.4.2-release-notes]

### Features and enhancements [connectors-9.4.2-features-enhancements]
* Tuned default Elasticsearch ingestion settings to better protect content-heavy connectors against bulk timeouts. Applies to self-managed, agent-managed, and Agentless deployments. [#4009](https://github.com/elastic/connectors/pull/4009), [#14289](https://github.com/elastic/search-team/issues/14289), [#14452](https://github.com/elastic/search-team/issues/14452)

### Fixes [connectors-9.4.2-fixes]
* Fixed an issue where the Elasticsearch sink could dispatch bulk requests larger than the configured `chunk_max_mem_size`, triggering `413 Request Entity Too Large` errors or memory pressure on the cluster. [#4012](https://github.com/elastic/connectors/pull/4012), [#14453](https://github.com/elastic/search-team/issues/14453)
* Fixed a `TypeError: Issuer (iss) must be a string` crash that prevented GitHub App authentication from working with PyJWT 2.11.0 or later. [#4027](https://github.com/elastic/connectors/pull/4027), [#1881](https://github.com/elastic/sdh-search/issues/1881)
* Fixed a `ModuleNotFoundError: No module named 'pkg_resources'` crash that prevented the Microsoft SQL Server connector from starting a sync on the official Docker image. [#4015](https://github.com/elastic/connectors/pull/4015), [#4014](https://github.com/elastic/connectors/issues/4014)

## 9.4.1 [connectors-9.4.1-release-notes]

### Fixes [connectors-9.4.1-fixes]

* Fixes a bug for {{connectors-app}} Document Level Security, where the generated query filter used an incorrect subfield. [#4006](https://github.com/elastic/connectors/pull/4006), [#4005](https://github.com/elastic/connectors/issues/4005)

## 9.4.0 [connectors-9.4.0-release-notes]

### Fixes [connectors-9.4.0-fixes]
* Fixed a bug in the Network Drive connector where users from different domains with the same RID could inherit each other's document permissions. [#3973](https://github.com/elastic/connectors/pull/3973), [#3972](https://github.com/elastic/connectors/issues/3972)
* Fixed an issue with access control syncs crashing due to `noop` returned from `bulk` API was treated as a failure. [#3961](https://github.com/elastic/connectors/pull/3961), [#3957](https://github.com/elastic/connectors/issues/3957)
* Fixed a bug where using `id_columns` in advanced sync rules with mixed-case table or column names caused all documents to receive the same `_id`, resulting in document overwrites and only 1 document being indexed instead of the expected count. [#3885](https://github.com/elastic/connectors/pull/3885), [#3884](https://github.com/elastic/connectors/issues/3884)

## 9.3.5 [connectors-9.3.5-release-notes]

### Features and enhancements [connectors-9.3.5-features-enhancements]
* Tuned default Elasticsearch ingestion settings to better protect content-heavy connectors against bulk timeouts. Applies to self-managed, agent-managed, and Agentless deployments. [#4009](https://github.com/elastic/connectors/pull/4009), [#14289](https://github.com/elastic/search-team/issues/14289), [#14452](https://github.com/elastic/search-team/issues/14452)

### Fixes [connectors-9.3.5-fixes]
* Fixed a bug for {{connectors-app}} Document Level Security, where the generated query filter used an incorrect subfield. [#4006](https://github.com/elastic/connectors/pull/4006), [#4005](https://github.com/elastic/connectors/issues/4005)
* Fixed an issue where the Elasticsearch sink could dispatch bulk requests larger than the configured `chunk_max_mem_size`, triggering `413 Request Entity Too Large` errors or memory pressure on the cluster. [#4012](https://github.com/elastic/connectors/pull/4012), [#14453](https://github.com/elastic/search-team/issues/14453)
* Fixed a `TypeError: Issuer (iss) must be a string` crash that prevented GitHub App authentication from working with PyJWT 2.11.0 or later. [#4027](https://github.com/elastic/connectors/pull/4027), [#1881](https://github.com/elastic/sdh-search/issues/1881)

## 9.3.4 [connectors-9.3.4-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.3.3 [connectors-9.3.3-release-notes]
* Fixed a bug in Network Drive connector where users from different domains with the same RID could inherit each other's document permissions. [#3973](https://github.com/elastic/connectors/pull/3973), [#3972](https://github.com/elastic/connectors/issues/3972)

## 9.2.8 [connectors-9.2.8-release-notes]
* Fixed a bug in Network Drive connector where users from different domains with the same RID could inherit each other's document permissions. [#3973](https://github.com/elastic/connectors/pull/3973), [#3972](https://github.com/elastic/connectors/issues/3972)

## 9.3.2 [connectors-9.3.2-release-notes]

### Fixes [connectors-9.3.2-fixes]
* Fixed a bug where using `id_columns` in advanced sync rules with mixed-case table or column names caused all documents to receive the same `_id`, resulting in document overwrites and only 1 document being indexed instead of the expected count.[#3885](https://github.com/elastic/connectors/pull/3885),[#3884](https://github.com/elastic/connectors/issues/3884)
* Fixed an issue with access control syncs crashing due to `noop` returned from `bulk` API was treated as a failure. [#3961](https://github.com/elastic/connectors/pull/3961), [#3957](https://github.com/elastic/connectors/issues/3957)

## 9.2.7 [connectors-9.2.7-release-notes]

### Fixes [connectors-9.2.7-fixes]
* Fixed a bug where using `id_columns` in advanced sync rules with mixed-case table or column names caused all documents to receive the same `_id`, resulting in document overwrites and only 1 document being indexed instead of the expected count. [#3885](https://github.com/elastic/connectors/pull/3885),[#3884](https://github.com/elastic/connectors/issues/3884)
* Fixed an issue with access control syncs crashing due to `noop` returned from `bulk` API was treated as a failure. [#3961](https://github.com/elastic/connectors/pull/3961), [#3957](https://github.com/elastic/connectors/issues/3957)

## 9.3.1 [connectors-9.3.1-release-notes]

### Fixes [connectors-9.3.1-fixes]
* Fixed an issue where MultiService would enter an unresponsive state instead of shutting down cleanly when a managed service crashed with an unhandled exception. ([#3940](https://github.com/elastic/connectors/pull/3940),[#3939](https://github.com/elastic/connectors/issues/3939))

## 9.2.6 [connectors-9.2.6-release-notes]

### Fixes [connectors-9.2.6-fixes]
* Fixed an issue where MultiService would enter an unresponsive  state instead of shutting down cleanly when a managed service crashed with an unhandled exception. ([#3940](https://github.com/elastic/connectors/pull/3940), [#3939](https://github.com/elastic/connectors/issues/3939))

## 9.3.0 [connectors-9.3.0-release-notes]

### Fixes [connectors-9.3.0-fixes]
* Fixed a bug in the Network Drive connector that caused connections to SMB servers to close prematurely, leading to errors when multiple connections were made to the same host. [#3868](https://github.com/elastic/connectors/pull/3868), [#3873](https://github.com/elastic/connectors/pull/3873)
* Fixed a serialization error in the PostgreSQL connector when handling `INET`, `CIDR`, `UUID`, and geometric types.[#3900](https://github.com/elastic/connectors/pull/3900), [#3879](https://github.com/elastic/connectors/issues/3879)

### Features and enhancements [connectors-9.3.0-features-enhancements]
* Added a new GitLab connector to sync Projects, Issues, Epics, Merge Requests, Releases, and README files. [#3770](https://github.com/elastic/connectors/pull/3770) [#11093](https://github.com/elastic/search-team/issues/11093)

## 9.2.5 [connectors-9.2.5-release-notes]

### Fixes [connectors-9.2.5-fixes]
* Fixed a serialization error in the PostgreSQL connector when handling `INET`, `CIDR`, `UUID`, and geometric types. [#3900](https://github.com/elastic/connectors/pull/3900), [#3879](https://github.com/elastic/connectors/issues/3879)

## 9.2.4 [connectors-9.2.4-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.2.3 [connectors-9.2.3-release-notes]

### Fixes [connectors-9.2.3-fixes]
* Fixed a bug in the Network Drive connector that caused connections to SMB servers to close prematurely, leading to errors when multiple connections were made to the same host. [#3868](https://github.com/elastic/connectors/pull/3868), [#3873](https://github.com/elastic/connectors/pull/3873)

## 9.2.2 [connectors-9.2.2-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.2.1 [connectors-9.2.1-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.2.0 [connectors-9.2.0-release-notes]

### Features and enhancements [connectors-9.2.0-features-enhancements]
* Refactored pagination from OFFSET-based to keyset (primary-key) pagination in the MySQL connector. This delivers 3×+ faster syncs on large tables and modest gains on smaller ones. [#3719](https://github.com/elastic/connectors/pull/3719).

* Updated the Jira connector to use the new `/rest/api/3/search/jql` endpoint, ensuring compatibility with Jira’s latest API. [#3710](https://github.com/elastic/connectors/pull/3710).

## 9.1.10 [connectors-9.1.10-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.1.9 [connectors-9.1.9-release-notes]

### Fixes [connectors-9.1.9-fixes]
* Fixed a bug in the Network Drive connector that caused connections to SMB servers to close prematurely, leading to errors when multiple connections were made to the same host. [#3868](https://github.com/elastic/connectors/pull/3868), [#3873](https://github.com/elastic/connectors/pull/3873)

## 9.1.8 [connectors-9.1.8-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.1.7 [connectors-9.1.7-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.1.6 [connectors-9.1.6-release-notes]

### Features and enhancements [connectors-9.1.6-features-enhancements]
* Idle Github connectors no longer excessively query set-up repositories, which reduces the number of calls to GitHub each connector makes and makes users less likely to hit GitHub API quotas. [#3708](https://github.com/elastic/connectors/pull/3708)

* In the Sharepoint Online connector, /contentstorage/ URLs are no longer synced. [#3630](https://github.com/elastic/connectors/pull/3630)

## 9.1.5 [connectors-9.1.5-release-notes]

### Features and enhancements [connectors-9.1.5-features-enhancements]
* Refactored pagination from OFFSET-based to keyset (primary-key) pagination in the MySQL connector. This delivers 3×+ faster syncs on large tables and modest gains on smaller ones. [#3719](https://github.com/elastic/connectors/pull/3719).

* Updated the Jira connector to use the new `/rest/api/3/search/jql` endpoint, ensuring compatibility with Jira’s latest API. [#3710](https://github.com/elastic/connectors/pull/3710).

## 9.1.4 [connectors-9.1.4-release-notes]

### Features and enhancements [connectors-9.1.4-features-enhancements]
* Reduced API calls during field validation with caching, improving sync performance in Salesforce connector. [#3668](https://github.com/elastic/connectors/pull/3668).

## 9.1.3 [connectors-9.1.3-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.1.2 [connectors-9.1.2-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.1.1 [connectors-9.1.1-release-notes]

### Fixes [connectors-9.1.1-fixes]

:::{dropdown} Resolves missing access control for “Everyone Except External Users” in SharePoint connector

Permissions granted to the `Everyone Except External Users` group were previously ignored, causing incomplete access control metadata in documents. This occurred because the connector did not recognize the group’s login name format.
[#3577](https://github.com/elastic/connectors/pull/3577) resolves this issue by recognizing the group’s login format and correctly applying its permissions to document access control metadata.
:::

## 9.1.0 [connectors-9.1.0-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.0.8 [connectors-9.0.8-release-notes]

### Features and enhancements [connectors-9.0.8-features-enhancements]
* Refactored pagination from OFFSET-based to keyset (primary-key) pagination in the MySQL connector. This delivers 3×+ faster syncs on large tables and modest gains on smaller ones. [#3719](https://github.com/elastic/connectors/pull/3719).

* Updated the Jira connector to use the new `/rest/api/3/search/jql` endpoint, ensuring compatibility with Jira’s latest API. [#3710](https://github.com/elastic/connectors/pull/3710).

## 9.0.7 [connectors-9.0.7-release-notes]

### Features and enhancements [connectors-9.0.7-features-enhancements]
* Reduced API calls during field validation with caching, improving sync performance in Salesforce connector. [#3668](https://github.com/elastic/connectors/pull/3668).

## 9.0.6 [connectors-9.0.6-release-notes]
No changes since 9.0.5

## 9.0.5 [connectors-9.0.5-release-notes]

### Fixes [connectors-9.0.5-fixes]

:::{dropdown} Resolves missing access control for `Everyone Except External Users` in SharePoint connector
Permissions granted to the `Everyone Except External Users` group were previously ignored, causing incomplete access control metadata in documents. This occurred because the connector did not recognize the group’s login name format. [#3577](https://github.com/elastic/connectors/pull/3577) resolves this issue by recognizing the group’s login format and correctly applying its permissions to document access control metadata.
:::

## 9.0.4 [connectors-9.0.4-release-notes]
No changes since 9.0.3

## 9.0.3 [connectors-9.0.3-release-notes]

### Features and enhancements [connectors-9.0.3-features-enhancements]

Improve UUID handling by correctly parsing type 4 UUIDs and skipping unsupported type 3 with a warning. See [#3459](https://github.com/elastic/connectors/pull/3459).

## 9.0.2 [connectors-9.0.2-release-notes]
No changes since 9.0.1

## 9.0.1 [connectors-9.0.1-release-notes]
No changes since 9.0.0

## 9.0.0 [connectors-9.0.0-release-notes]

### Features and enhancements [connectors-9.0.0-features-enhancements]

* Switched the default ingestion pipeline from `ent-search-generic-ingestion` to `search-default-ingestion`. The pipelines are functionally identical; only the name has changed to align with the deprecation of Enterprise Search. [#3049](https://github.com/elastic/connectors/pull/3049)
* Removed opinionated index mappings and settings from Connectors. Going forward, indices will use Elastic’s default mappings and settings, rather than legacy App Search–optimized ones. To retain the previous behavior, create the index manually before pointing a connector to it. [#3013](https://github.com/elastic/connectors/pull/3013)

### Fixes [connectors-9.0.0-fixes]

* Fixed an issue where full syncs could delete newly ingested documents if the document ID from the third-party source was numeric. [#3031](https://github.com/elastic/connectors/pull/3031)
* Fixed a bug where the Confluence connector failed to download some blog post documents due to unexpected response formats. [#2984](https://github.com/elastic/connectors/pull/2984)
* Fixed a bug in the Outlook connector where deactivated users could cause syncs to fail. [#2967](https://github.com/elastic/connectors/pull/2967)
* Resolved an issue where Network Drive connectors had trouble connecting to SMB 3.1.1 shares. [#2852](https://github.com/elastic/connectors/pull/2852)

% ## Breaking changes [connectors-9.0.0-breaking-changes]
% ## Deprications [connectorsch-9.0.0-deprecations]
% ## Known issues [connectors-9.0.0-known-issues]

