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

## 9.2.1 [connectors-9.2.1-release-notes]
There are no new features, enhancements, fixes, known issues, or deprecations associated with this release.

## 9.2.0 [connectors-9.2.0-release-notes]

### Features and enhancements [connectors-9.2.0-features-enhancements]
* Refactored pagination from OFFSET-based to keyset (primary-key) pagination in the MySQL connector. This delivers 3×+ faster syncs on large tables and modest gains on smaller ones. [#3719](https://github.com/elastic/connectors/pull/3719).

* Updated the Jira connector to use the new `/rest/api/3/search/jql` endpoint, ensuring compatibility with Jira’s latest API. [#3710](https://github.com/elastic/connectors/pull/3710).

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

