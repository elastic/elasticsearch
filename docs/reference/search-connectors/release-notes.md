---
navigation_title: "Release notes"
---

# Connector release notes

:::{admonition} Enterprise Search is discontinued in Elastic 9.0.0
Please note that Enterprise Search is not available in Elastic 9.0+, including App Search, Workplace Search, the Elastic Web Crawler, and Elastic managed connectors.

If you are an Enterprise Search user and want to upgrade to Elastic 9.0, refer to [our Enterprise Search FAQ](https://www.elastic.co/resources/search/enterprise-search-faq#what-features-are-impacted-by-this-announcement).
It includes detailed steps, tooling, and resources to help you transition to supported alternatives in 9.x, such as Elasticsearch, the Open Web Crawler, and self-managed connectors.
:::

## 9.0.1 [connectors-901-release-notes]
No changes since 9.0.0

## 9.0.0 [connectors-900-release-notes]

### Features and enhancements [connectors-900-features-enhancements]

* Switched the default ingestion pipeline from `ent-search-generic-ingestion` to `search-default-ingestion`. The pipelines are functionally identical; only the name has changed to align with the deprecation of Enterprise Search. [#3049](https://github.com/elastic/connectors/pull/3049)
* Removed opinionated index mappings and settings from Connectors. Going forward, indices will use Elastic’s default mappings and settings, rather than legacy App Search–optimized ones. To retain the previous behavior, create the index manually before pointing a connector to it. [#3013](https://github.com/elastic/connectors/pull/3013)

### Fixes [connectors-900-fixes]

* Fixed an issue where full syncs could delete newly ingested documents if the document ID from the third-party source was numeric. [#3031](https://github.com/elastic/connectors/pull/3031)
* Fixed a bug where the Confluence connector failed to download some blog post documents due to unexpected response formats. [#2984](https://github.com/elastic/connectors/pull/2984)
* Fixed a bug in the Outlook connector where deactivated users could cause syncs to fail. [#2967](https://github.com/elastic/connectors/pull/2967)
* Resolved an issue where Network Drive connectors had trouble connecting to SMB 3.1.1 shares. [#2852](https://github.com/elastic/connectors/pull/2852)

% ## Breaking changes [connectors-900-breaking-changes]
% ## Deprications [connectorsch-900-deprecations]
% ## Known issues [connectors-900-known-issues]

