---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes.html
---

# Elasticsearch breaking changes [elasticsearch-breaking-changes]
Before you upgrade, carefully review the Elasticsearch breaking changes and take the necessary steps to mitigate any issues.

To learn how to upgrade, check out <upgrade docs>.

% ## Next version [elasticsearch-nextversion-breaking-changes]
% **Release date:** Month day, year

## 9.1.0 [elasticsearch-910-breaking-changes]
**Release date:** April 01, 2025

Discovery-Plugins:
* Upgrade `discovery-ec2` to AWS SDK v2 [#122062](https://github.com/elastic/elasticsearch/pull/122062)

TLS:
* Drop `TLS_RSA` cipher support for JDK 24 [#123600](https://github.com/elastic/elasticsearch/pull/123600)


