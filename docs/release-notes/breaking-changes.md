---
navigation_title: "Breaking changes"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes.html
---

# Elasticsearch breaking changes [elasticsearch-breaking-changes]

Breaking changes can impact your Elastic applications, potentially disrupting normal operations. Before you upgrade, carefully review the Elasticsearch breaking changes and take the necessary steps to mitigate any issues.

If you are migrating from a version prior to version 9.0, you must first upgrade to the last 8.x version available. To learn how to upgrade, check out [Upgrade](docs-content://deploy-manage/upgrade.md).

To learn how to upgrade, check out <upgrade docs>.

% ## Next version [elasticsearch-nextversion-breaking-changes]

## 9.1.0 [elasticsearch-910-breaking-changes]

Discovery-Plugins:
* Upgrade `discovery-ec2` to AWS SDK v2 [#122062](https://github.com/elastic/elasticsearch/pull/122062)

Infra/Logging:
* Rename deprecation index template [#125606](https://github.com/elastic/elasticsearch/pull/125606) (issue: {es-issue}125445[#125445])


