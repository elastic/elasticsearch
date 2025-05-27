---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-api-compatibility.html
applies_to:
  stack: all
navigation_title: Compatibility
---

# Elasticsearch API compatibility [rest-api-compatibility]

To help REST clients mitigate the impact of non-compatible (breaking) API changes, {{es}} provides a per-request, opt-in API compatibility mode.

{{es}} REST APIs are generally stable across versions. However, some improvements require changes that are not compatible with previous versions.

When an API is targeted for removal or is going to be changed in a non-compatible way, the original API is deprecated for one or more releases. Using the original API triggers a deprecation warning in the logs. This enables you to review the deprecation logs  and take the appropriate actions before upgrading. However, in some cases it is difficult to identify all places where deprecated APIs are being used. This is where REST API compatibility can help.

When you request REST API compatibility, {{es}} attempts to honor the previous REST API version. {{es}} attempts to apply the most compatible URL, request body, response body, and HTTP parameters.

For compatible APIs, this has no effect— it only impacts calls to APIs that have breaking changes from the previous version. An error can still be returned in compatibility mode if {{es}} cannot automatically resolve the incompatibilities.

::::{important}
REST API compatibility does not guarantee the same behavior as the prior version. It instructs {{es}} to automatically resolve any incompatibilities so the request can be processed instead of returning an error.
::::


REST API compatibility should be a bridge to smooth out the upgrade process, not a long term strategy. REST API compatibility is only honored across one major version: honor 8.x requests/responses from 9.x.

When you submit requests using REST API compatibility and {{es}} resolves the incompatibility, a message is written to the deprecation log with the category "compatible_api". Review the deprecation log to identify any gaps in usage and fully supported features.


## Requesting REST API compatibility [request-rest-api-compatibility]

REST API compatibility is implemented per request via the Accept and/or Content-Type headers.

For example:

```text
Accept: "application/vnd.elasticsearch+json;compatible-with=8"
Content-Type: "application/vnd.elasticsearch+json;compatible-with=8"
```

The Accept header is always required and the Content-Type header is only required when a body is sent with the request. The following values are valid when communicating with a 8.x or 9.x {{es}} server:

```text
"application/vnd.elasticsearch+json;compatible-with=8"
"application/vnd.elasticsearch+yaml;compatible-with=8"
"application/vnd.elasticsearch+smile;compatible-with=8"
"application/vnd.elasticsearch+cbor;compatible-with=8"
```

The [officially supported {{es}} clients](https://www.elastic.co/guide/en/elasticsearch/client/index.html) can enable REST API compatibility for all requests.

To enable REST API compatibility for all requests received by {{es}} set the environment variable `ELASTIC_CLIENT_APIVERSIONING` to true.


## REST API compatibility workflow [_rest_api_compatibility_workflow]

To leverage REST API compatibility during an upgrade from the last 8.x to {{version}}:

1. Upgrade your [{{es}} clients](https://www.elastic.co/guide/en/elasticsearch/client/index.html) to the latest 8.x version and enable REST API compatibility.
2. Use the [Upgrade Assistant](docs-content://deploy-manage/upgrade/prepare-to-upgrade/upgrade-assistant.md) to review all critical issues and explore the deprecation logs. Some critical issues might be mitigated by REST API compatibility.
3. Resolve all critical issues before proceeding with the upgrade.
4. Upgrade Elasticsearch to {{version}}.
5. Review the deprecation logs for entries with the category `compatible_api`. Review the workflow associated with the requests that relied on compatibility mode.
6. Upgrade your {{es}} clients to 9.x and resolve compatibility issues manually where needed.

