---
mapped_pages:
  - https://www.elastic.co/guide/en/cloud/current/ec-add-user-settings.html#ec-es-elasticsearch-settings
---

# Elastic Cloud Hosted Elasticsearch settings [ec-add-user-settings]

Change how {{es}} runs by providing your own user settings. Elasticsearch Service appends these settings to each node’s `elasticsearch.yml` configuration file.

Elasticsearch Service automatically rejects `elasticsearch.yml` settings that could break your cluster. For a list of supported settings, check [Supported {{es}} settings](#ec-es-elasticsearch-settings).

::::{warning}
You can also update [dynamic cluster settings](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting) using {{es}}'s [update cluster settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings). However, Elasticsearch Service doesn’t reject unsafe setting changes made using this API. Use with caution.
::::


To add or edit user settings:

1. Log in to the [Elasticsearch Service Console](https://cloud.elastic.co?page=docs&placement=docs-body).
2. Find your deployment on the home page in the Elasticsearch Service card and select **Manage** to access it directly. Or, select **Hosted deployments** to go to the deployments page to view all of your deployments.

    On the deployments page you can narrow your deployments by name, ID, or choose from several other filters. To customize your view, use a combination of filters, or change the format from a grid to a list.

3. From your deployment menu, go to the **Edit** page.
4. In the **Elasticsearch** section, select **Manage user settings and extensions**.
5. Update the user settings.
6. Select **Save changes**.

::::{note}
In some cases, you may get a warning saying "User settings are different across Elasticsearch instances". To fix this issue, ensure that your user settings (including the comments sections and whitespaces) are identical across all Elasticsearch nodes (not only the data tiers, but also the Master, Machine Learning, and Coordinating nodes).
::::


## Supported {{es}} settings [ec-es-elasticsearch-settings]

Elasticsearch Service supports the following `elasticsearch.yml` settings.

### General settings [ec_general_settings]

The following general settings are supported:

$$$http-cors-settings$$$`http.cors.*`
:   Enables cross-origin resource sharing (CORS) settings for the [HTTP module](/reference/elasticsearch/configuration-reference/networking-settings.md).

    ::::{note}
    If your use case depends on the ability to receive CORS requests and you have a cluster that was provisioned prior to January 25th 2019, you must manually set `http.cors.enabled` to `true` and allow a specific set of hosts with `http.cors.allow-origin`. Applying these changes in your Elasticsearch configuration  allows cross-origin resource sharing requests.
    ::::


`http.compression`
:   Support for [HTTP compression](/reference/elasticsearch/configuration-reference/networking-settings.md) when possible (with Accept-Encoding). Defaults to `true`.

`transport.compress`
:   Configures [transport compression](/reference/elasticsearch/configuration-reference/networking-settings.md) for node-to-node traffic.

`transport.compression_scheme`
:   Configures [transport compression](/reference/elasticsearch/configuration-reference/networking-settings.md) for node-to-node traffic.

`repositories.url.allowed_urls`
:   Enables explicit allowing of [read-only URL repositories](docs-content://deploy-manage/tools/snapshot-and-restore/read-only-url-repository.md).

`reindex.remote.whitelist`
:   Explicitly allows the set of hosts that can be [reindexed from remotely](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex). Expects a YAML array of `host:port` strings. Consists of a comma-delimited list of `host:port` entries. Defaults to `["\*.io:*", "\*.com:*"]`.

`reindex.ssl.*`
:   To learn more on how to configure reindex SSL user settings, check [configuring reindex SSL parameters](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex).

`script.painless.regex.enabled`
:   Enables [regular expressions](/reference/scripting-languages/painless/brief-painless-walkthrough.md#modules-scripting-painless-regex) for the Painless scripting language.

`action.auto_create_index`
:   [Automatically create index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) if it doesn’t already exist.

`action.destructive_requires_name`
:   When set to `true`, users must [specify the index name](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete) to delete an index. It’s not possible to delete _all or use wildcards.

`xpack.notification.webhook.additional_token_enabled`
:   When set to `true`, {{es}} automatically sets a token which enables the bypassing of traffic filters for calls initiated by Watcher towards {{es}} or {{kib}}. The default is `false` and the feature is available starting with {{es}} version 8.7.1 and later.

    ::::{important}
    This setting only applies to the Watcher `webhook` action, not the `http` input action.
    ::::


`cluster.indices.close.enable`
:   Enables closing indices in Elasticsearch. Defaults to `true` for versions 7.2.0 and later, and to `false` for previous versions. In versions 7.1 and below, closed indices represent a data loss risk: if you close an index, it is not included in snapshots and you will not be able to restore the data. Similarly, closed indices are not included when you make cluster configuration changes, such as scaling to a different capacity, failover, and many other operations. Lastly, closed indices can lead to inaccurate disk space counts.

    ::::{warning}
    For versions 7.1 and below, closed indices represent a data loss risk. Enable this setting only temporarily for these versions.
    ::::


`azure.client.CLIENT_NAME.endpoint_suffix`
:   Allows providing the [endpoint_suffix client setting](docs-content://deploy-manage/tools/snapshot-and-restore/azure-repository.md#repository-azure-client-settings) for a non-internal Azure client used for snapshot/restore. Note that `CLIENT_NAME` should be replaced with the name of the created client.


### Circuit breaker settings [ec_circuit_breaker_settings]

The following circuit breaker settings are supported:

`indices.breaker.total.limit`
:   Configures [the parent circuit breaker settings](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#parent-circuit-breaker).

`indices.breaker.fielddata.limit`
:   Configures [the limit for the fielddata breaker](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#fielddata-circuit-breaker).

`indices.breaker.fielddata.overhead`
:   Configures [a constant that all field data estimations are multiplied with to determine a final estimation](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#fielddata-circuit-breaker).

`indices.breaker.request.limit`
:   Configures [the limit for the request breaker](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#request-circuit-breaker).

`indices.breaker.request.overhead`
:   Configures [a constant that all request estimations are multiplied by to determine a final estimation](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#request-circuit-breaker).


### Indexing pressure settings [ec_indexing_pressure_settings]

The following indexing pressure settings are supported:

`indexing_pressure.memory.limit`
:   Configures [the indexing pressure settings](/reference/elasticsearch/index-settings/pressure.md).


### X-Pack [ec_x_pack]

#### Version 8.5.3+, 7.x support in 7.17.8+ [ec_version_8_5_3_7_x_support_in_7_17_8]

`xpack.security.transport.ssl.trust_restrictions.x509_fields`
:   Specifies which field(s) from the TLS certificate is used to match for the restricted trust management that is used for remote clusters connections. This should only be set when a self managed cluster can not create certificates that follow the Elastic Cloud pattern. The default value is ["subjectAltName.otherName.commonName"], the Elastic Cloud pattern. "subjectAltName.dnsName" is also supported and can be configured in addition to or in replacement of the default.


#### All supported versions [ec_all_supported_versions]

`xpack.ml.inference_model.time_to_live`
:   Sets the duration of time that the trained models are cached. Check [{{ml-cap}} settings](/reference/elasticsearch/configuration-reference/machine-learning-settings.md).

`xpack.security.loginAssistanceMessage`
:   Adds a message to the login screen. Useful for displaying corporate messages.

`xpack.security.authc.anonymous.*`
:   To learn more on how to enable anonymous access, check [Enabling anonymous access](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/anonymous-access.md)

`xpack.notification.slack`
:   Configures [Slack notification settings](docs-content://explore-analyze/alerts-cases/watcher/actions-slack.md). Note that you need to add `secure_url` as a [secret value to the keystore](docs-content://deploy-manage/security/secure-settings.md).

`xpack.notification.pagerduty`
:   Configures [PagerDuty notification settings](docs-content://explore-analyze/alerts-cases/watcher/actions-pagerduty.md#configuring-pagerduty).

`xpack.watcher.trigger.schedule.engine`
:   Defines when the watch should start, based on date and time [Learn more](docs-content://explore-analyze/alerts-cases/watcher/schedule-types.md).

`xpack.notification.email.html.sanitization.*`
:   Enables [email notification settings](/reference/elasticsearch/configuration-reference/watcher-settings.md) to sanitize HTML elements in emails that are sent.

`xpack.monitoring.collection.interval`
:   Controls [how often data samples are collected](/reference/elasticsearch/configuration-reference/monitoring-settings.md#monitoring-collection-settings).

`xpack.monitoring.collection.min_interval_seconds`
:   Specifies the minimum number of seconds that a time bucket in a chart can represent. If you modify the `xpack.monitoring.collection.interval`, use the same value in this setting.

    Defaults to `10` (10 seconds).


$$$xpack-monitoring-history-duration$$$`xpack.monitoring.history.duration`
:   Sets the [retention duration](/reference/elasticsearch/configuration-reference/monitoring-settings.md#monitoring-collection-settings) beyond which the indices created by a monitoring exporter will be automatically deleted.

`xpack.watcher.history.cleaner_service.enabled`
:   Controls [whether old watcher indices are automatically deleted](/reference/elasticsearch/configuration-reference/watcher-settings.md#general-notification-settings).

`xpack.http.ssl.cipher_suites`
:   Controls the list of supported cipher suites for all outgoing TLS connections.

`xpack.security.authc.realms.saml.*`
:   To learn more on how to enable SAML and related user settings, check [secure your clusters with SAML](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/saml.md).

`xpack.security.authc.realms.oidc.*`
:   To learn more on how to enable OpenID Connect and related user settings, check [secure your clusters with OpenID Connect](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/openid-connect.md).

`xpack.security.authc.realms.kerberos.*`
:   To learn more on how to enable Kerberos and relate user settings, check [secure your clusters with Kerberos](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/kerberos.md).

`xpack.security.authc.realms.jwt.*`
:   To learn more on how to enable JWT and related user settings, check [secure your clusters with JWT](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/jwt.md).

::::{note}
All SAML, OpenID Connect, Kerberos, and JWT settings are allowlisted.
::::




### Search [ec_search]

The following search settings are supported:

* `search.aggs.rewrite_to_filter_by_filter`


### Disk-based shard allocation settings [shard-allocation-settings]

The following disk-based allocation settings are supported:

`cluster.routing.allocation.disk.threshold_enabled`
:   Enable or disable [disk allocation](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md) decider and defaults to `true`.

`cluster.routing.allocation.disk.watermark.low`
:   Configures [disk-based shard allocation’s low watermark](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md).

`cluster.routing.allocation.disk.watermark.high`
:   Configures [disk-based shard allocation’s high watermark](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md).

`cluster.routing.allocation.disk.watermark.flood_stage`
:   Configures [disk-based shard allocation’s flood_stage](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md).

::::{tip}
Remember to update user settings for alerts when performing a major version upgrade.
::::



### Enrich settings [ec_enrich_settings]

The following enrich settings are supported:

`enrich.cache_size`
:   Maximum number of searches to cache for enriching documents. Defaults to 1000. There is a single cache for all enrich processors in the cluster. This setting determines the size of that cache.

`enrich.coordinator_proxy.max_concurrent_requests`
:   Maximum number of concurrent multi-search requests to run when enriching documents. Defaults to 8.

`enrich.coordinator_proxy.max_lookups_per_request`
:   Maximum number of searches to include in a multi-search request when enriching documents. Defaults to 128.

`enrich.coordinator_proxy.queue_capacity`
:   coordinator queue capacity, defaults to max_concurrent_requests * max_lookups_per_request


### Audit settings [ec_audit_settings]

The following audit settings are supported:

`xpack.security.audit.enabled`
:   Enables auditing on Elasticsearch cluster nodes. Defaults to *false*.

`xpack.security.audit.logfile.events.include`
:   Specifies which events to include in the auditing output.

`xpack.security.audit.logfile.events.exclude`
:   Specifies which events to exclude from the output. No events are excluded by default.

`xpack.security.audit.logfile.events.emit_request_body`
:   Specifies whether to include the request body from REST requests on certain event types, for example *authentication_failed*. Defaults to *false*.

`xpack.security.audit.logfile.emit_node_name`
:   Specifies whether to include the node name as a field in each audit event. Defaults to *true*.

`xpack.security.audit.logfile.emit_node_host_address`
:   Specifies whether to include the node’s IP address as a field in each audit event. Defaults to *false*.

`xpack.security.audit.logfile.emit_node_host_name`
:   Specifies whether to include the node’s host name as a field in each audit event. Defaults to *false*.

`xpack.security.audit.logfile.emit_node_id`
:   Specifies whether to include the node ID as a field in each audit event. Defaults to *true*.

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.users`
:   A list of user names or wildcards. The specified policy will not print audit events for users matching these values.

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.realms`
:   A list of authentication realm names or wildcards. The specified policy will not print audit events for users in these realms.

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.roles`
:   A list of role names or wildcards. The specified policy will not print audit events for users that have these roles.

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.indices`
:   A list of index names or wildcards. The specified policy will not print audit events when all the indices in the event match these values.

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.actions`
:   A list of action names or wildcards. The specified policy will not print audit events for actions matching these values.

::::{note}
To enable auditing you must first [enable deployment logging](docs-content://deploy-manage/monitor/stack-monitoring/elastic-cloud-stack-monitoring.md).
::::



### Universal Profiling settings [ec_universal_profiling_settings]

The following settings for Elastic Universal Profiling are supported:

`xpack.profiling.enabled`
:   *Version 8.7.0+*: Specifies whether the Universal Profiling Elasticsearch plugin is enabled. Defaults to *true*.

`xpack.profiling.templates.enabled`
:   *Version 8.9.0+*: Specifies whether Universal Profiling related index templates should be created on startup. Defaults to *false*.
