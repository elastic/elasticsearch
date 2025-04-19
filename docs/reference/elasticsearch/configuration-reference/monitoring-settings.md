---
navigation_title: "Monitoring settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/monitoring-settings.html
applies_to:
  deployment:
    ess:
    self:
---

# Monitoring settings in {{es}} [monitoring-settings]


::::{admonition} Deprecated in 7.16.
:class: warning

Using the {{es}} Monitoring plugin to collect and ship monitoring data is deprecated. {{agent}} and {{metricbeat}} are the recommended methods for collecting and shipping monitoring data to a monitoring cluster. If you previously configured legacy collection methods, you should migrate to using [{{agent}}](docs-content://deploy-manage/monitor/stack-monitoring/collecting-monitoring-data-with-elastic-agent.md) or [{{metricbeat}}](docs-content://deploy-manage/monitor/stack-monitoring/collecting-monitoring-data-with-metricbeat.md) collection methods.
::::


By default, {{es}} {{monitor-features}} are enabled but data collection is disabled. To enable data collection, use the `xpack.monitoring.collection.enabled` setting.

Except where noted otherwise, these settings can be dynamically updated on a live cluster with the [cluster-update-settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) API.

To adjust how monitoring data is displayed in the monitoring UI, configure [`xpack.monitoring` settings](kibana://reference/configuration-reference/monitoring-settings.md) in `kibana.yml`. To control how monitoring data is collected from {{ls}}, configure monitoring settings in `logstash.yml`.

For more information, see [Monitor a cluster](docs-content://deploy-manage/monitor.md).


### General monitoring settings [general-monitoring-settings]

`xpack.monitoring.enabled`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting))

    :::{admonition} Deprecated in 7.8.0
    This deprecated setting has no effect.
    :::

### Monitoring collection settings [monitoring-collection-settings]

$$$monitoring-settings-description$$$
The `xpack.monitoring.collection` settings control how data is collected from your {{es}} nodes.

`xpack.monitoring.collection.enabled`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Set to `true` to enable the collection of monitoring data. When this setting is `false` (default), {{es}} monitoring data is not collected and all monitoring data from other sources such as {{kib}}, Beats, and {{ls}} is ignored.

$$$xpack-monitoring-collection-interval$$$

`xpack.monitoring.collection.interval` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   :::{admonition} Deprecated in 6.3.0
    This setting was deprecated in 6.3.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Setting to `-1` to disable data collection is no longer supported beginning with 7.0.0.

    Controls how often data samples are collected. Defaults to `10s`. If you modify the collection interval, set the `xpack.monitoring.min_interval_seconds` option in `kibana.yml` to the same value.


`xpack.monitoring.elasticsearch.collection.enabled`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Controls whether statistics about your {{es}} cluster should be collected. Defaults to `true`. This is different from `xpack.monitoring.collection.enabled`, which allows you to enable or disable all monitoring collection. However, this setting simply disables the collection of {{es}} data while still allowing other data (e.g., {{kib}}, {{ls}}, Beats, or APM Server monitoring data) to pass through this cluster.

`xpack.monitoring.collection.cluster.stats.timeout`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Timeout for collecting the cluster statistics, in [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). Defaults to `10s`.

`xpack.monitoring.collection.node.stats.timeout`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Timeout for collecting the node statistics, in [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). Defaults to `10s`.

`xpack.monitoring.collection.indices`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Controls which indices the {{monitor-features}} collect data from. Defaults to all indices. Specify the index names as a comma-separated list, for example `test1,test2,test3`. Names can include wildcards, for example `test*`. You can explicitly exclude indices by prepending `-`. For example `test*,-test3` will monitor all indexes that start with `test` except for `test3`. System indices like .security* or .kibana* always start with a `.` and generally should be monitored. Consider adding `.*` to the list of indices ensure monitoring of system indices. For example: `.*,test*,-test3`

`xpack.monitoring.collection.index.stats.timeout`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Timeout for collecting index statistics, in [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). Defaults to `10s`.

`xpack.monitoring.collection.index.recovery.active_only`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Controls whether or not all recoveries are collected. Set to `true` to collect only active recoveries. Defaults to `false`.

`xpack.monitoring.collection.index.recovery.timeout`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Timeout for collecting the recovery information, in [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). Defaults to `10s`.

`xpack.monitoring.collection.min_interval_seconds` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies the minimum number of seconds that a time bucket in a chart can represent. If you modify the `xpack.monitoring.collection.interval`, use the same value in this setting.

    Defaults to `10` (10 seconds).

$$$xpack-monitoring-history-duration$$$

`xpack.monitoring.history.duration` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Retention duration beyond which the indices created by a monitoring exporter are automatically deleted, in [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). Defaults to `7d` (7 days).

    This setting has a minimum value of `1d` (1 day) to ensure that something is being monitored and it cannot be disabled.

    ::::{important}
    This setting currently impacts only `local`-type exporters. Indices created using the `http` exporter are not deleted automatically.
    ::::


`xpack.monitoring.exporters`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Configures where the agent stores monitoring data. By default, the agent uses a local exporter that indexes monitoring data on the cluster where it is installed. Use an HTTP exporter to send data to a separate monitoring cluster. For more information, see [Local exporter settings](#local-exporter-settings), [HTTP exporter settings](#http-exporter-settings), and [How it works](docs-content://deploy-manage/monitor/stack-monitoring.md).


### Local exporter settings [local-exporter-settings]

The `local` exporter is the default exporter used by {{monitor-features}}. As the name is meant to imply, it exports data to the *local* cluster, which means that there is not much needed to be configured.

If you do not supply *any* exporters, then the {{monitor-features}} automatically create one for you. If any exporter is provided, then no default is added.

```yaml
xpack.monitoring.exporters.my_local:
  type: local
```

`type`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    The value for a Local exporter must always be `local` and it is required.

`use_ingest`
:   Whether to supply a placeholder pipeline to the cluster and a pipeline processor with every bulk request. The default value is `true`. If disabled, then it means that it will not use pipelines, which means that a future release cannot automatically upgrade bulk requests to future-proof them.

`cluster_alerts.management.enabled`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    Whether to create cluster alerts for this cluster. The default value is `true`. To use this feature, {{watcher}} must be enabled. If you have a basic license, cluster alerts are not displayed.

`wait_master.timeout`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

Time to wait for the master node to setup `local` exporter for monitoring, in [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). After that wait period, the non-master nodes warn the user for possible missing configuration. Defaults to `30s`.

### HTTP exporter settings [http-exporter-settings]

The following lists settings that can be supplied with the `http` exporter. All settings are shown as what follows the name you select for your exporter:

```yaml
xpack.monitoring.exporters.my_remote:
  type: http
  host: ["host:port", ...]
```

`type`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    The value for an HTTP exporter must always be `http` and it is required.

`host`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    Host supports multiple formats, both as an array or as a single value. Supported formats include `hostname`, `hostname:port`, `http://hostname` `http://hostname:port`, `https://hostname`, and `https://hostname:port`. Hosts cannot be assumed. The default scheme is always `http` and the default port is always `9200` if not supplied as part of the `host` string.

    ```yaml
    xpack.monitoring.exporters:
      example1:
        type: http
        host: "10.1.2.3"
      example2:
        type: http
        host: ["http://10.1.2.4"]
      example3:
        type: http
        host: ["10.1.2.5", "10.1.2.6"]
      example4:
        type: http
        host: ["https://10.1.2.3:9200"]
    ```


`auth.username`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    The username is required if `auth.secure_password` is supplied.

`auth.secure_password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings)) The password for the `auth.username`.

`connection.timeout`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    Amount of time that the HTTP connection is supposed to wait for a socket to open for the request, in [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). The default value is `6s`.

`connection.read_timeout`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    Amount of time that the HTTP connection is supposed to wait for a socket to send back a response, in [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). The default value is `10 * connection.timeout` (`60s` if neither are set).

`ssl`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    Each HTTP exporter can define its own TLS / SSL settings or inherit them. See [{{monitoring}} TLS/SSL settings](#ssl-monitoring-settings).

`proxy.base_path`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    The base path to prefix any outgoing request, such as `/base/path` (e.g., bulk requests would then be sent as `/base/path/_bulk`). There is no default value.

`headers`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    Optional headers that are added to every request, which can assist with routing requests through proxies.

    ```yaml
    xpack.monitoring.exporters.my_remote:
      headers:
        X-My-Array: [abc, def, xyz]
        X-My-Header: abc123
    ```

    Array-based headers are sent `n` times where `n` is the size of the array. `Content-Type` and `Content-Length` cannot be set. Any headers created by the monitoring agent will override anything defined here.


`index.name.time_format`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    A mechanism for changing the default date suffix for daily monitoring indices. The default format is `yyyy.MM.dd`. For example, `.monitoring-es-7-2021.08.26`.

`use_ingest`
:   Whether to supply a placeholder pipeline to the monitoring cluster and a pipeline processor with every bulk request. The default value is `true`. If disabled, then it means that it will not use pipelines, which means that a future release cannot automatically upgrade bulk requests to future-proof them.

`cluster_alerts.management.enabled`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    Whether to create cluster alerts for this cluster. The default value is `true`. To use this feature, {{watcher}} must be enabled. If you have a basic license, cluster alerts are not displayed.

`cluster_alerts.management.blacklist`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    Prevents the creation of specific cluster alerts. It also removes any applicable watches that already exist in the current cluster.

    You can add any of the following watch identifiers to the list of blocked alerts:

    * `elasticsearch_cluster_status`
    * `elasticsearch_version_mismatch`
    * `elasticsearch_nodes`
    * `kibana_version_mismatch`
    * `logstash_version_mismatch`
    * `xpack_license_expiration`

    For example: `["elasticsearch_version_mismatch","xpack_license_expiration"]`.


## {{monitoring}} TLS/SSL settings [ssl-monitoring-settings]

You can configure the following TLS/SSL settings.

`xpack.monitoring.exporters.$NAME.ssl.supported_protocols`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Supported protocols with versions. Valid protocols: `SSLv2Hello`, `SSLv3`, `TLSv1`, `TLSv1.1`, `TLSv1.2`, `TLSv1.3`. If the JVM’s SSL provider supports TLSv1.3, the default is `TLSv1.3,TLSv1.2,TLSv1.1`. Otherwise, the default is `TLSv1.2,TLSv1.1`.

    {{es}} relies on your JDK’s implementation of SSL and TLS. View [Supported SSL/TLS versions by JDK version](docs-content://deploy-manage/security/supported-ssltls-versions-by-jdk-version.md) for more information.

    ::::{note}
    If `xpack.security.fips_mode.enabled` is `true`, you cannot use `SSLv2Hello` or `SSLv3`. See [FIPS 140-2](docs-content://deploy-manage/security/fips-140-2.md).
    ::::


`xpack.monitoring.exporters.$NAME.ssl.verification_mode`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Controls the verification of certificates.

    Defaults to `full`.

    **Valid values**:
    * `full`: Validates that the provided certificate: has an issue date that’s within the `not_before` and `not_after` dates; chains to a trusted Certificate Authority (CA); has a `hostname` or IP address that matches the names within the certificate.
    * `certificate`: Validates the provided certificate and verifies that it’s signed by a trusted authority (CA), but doesn’t check the certificate `hostname`.
    * `none`: Performs no certificate validation.

      ::::{important}
      Setting certificate validation to `none` disables many security benefits of SSL/TLS, which is very dangerous. Only set this value if instructed by Elastic Support as a temporary diagnostic mechanism when attempting to resolve TLS errors.
      ::::

`xpack.monitoring.exporters.$NAME.ssl.cipher_suites`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Supported cipher suites vary depending on which version of Java you use. For example, for version 12 the default value is `TLS_AES_256_GCM_SHA384`, `TLS_AES_128_GCM_SHA256`, `TLS_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256`, `TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384`, `TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA`, `TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA`, `TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA`, `TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA`, `TLS_RSA_WITH_AES_256_GCM_SHA384`, `TLS_RSA_WITH_AES_128_GCM_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA256`, `TLS_RSA_WITH_AES_128_CBC_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA`, `TLS_RSA_WITH_AES_128_CBC_SHA`.

    For more information, see Oracle’s [Java Cryptography Architecture documentation](https://docs.oracle.com/en/java/javase/11/security/oracle-providers.md#GUID-7093246A-31A3-4304-AC5F-5FB6400405E2).


### {{monitoring}} TLS/SSL key and trusted certificate settings [monitoring-tls-ssl-key-trusted-certificate-settings]

The following settings are used to specify a private key, certificate, and the trusted certificates that should be used when communicating over an SSL/TLS connection. A private key and certificate are optional and would be used if the server requires client authentication for PKI authentication.


### PEM encoded files [_pem_encoded_files]

When using PEM encoded files, use the following settings:

`xpack.monitoring.exporters.$NAME.ssl.key`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Path to a PEM encoded file containing the private key.

    If HTTP client authentication is required, it uses this file. You cannot use this setting and `ssl.keystore.path` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.key_passphrase`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The passphrase that is used to decrypt the private key. Since the key might not be encrypted, this value is optional

    :::{admonition} Deprecated in 7.17.0
    Prefer `ssl.secure_key_passphrase` instead.
    :::

    You cannot use this setting and `ssl.secure_key_passphrase` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.secure_key_passphrase`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The passphrase that is used to decrypt the private key. Since the key might not be encrypted, this value is optional.

`xpack.monitoring.exporters.$NAME.ssl.certificate`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the path for the PEM encoded certificate (or certificate chain) that is associated with the key.

    This setting can be used only if `ssl.key` is set.


`xpack.monitoring.exporters.$NAME.ssl.certificate_authorities`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) List of paths to PEM encoded certificate files that should be trusted.

    This setting and `ssl.truststore.path` cannot be used at the same time.



### Java keystore files [_java_keystore_files]

When using Java keystore files (JKS), which contain the private key, certificate and certificates that should be trusted, use the following settings:

`xpack.monitoring.exporters.$NAME.ssl.keystore.path`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore file that contains a private key and certificate.

    It must be either a Java keystore (jks) or a PKCS#12 file. You cannot use this setting and `ssl.key` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.keystore.password`
:   :::{admonition} Deprecated in 7.17.0
    Prefer `ssl.keystore.secure_password` instead.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The password for the keystore.

`xpack.monitoring.exporters.$NAME.ssl.keystore.secure_password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the keystore.

`xpack.monitoring.exporters.$NAME.ssl.keystore.key_password`
:   :::{admonition} Deprecated in 7.17.0
    Prefer `ssl.keystore.secure_key_password` instead.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The password for the key in the keystore. The default is the keystore password.

    You cannot use this setting and `ssl.keystore.secure_password` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.keystore.secure_key_password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the key in the keystore. The default is the keystore password.

`xpack.monitoring.exporters.$NAME.ssl.truststore.path`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore that contains the certificates to trust. It must be either a Java keystore (jks) or a PKCS#12 file.

    You cannot use this setting and `ssl.certificate_authorities` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.truststore.password`
:   :::{admonition} Deprecated in 7.17.0
    Prefer `ssl.truststore.secure_password` instead.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The password for the truststore.

    You cannot use this setting and `ssl.truststore.secure_password` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.truststore.secure_password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Secure](docs-content://deploy-manage/security/secure-settings.md)) Password for the truststore.


### PKCS#12 files [monitoring-pkcs12-files]

{{es}} can be configured to use PKCS#12 container files (`.p12` or `.pfx` files) that contain the private key, certificate and certificates that should be trusted.

PKCS#12 files are configured in the same way as Java keystore files:

`xpack.monitoring.exporters.$NAME.ssl.keystore.path`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore file that contains a private key and certificate.

    It must be either a Java keystore (jks) or a PKCS#12 file. You cannot use this setting and `ssl.key` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.keystore.type`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The format of the keystore file. It must be either `jks` or `PKCS12`. If the keystore path ends in ".p12", ".pfx", or ".pkcs12", this setting defaults to `PKCS12`. Otherwise, it defaults to `jks`.

`xpack.monitoring.exporters.$NAME.ssl.keystore.password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The password for the keystore.

    :::{admonition} Deprecated in 7.17.0
    Prefer `ssl.keystore.secure_password` instead.
    :::

`xpack.monitoring.exporters.$NAME.ssl.keystore.secure_password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the keystore.

`xpack.monitoring.exporters.$NAME.ssl.keystore.key_password`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The password for the key in the keystore. The default is the keystore password.

    :::{admonition} Deprecated in 7.17.0
    Prefer `ssl.keystore.secure_key_password` instead.
    :::

    You cannot use this setting and `ssl.keystore.secure_password` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.keystore.secure_key_password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the key in the keystore. The default is the keystore password.

`xpack.monitoring.exporters.$NAME.ssl.truststore.path`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore that contains the certificates to trust. It must be either a Java keystore (jks) or a PKCS#12 file.

    You cannot use this setting and `ssl.certificate_authorities` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.truststore.type`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Set this to `PKCS12` to indicate that the truststore is a PKCS#12 file.

`xpack.monitoring.exporters.$NAME.ssl.truststore.password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The password for the truststore.

    :::{admonition} Deprecated in 7.17.0
    Prefer `ssl.truststore.secure_password` instead.
    :::

    You cannot use this setting and `ssl.truststore.secure_password` at the same time.


`xpack.monitoring.exporters.$NAME.ssl.truststore.secure_password`
:   :::{admonition} Deprecated in 7.16.0
    This setting was deprecated in 7.16.0.
    :::

    ([Secure](docs-content://deploy-manage/security/secure-settings.md)) Password for the truststore.



