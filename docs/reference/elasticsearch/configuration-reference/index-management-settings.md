---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-management-settings.html
applies_to:
  deployment:
    ess:
    self:
---

# Index management settings [index-management-settings]

You can use the following cluster settings to enable or disable index management features.

$$$auto-create-index$$$

`action.auto_create_index` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) [Automatically create an index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) if it doesn’t already exist and apply any configured index templates. Defaults to `true`.

$$$action-destructive-requires-name$$$

`action.destructive_requires_name` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) When set to `true`, you must specify the index name to [delete an index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete). It is not possible to delete all indices with `_all` or use wildcards. Defaults to `true`.

$$$cluster-indices-close-enable$$$

`cluster.indices.close.enable` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Enables [closing of open indices](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-close) in {{es}}. If `false`, you cannot close open indices. Defaults to `true` for versions 7.2.0 and later, and to `false` for previous versions. In versions 7.1 and below, closed indices represent a data loss risk: if you close an index, it is not included in snapshots and you will not be able to restore the data. Similarly, closed indices are not included when you make cluster configuration changes, such as scaling to a different capacity, failover, and many other operations. Lastly, closed indices can lead to inaccurate disk space counts.

    ::::{warning}
    For versions 7.1 and below, closed indices represent a data loss risk. Enable this setting only temporarily for these versions.
    ::::

    ::::{note}
    Closed indices still consume a significant amount of disk space.
    ::::


$$$stack-templates-enabled$$$

`stack.templates.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) If `true`, enables built-in index and component templates. [{{agent}}](docs-content://reference/fleet/index.md) uses these templates to create data streams. If `false`, {{es}} disables these index and component templates. Defaults to `true`.

::::{note}
It is not recommended to disable the built-in stack templates, as some functionality of {{es}} or Kibana will not work correctly when disabled. Features like log and metric collection, as well as Kibana reporting, may malfunction without the built-in stack templates. Stack templates should only be disabled temporarily, if necessary, to resolve upgrade issues, then re-enabled after any issues have been resolved.
::::


This setting affects the following built-in index templates:

* `.kibana-reporting*`
* `logs-*-*`
* `metrics-*-*`
* `synthetics-*-*`
* `profiling-*`
* `security_solution-*-*`

This setting also affects the following built-in component templates:

* `kibana-reporting@settings`
* `logs@mappings`
* `logs@settings`
* `metrics@mappings`
* `metrics@settings`
* `metrics@tsdb-settings`
* `synthetics@mapping`
* `synthetics@settings`

### Universal Profiling settings

The following settings for Elastic Universal Profiling are supported:

`xpack.profiling.enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   *Version 8.7.0+*: Specifies whether the Universal Profiling Elasticsearch plugin is enabled. Defaults to *true*.

`xpack.profiling.templates.enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   *Version 8.9.0+*: Specifies whether Universal Profiling related index templates should be created on startup. Defaults to *false*.


## Reindex settings [reindex-settings]

$$$reindex-remote-whitelist$$$

`reindex.remote.whitelist` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies the hosts that can be [reindexed from remotely](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex). Expects a YAML array of `host:port` strings. Consists of a comma-delimited list of `host:port` entries. Defaults to `["\*.io:*", "\*.com:*"]`.

`reindex.ssl.certificate` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies the path to the PEM encoded certificate (or certificate chain) to be used for HTTP client authentication (if required by the remote cluster) This setting requires that `reindex.ssl.key` also be set. You cannot specify both `reindex.ssl.certificate` and `reindex.ssl.keystore.path`.

`reindex.ssl.certificate_authorities` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   List of paths to PEM encoded certificate files that should be trusted. You cannot specify both `reindex.ssl.certificate_authorities` and `reindex.ssl.truststore.path`.

`reindex.ssl.key` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies the path to the PEM encoded private key associated with the certificate used for client authentication (`reindex.ssl.certificate`). You cannot specify both `reindex.ssl.key` and `reindex.ssl.keystore.path`.

`reindex.ssl.key_passphrase` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies the passphrase to decrypt the PEM encoded private key (`reindex.ssl.key`) if it is encrypted.

    :::{admonition} Deprecated in 7.17.0
    Prefer `reindex.ssl.secure_key_passphrase` instead. Cannot be used with `reindex.ssl.secure_key_passphrase`.
    :::

`reindex.ssl.keystore.key_password` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The password for the key in the keystore (`reindex.ssl.keystore.path`). Defaults to the keystore password.

    :::{admonition} Deprecated in 7.17.0
    Prefer `reindex.ssl.keystore.secure_key_password` instead. This setting cannot be used with `reindex.ssl.keystore.secure_key_password`.
    :::

`reindex.ssl.keystore.password` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The password to the keystore (`reindex.ssl.keystore.path`).

    :::{admonition} Deprecated in 7.17.0
    Prefer `reindex.ssl.keystore.secure_password` instead. This setting cannot be used with `reindex.ssl.keystore.secure_password`.
    :::

`reindex.ssl.keystore.path` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies the path to the keystore that contains a private key and certificate to be used for HTTP client authentication (if required by the remote cluster). This keystore can be in "JKS" or "PKCS#12" format. You cannot specify both `reindex.ssl.key` and `reindex.ssl.keystore.path`.

`reindex.ssl.keystore.type` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The type of the keystore (`reindex.ssl.keystore.path`). Must be either `jks` or `PKCS12`. If the keystore path ends in ".p12", ".pfx" or "pkcs12", this setting defaults to `PKCS12`. Otherwise, it defaults to `jks`.

`reindex.ssl.secure_key_passphrase` ([Secure](docs-content://deploy-manage/security/secure-settings.md)) ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies the passphrase to decrypt the PEM encoded private key (`reindex.ssl.key`) if it is encrypted. Cannot be used with `reindex.ssl.key_passphrase`.

`reindex.ssl.keystore.secure_key_password` ([Secure](docs-content://deploy-manage/security/secure-settings.md)) ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The password for the key in the keystore (`reindex.ssl.keystore.path`). Defaults to the keystore password. This setting cannot be used with `reindex.ssl.keystore.key_password`.

`reindex.ssl.keystore.secure_password` ([Secure](docs-content://deploy-manage/security/secure-settings.md)) ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The password to the keystore (`reindex.ssl.keystore.path`). This setting cannot be used with `reindex.ssl.keystore.password`.

`reindex.ssl.truststore.password` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The password to the truststore (`reindex.ssl.truststore.path`).

    :::{admonition} Deprecated in 7.17.0
    Prefer `reindex.ssl.truststore.secure_password` instead. This setting cannot be used with `reindex.ssl.truststore.secure_password`.
    :::

`reindex.ssl.truststore.path` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The path to the Java Keystore file that contains the certificates to trust. This keystore can be in "JKS" or "PKCS#12" format. You cannot specify both `reindex.ssl.certificate_authorities` and `reindex.ssl.truststore.path`.

`reindex.ssl.truststore.secure_password` ([Secure](docs-content://deploy-manage/security/secure-settings.md)) ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The password to the truststore (`reindex.ssl.truststore.path`). This setting cannot be used with `reindex.ssl.truststore.password`.

`reindex.ssl.truststore.type` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   The type of the truststore (`reindex.ssl.truststore.path`). Must be either `jks` or `PKCS12`. If the truststore path ends in ".p12", ".pfx" or "pkcs12", this setting defaults to `PKCS12`. Otherwise, it defaults to `jks`.

`reindex.ssl.verification_mode` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Indicates the type of verification to protect against man in the middle attacks and certificate forgery. One of `full` (verify the hostname and the certificate path), `certificate` (verify the certificate path, but not the hostname) or `none` (perform no verification - this is strongly discouraged in production environments). Defaults to `full`.