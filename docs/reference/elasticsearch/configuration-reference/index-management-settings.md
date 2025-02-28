---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-management-settings.html
---

# Index management settings [index-management-settings]

You can use the following cluster settings to enable or disable index management features.

$$$auto-create-index$$$

`action.auto_create_index` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on {{ess}}")
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) [Automatically create an index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) if it doesn’t already exist and apply any configured index templates. Defaults to `true`.

$$$action-destructive-requires-name$$$

`action.destructive_requires_name` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on {{ess}}")
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) When set to `true`, you must specify the index name to [delete an index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete). It is not possible to delete all indices with `_all` or use wildcards. Defaults to `true`.

$$$cluster-indices-close-enable$$$

`cluster.indices.close.enable` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on {{ess}}")
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Enables [closing of open indices](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-close) in {{es}}. If `false`, you cannot close open indices. Defaults to `true`.

    ::::{note}
    Closed indices still consume a significant amount of disk space.
    ::::


$$$stack-templates-enabled$$$

`stack.templates.enabled`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) If `true`, enables built-in index and component templates. [{{agent}}](docs-content://reference/ingestion-tools/fleet/index.md) uses these templates to create data streams. If `false`, {{es}} disables these index and component templates. Defaults to `true`.

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



## Reindex settings [reindex-settings]

$$$reindex-remote-whitelist$$$

`reindex.remote.whitelist` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on {{ess}}")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the hosts that can be [reindexed from remotely](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex). Expects a YAML array of `host:port` strings. Consists of a comma-delimited list of `host:port` entries. Defaults to `["\*.io:*", "\*.com:*"]`.

`reindex.ssl.certificate`
:   Specifies the path to the PEM encoded certificate (or certificate chain) to be used for HTTP client authentication (if required by the remote cluster) This setting requires that `reindex.ssl.key` also be set. You cannot specify both `reindex.ssl.certificate` and `reindex.ssl.keystore.path`.

`reindex.ssl.certificate_authorities`
:   List of paths to PEM encoded certificate files that should be trusted. You cannot specify both `reindex.ssl.certificate_authorities` and `reindex.ssl.truststore.path`.

`reindex.ssl.key`
:   Specifies the path to the PEM encoded private key associated with the certificate used for client authentication (`reindex.ssl.certificate`). You cannot specify both `reindex.ssl.key` and `reindex.ssl.keystore.path`.

`reindex.ssl.key_passphrase`
:   Specifies the passphrase to decrypt the PEM encoded private key (`reindex.ssl.key`) if it is encrypted. [7.17.0] Prefer `reindex.ssl.secure_key_passphrase` instead. Cannot be used with `reindex.ssl.secure_key_passphrase`.

`reindex.ssl.keystore.key_password`
:   The password for the key in the keystore (`reindex.ssl.keystore.path`). Defaults to the keystore password. [7.17.0] Prefer `reindex.ssl.keystore.secure_key_password` instead. This setting cannot be used with `reindex.ssl.keystore.secure_key_password`.

`reindex.ssl.keystore.password`
:   The password to the keystore (`reindex.ssl.keystore.path`). [7.17.0] Prefer `reindex.ssl.keystore.secure_password` instead. This setting cannot be used with `reindex.ssl.keystore.secure_password`.

`reindex.ssl.keystore.path`
:   Specifies the path to the keystore that contains a private key and certificate to be used for HTTP client authentication (if required by the remote cluster). This keystore can be in "JKS" or "PKCS#12" format. You cannot specify both `reindex.ssl.key` and `reindex.ssl.keystore.path`.

`reindex.ssl.keystore.type`
:   The type of the keystore (`reindex.ssl.keystore.path`). Must be either `jks` or `PKCS12`. If the keystore path ends in ".p12", ".pfx" or "pkcs12", this setting defaults to `PKCS12`. Otherwise, it defaults to `jks`.

`reindex.ssl.secure_key_passphrase` ([Secure](docs-content://deploy-manage/security/secure-settings.md))
:   Specifies the passphrase to decrypt the PEM encoded private key (`reindex.ssl.key`) if it is encrypted. Cannot be used with `reindex.ssl.key_passphrase`.

`reindex.ssl.keystore.secure_key_password` ([Secure](docs-content://deploy-manage/security/secure-settings.md))
:   The password for the key in the keystore (`reindex.ssl.keystore.path`). Defaults to the keystore password. This setting cannot be used with `reindex.ssl.keystore.key_password`.

`reindex.ssl.keystore.secure_password` ([Secure](docs-content://deploy-manage/security/secure-settings.md))
:   The password to the keystore (`reindex.ssl.keystore.path`). This setting cannot be used with `reindex.ssl.keystore.password`.

`reindex.ssl.truststore.password`
:   The password to the truststore (`reindex.ssl.truststore.path`). [7.17.0] Prefer `reindex.ssl.truststore.secure_password` instead. This setting cannot be used with `reindex.ssl.truststore.secure_password`.

`reindex.ssl.truststore.path`
:   The path to the Java Keystore file that contains the certificates to trust. This keystore can be in "JKS" or "PKCS#12" format. You cannot specify both `reindex.ssl.certificate_authorities` and `reindex.ssl.truststore.path`.

`reindex.ssl.truststore.secure_password` ([Secure](docs-content://deploy-manage/security/secure-settings.md))
:   The password to the truststore (`reindex.ssl.truststore.path`). This setting cannot be used with `reindex.ssl.truststore.password`.

`reindex.ssl.truststore.type`
:   The type of the truststore (`reindex.ssl.truststore.path`). Must be either `jks` or `PKCS12`. If the truststore path ends in ".p12", ".pfx" or "pkcs12", this setting defaults to `PKCS12`. Otherwise, it defaults to `jks`.

`reindex.ssl.verification_mode`
:   Indicates the type of verification to protect against man in the middle attacks and certificate forgery. One of `full` (verify the hostname and the certificate path), `certificate` (verify the certificate path, but not the hostname) or `none` (perform no verification - this is strongly discouraged in production environments). Defaults to `full`.

