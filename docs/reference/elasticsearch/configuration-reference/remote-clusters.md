---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/remote-clusters-settings.html
applies_to:
  stack: ga
  serverless: unavailable
products:
  - id: elasticsearch
---

# Remote cluster settings [remote-clusters-settings]

This document describes the client settings available when configuring a remote cluster connection. These settings control how your local cluster connects to and communicates with the remote cluster as a client. For a general overview of remote clusters and how to set them up, refer to [Remote clusters](docs-content://deploy-manage/remote-clusters.md).

For settings related to configuring the remote cluster server interface (used with the [API key-based security model](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md)), refer to [Remote cluster network settings](/reference/elasticsearch/configuration-reference/networking-settings.md#remote-cluster-network-settings).

## General remote cluster settings

The following settings apply to both [sniff mode](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md#sniff-mode) and [proxy mode](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md#proxy-mode). Settings that are specific to sniff mode and proxy mode are described separately.

`cluster.remote.<cluster_alias>.mode`
:   The mode used for a remote cluster connection. The only supported modes are `sniff` and `proxy`. The default is `sniff`. See [Connection modes](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md#sniff-proxy-modes) for further information about these modes, and [Sniff mode remote cluster settings](#remote-cluster-sniff-settings) and [Proxy mode remote cluster settings](#remote-cluster-proxy-settings) for further information about their settings.

`cluster.remote.initial_connect_timeout`
:   The time to wait for remote connections to be established when the node starts. The default is `30s`.

`remote_cluster_client` [role](/reference/elasticsearch/configuration-reference/node-settings.md#node-roles)
:   By default, any node in the cluster can act as a cross-cluster client and connect to remote clusters. To prevent a node from connecting to remote clusters, specify the [node.roles](/reference/elasticsearch/configuration-reference/node-settings.md#node-roles) setting in [`elasticsearch.yml`](docs-content://deploy-manage/stack-settings.md) and exclude `remote_cluster_client` from the listed roles. Search requests targeting remote clusters must be sent to a node that is allowed to act as a cross-cluster client. Other features such as {{ml}} [data feeds](/reference/elasticsearch/configuration-reference/machine-learning-settings.md#general-ml-settings), [transforms](/reference/elasticsearch/configuration-reference/transforms-settings.md#general-transform-settings), and [{{ccr}}](docs-content://deploy-manage/tools/cross-cluster-replication/set-up-cross-cluster-replication.md) require the `remote_cluster_client` role.

`cluster.remote.<cluster_alias>.skip_unavailable`
:   Per cluster boolean setting that allows to skip specific clusters when no nodes belonging to them are available and they are the target of a remote cluster request.

::::{important}
In {{es}} 8.15, the default value for `skip_unavailable` was changed from `false` to `true`. Before {{es}} 8.15, if you want a cluster to be treated as optional for a {{ccs}}, then you need to set that configuration. From {{es}} 8.15 forward, you need to set the configuration in order to make a cluster required for the {{ccs}}. Once you upgrade the local ("querying") cluster search coordinator node (the node you send CCS requests to) to 8.15 or later, any remote clusters that do not have an explicit setting for `skip_unavailable` will immediately change over to using the new default of true. This is true regardless of whether you have upgraded the remote clusters to 8.15, as the `skip_unavailable` search behavior is entirely determined by the setting on the local cluster where you configure the remotes.
::::


`cluster.remote.<cluster_alias>.transport.ping_schedule`
:   Sets the time interval between regular application-level ping messages that are sent to try and keep remote cluster connections alive. If set to `-1`, application-level ping messages to this remote cluster are not sent. If unset, application-level ping messages are sent according to the global `transport.ping_schedule` setting, which defaults to `-1` meaning that pings are not sent. It is preferable to correctly configure TCP keep-alives instead of configuring a `ping_schedule`, because TCP keep-alives are handled by the operating system and not by {{es}}. By default {{es}} enables TCP keep-alives on remote cluster connections. Remote cluster connections are transport connections so the `transport.tcp.*` [advanced settings](/reference/elasticsearch/configuration-reference/networking-settings.md#transport-settings) regarding TCP keep-alives apply to them.

`cluster.remote.<cluster_alias>.transport.compress`
:   Per-cluster setting that enables you to configure compression for requests to a specific remote cluster. The handling cluster will automatically compress responses to compressed requests. The setting options are `true`, `indexing_data`, and `false`. If unset, defaults to the behavior specified by the node-wide `transport.compress` setting. See the [documentation for the `transport.compress` setting](/reference/elasticsearch/configuration-reference/networking-settings.md#transport-settings-compress) for further information.

`cluster.remote.<cluster_alias>.transport.compression_scheme`
:   Per-cluster setting that enables you to configure the compression scheme for requests to a specific cluster if those requests are selected to be compressed by to the `cluster.remote.<cluster_alias>.transport.compress` setting. The handling cluster will automatically use the same compression scheme for responses as for the corresponding requests. The setting options are `deflate` and `lz4`. If unset, defaults to the behavior specified by the node-wide `transport.compression_scheme` setting. See the [documentation for the `transport.compression_scheme` setting](/reference/elasticsearch/configuration-reference/networking-settings.md#transport-settings-compression-scheme) for further information.

$$$remote-cluster-credentials-setting$$$

`cluster.remote.<cluster_alias>.credentials`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md), [Reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings)) Per-cluster setting for configuring [remote clusters with the API Key based model](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md). This setting takes the encoded value of a [cross-cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) and must be set in the [{{es}} keystore](docs-content://deploy-manage/security/secure-settings.md) on each node in the cluster. The presence (or not) of this setting determines which model a remote cluster uses. If present, the remote cluster uses the API key based model. Otherwise, it uses the certificate based model. If the setting is added, removed, or updated in the {{es}} keystore and reloaded via the [Nodes reload secure settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-reload-secure-settings) API, the cluster will automatically rebuild its connection to the remote.

## Sniff mode remote cluster settings [remote-cluster-sniff-settings]

To use [sniff mode](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md#sniff-mode) to connect to a remote cluster, set `cluster.remote.<cluster_alias>.mode: sniff` and then configure the following settings. You may also leave `cluster.remote.<cluster_alias>.mode` unset since `sniff` is the default mode.

`cluster.remote.<cluster_alias>.seeds`
:   The list of seed nodes used to sniff the remote cluster state.

`cluster.remote.<cluster_alias>.node_connections`
:   The number of gateway nodes to connect to for this remote cluster. The default is `3`.

$$$cluster-remote-node-attr$$$

`cluster.remote.node.attr`
:   A node attribute to filter out nodes that are eligible as a gateway node in the remote cluster. For instance a node can have a node attribute `node.attr.gateway: true` such that only nodes with this attribute will be connected to if `cluster.remote.node.attr` is set to `gateway`.


## Proxy mode remote cluster settings [remote-cluster-proxy-settings]

To use [proxy mode](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md#proxy-mode) to connect to a remote cluster, set `cluster.remote.<cluster_alias>.mode: proxy` and then configure the following settings.

`cluster.remote.<cluster_alias>.proxy_address`
:   The address used for all remote connections. A single address value, either an IP address or a fully qualified domain name (FQDN).

`cluster.remote.<cluster_alias>.proxy_socket_connections`
:   The number of socket connections to open per remote cluster. The default is `18`.

`cluster.remote.<cluster_alias>.server_name`
:   An optional hostname string which is sent in the `server_name` field of the TLS Server Name Indication extension if [TLS is enabled](docs-content://deploy-manage/security/secure-cluster-communications.md#encrypt-internode-communication). The TLS transport will fail to open remote connections if this field is not a valid hostname as defined by the TLS SNI specification.

## Remote cluster strong identity verification settings [remote-cluster-signing-settings]
```{applies_to}
deployment:
  stack: preview 9.3
```

The following settings are used to sign and verify cross-cluster API key requests when using the [API key-based security model](docs-content://deploy-manage/remote-clusters/security-models.md#api-key). These settings enable certificate-based signatures on cross-cluster requests to provide additional security by validating that requests originate from trusted clusters.

### Strong identity verification settings [remote-cluster-signature-verification]

The following global settings are used on the remote cluster to verify signatures from incoming cross-cluster requests. At least one of 
`cluster.remote.signing.certificate_authorities` or `cluster.remote.signing.truststore.path` must be configured to enable signature verification.

`cluster.remote.signing.certificate_authorities` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported onElastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) List of paths to PEM encoded certificate files that should be trusted when verifying signatures from cross-cluster requests. These certificates are used to establish trust with the signing certificates presented by querying clusters. You cannot use this setting and `cluster.remote.signing.truststore.path` at 
the same time.

`cluster.remote.signing.truststore.path` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported onElastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The path for the truststore that contains the certificates to trust when verifying signatures. It must be either a Java keystore (jks) or a PKCS#12 file. You cannot use this setting and `cluster.remote.signing.certificate_authorities` at the same time.

`cluster.remote.signing.truststore.secure_password` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the truststore specified by `cluster.remote.signing.truststore.path`.

`cluster.remote.signing.truststore.algorithm` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The algorithm for the truststore. Defaults to `SunX509`.

`cluster.remote.signing.truststore.type` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The format of the truststore file. It must be either `jks` or `PKCS12`. If the file name ends in ".p12", ".pfx", or "pkcs12", the default is `PKCS12`. Otherwise, it defaults to `jks`.

`cluster.remote.signing.diagnose.trust` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls whether to output diagnostic messages for signature verification trust failures. If this is `true` (the default), a message will be printed to the {{es}} log whenever a cross-cluster signature verification is rejected due to a failure to establish trust. This diagnostic message contains information that can be used to determine the cause of the failure and assist with resolving the problem. Set to `false` to disable these messages.


### Request signing settings [remote-cluster-request-signing]

The following per-cluster settings are used on the local cluster to sign outgoing cross-cluster requests per-cluster alias. A private key and certificate must be configured for each remote cluster that requires signing requests.


#### PEM encoded files [remote-cluster-signing-pem-files]

When using PEM encoded files for signing, use the following settings:

`cluster.remote.<cluster_alias>.signing.key` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The path to a PEM encoded file containing the private key used to sign cross-cluster requests to the remote cluster. You cannot use this setting and `cluster.remote.<cluster_alias>.signing.keystore.path` at the same time.

`cluster.remote.<cluster_alias>.signing.key.secure_passphrase` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The passphrase that is used to decrypt the private key specified by `cluster.remote.<cluster_alias>.signing.key`. Because the key might not be encrypted, this value is optional.

`cluster.remote.<cluster_alias>.signing.certificate` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The path for the PEM encoded certificate (or certificate chain) that is associated with the signing key. This certificate is sent as part of the signature and must be trusted by the remote cluster's `cluster.remote.signing.certificate_authorities` or `cluster.remote.signing.truststore.path` configuration. This setting can be used only if `cluster.remote.<cluster_alias>.signing.key` is set.


#### Java keystore files [remote-cluster-signing-keystore-files]

When using Java keystore files (JKS) for signing, which contain the private key and certificate, use the following settings:

`cluster.remote.<cluster_alias>.signing.keystore.path` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The path for the keystore file that contains the private key and certificate used to sign cross-cluster requests. It must be either a Java keystore (jks) or a PKCS#12 file. You cannot use this setting and `cluster.remote.<cluster_alias>.signing.key` at the same time.

`cluster.remote.<cluster_alias>.signing.keystore.type` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported onElastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The format of the keystore file. It must be either `jks` or `PKCS12`. If the keystore path ends in ".p12", ".pfx", or ".pkcs12", this setting defaults to `PKCS12`. Otherwise, it defaults to `jks`.

`cluster.remote.<cluster_alias>.signing.keystore.alias` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The alias of the key within the keystore that should be used for signing cross-cluster requests. If the keystore contains more than one private key, this setting must be specified.

`cluster.remote.<cluster_alias>.signing.keystore.secure_password` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the keystore specified by `cluster.remote.<cluster_alias>.signing.keystore.path`.

`cluster.remote.<cluster_alias>.signing.keystore.secure_key_password` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the key in the keystore specified by `cluster.remote.<cluster_alias>.signing.keystore.path`. Defaults to the keystore password.

`cluster.remote.<cluster_alias>.signing.keystore.algorithm` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The algorithm for the keystore. Defaults to the default algorithm for the Java KeyManagerFactory.
