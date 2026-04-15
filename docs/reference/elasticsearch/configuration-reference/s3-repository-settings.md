---
navigation_title: S3 repository
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/repository-s3.html
applies_to:
  stack: all
---

# S3 repository settings [repository-s3-settings]

You can use [AWS S3](https://aws.amazon.com/s3/) as a repository for [Snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md). This page lists the settings you can use to configure how {{es}} connects to S3 and how it stores data in your repository.

There are two categories of settings:

- **[Client settings](#repository-s3-client-settings)** control how {{es}} connects to S3, including authentication credentials, endpoints, proxies, and timeouts. These are defined per client in `elasticsearch.yml` or the {{es}} keystore, and can be shared across multiple repositories.
- **[Repository settings](#repository-s3-repository-settings)** control per-repository behavior such as the target bucket, chunk size, throttling, and encryption. These are specified when creating or updating a repository via the API.

For step-by-step setup instructions, refer to [S3 repository](docs-content://deploy-manage/tools/snapshot-and-restore/s3-repository.md).

## Client settings [repository-s3-client-settings]

The S3 client used to connect to S3 has a number of available settings. The settings have the form `s3.client.CLIENT_NAME.SETTING_NAME`. By default, `s3` repositories use a client named `default`, but this can be modified using the [repository setting](#repository-s3-repository-settings) `client`. For example, to use an S3 client named `my-alternate-client`, register the repository as follows:

```console
PUT _snapshot/my_s3_repository
{
  "type": "s3",
  "settings": {
    "bucket": "my-bucket",
    "client": "my-alternate-client"
  }
}
```

Most client settings can be added to the [`elasticsearch.yml`](docs-content://deploy-manage/stack-settings.md) configuration file with the exception of the secure settings, which you add to the {{es}} keystore. For more information about creating and updating the {{es}} keystore, refer to [Secure settings](docs-content://deploy-manage/security/secure-settings.md).

For example, if you want to use specific credentials to access S3, then run the following commands to add these credentials to the keystore.

```sh
bin/elasticsearch-keystore add s3.client.default.access_key
bin/elasticsearch-keystore add s3.client.default.secret_key
# a session token is optional so the following command may not be needed
bin/elasticsearch-keystore add s3.client.default.session_token
```

If you do not configure these settings then {{es}} will attempt to automatically obtain credentials from the environment in which it is running:

* Nodes running on an instance in AWS EC2 will attempt to use the EC2 Instance Metadata Service (IMDS) to obtain instance role credentials. {{es}} supports IMDS version 2 only.
* Nodes running in a container in AWS ECS and AWS EKS will attempt to obtain container role credentials similarly.

You can switch from using specific credentials back to the default of using the instance role or container role by removing these settings from the keystore as follows:

```sh
bin/elasticsearch-keystore remove s3.client.default.access_key
bin/elasticsearch-keystore remove s3.client.default.secret_key
# a session token is optional so the following command may not be needed
bin/elasticsearch-keystore remove s3.client.default.session_token
```

Define the relevant secure settings in each node's keystore before starting the node. The secure settings described here are all [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings) so you may update the keystore contents on each node while the node is running and then call the [Nodes reload secure settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-reload-secure-settings) to apply the updated settings to the nodes in the cluster. After this API completes, {{es}} will use the updated setting values for all future snapshot operations, but ongoing operations may continue to use older setting values.

The following list contains the available S3 client settings. Those that must be stored in the keystore are marked as "secure" and are **reloadable**; the other settings belong in the `elasticsearch.yml` file.

`s3.client.CLIENT_NAME.region`
:   Determines the region to use to sign requests made to the service. Also determines the regional endpoint to which {{es}} sends its requests, unless you specify a particular endpoint using the `endpoint` setting. If not set, {{es}} will attempt to determine the region automatically using the AWS SDK. {{es}} must use the correct region to sign requests because this value is required by [the S3 request-signing process](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html).

    If you are using an [S3-compatible service](docs-content://deploy-manage/tools/snapshot-and-restore/s3-repository.md#repository-s3-compatible-services) then it is unlikely the AWS SDK will be able to determine the correct region name automatically, so you must set it manually. Your service's region name is under the control of your service administrator and need not refer to a real AWS region, but the value to which you configure this setting must match the region name your service expects.

`s3.client.CLIENT_NAME.access_key` ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings))
:   An S3 access key. If set, the `secret_key` setting must also be specified. If unset, the client will use the instance or container role instead.

`s3.client.CLIENT_NAME.secret_key` ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings))
:   An S3 secret key. If set, the `access_key` setting must also be specified.

`s3.client.CLIENT_NAME.session_token` ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings))
:   An S3 session token. If set, the `access_key` and `secret_key` settings must also be specified.

`s3.client.CLIENT_NAME.endpoint`
:   The S3 service endpoint to connect to. This defaults to the regional endpoint corresponding to the configured `region`, but the [AWS documentation](https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region) lists alternative S3 endpoints. If you are using an [S3-compatible service](docs-content://deploy-manage/tools/snapshot-and-restore/s3-repository.md#repository-s3-compatible-services) then you should set this to the service's endpoint. The endpoint should specify the protocol and host name, e.g. `https://s3.ap-southeast-4.amazonaws.com`, `http://minio.local:9000`.

    When using HTTPS, this repository type validates the repository's certificate chain using the JVM-wide truststore. Ensure that the root certificate authority is in this truststore using the JVM's `keytool` tool. If you have a custom certificate authority for your S3 repository and you use the {{es}} [bundled JDK](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md#jvm-version), then you will need to reinstall your CA certificate every time you upgrade {{es}}.

`s3.client.CLIENT_NAME.protocol` {applies_to}`stack: deprecated 8.19`
:   The protocol scheme to use to connect to S3, if `endpoint` is set to an incomplete URL which does not specify the scheme. Valid values are either `http` or `https`. Defaults to `https`. Avoid using this setting. Instead, set the `endpoint` setting to a fully-qualified URL that starts with either `http://` or `https://`.

`s3.client.CLIENT_NAME.proxy.host`
:   The host name of a proxy to connect to S3 through.

`s3.client.CLIENT_NAME.proxy.port`
:   The port of a proxy to connect to S3 through.

`s3.client.CLIENT_NAME.proxy.scheme`
:   The scheme to use for the proxy connection to S3. Valid values are either `http` or `https`. Defaults to `http`. This setting allows to specify the protocol used for communication with the proxy server.

`s3.client.CLIENT_NAME.proxy.username` ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings))
:   The username to connect to the `proxy.host` with.

`s3.client.CLIENT_NAME.proxy.password` ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings))
:   The password to connect to the `proxy.host` with.

`s3.client.CLIENT_NAME.read_timeout`
:   ([time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) The maximum time {{es}} will wait to receive the next byte of data over an established, open connection to the repository before it closes the connection. The default value is 50 seconds.

`s3.client.CLIENT_NAME.max_connections`
:   The maximum number of concurrent connections to S3. The default value is `50`.

`s3.client.CLIENT_NAME.max_retries`
:   The number of retries to use when an S3 request fails. The default value is `3`.

`s3.client.CLIENT_NAME.connection_max_idle_time`
:   ([time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) The timeout after which {{es}} will close an idle connection. The default value is 60 seconds.

`s3.client.CLIENT_NAME.path_style_access`
:   Whether to force the use of the path style access pattern. If `true`, the path style access pattern will be used. If `false`, the access pattern will be automatically determined by the AWS Java SDK (See [AWS documentation](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Builder.html#setPathStyleAccessEnabled-java.lang.Boolean-) for details). Defaults to `false`.

::::{note}
:name: repository-s3-path-style-deprecation

In versions `7.0`, `7.1`, `7.2` and `7.3` all bucket operations used the [now-deprecated](https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/) path style access pattern. If your deployment requires the path style access pattern then you should set this setting to `true` when upgrading.
::::


`s3.client.CLIENT_NAME.disable_chunked_encoding`
:   Whether chunked encoding should be disabled or not. If `false`, chunked encoding is enabled and will be used where appropriate. If `true`, chunked encoding is disabled and will not be used, which may mean that snapshot operations consume more resources and take longer to complete. It should only be set to `true` if you are using a storage service that does not support chunked encoding. See the [AWS Java SDK documentation](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Builder.html#disableChunkedEncoding--) for details. Defaults to `false`.


## Repository settings [repository-s3-repository-settings]

The `s3` repository type supports a number of settings to customize how data is stored in S3. These can be specified when creating the repository. For example:

```console
PUT _snapshot/my_s3_repository
{
  "type": "s3",
  "settings": {
    "bucket": "my-bucket",
    "another_setting": "setting-value"
  }
}
```

The following settings are supported:

`bucket`
:   (Required) Name of the S3 bucket to use for snapshots.

    The bucket name must adhere to Amazon's [S3 bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules).


`client`
:   The name of the [S3 client](#repository-s3-client-settings) to use to connect to S3. Defaults to `default`.

`base_path`
:   Specifies the path to the repository data within its bucket. Defaults to an empty string, meaning that the repository is at the root of the bucket. The value of this setting should not start or end with a `/`.

    ::::{note}
    Don't set `base_path` when configuring a snapshot repository for {{ECE}}. {{ECE}} automatically generates the `base_path` for each deployment so that multiple deployments may share the same bucket.
    ::::


`chunk_size`
:   ([byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The maximum size of object that {{es}} will write to the repository when creating a snapshot. Files which are larger than `chunk_size` will be chunked into several smaller objects. {{es}} may also split a file across multiple objects to satisfy other constraints such as the `max_multipart_parts` limit. Defaults to `5TB` which is the [maximum size of an object in AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html).

`compress`
:   When set to `true` metadata files are stored in compressed format. This setting doesn't affect index files that are already compressed by default. Defaults to `true`.

`max_restore_bytes_per_sec`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum snapshot restore rate per node. Defaults to unlimited. Note that restores are also throttled through [recovery settings](/reference/elasticsearch/configuration-reference/index-recovery-settings.md).

`max_snapshot_bytes_per_sec`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum snapshot creation rate per node. Defaults to `40mb` per second. Note that if the [recovery settings for managed services](/reference/elasticsearch/configuration-reference/index-recovery-settings.md#recovery-settings-for-managed-services) are set, then it defaults to unlimited, and the rate is additionally throttled through [recovery settings](/reference/elasticsearch/configuration-reference/index-recovery-settings.md).

`readonly`
:   (Optional, Boolean) If `true`, the repository is read-only. The cluster can retrieve and restore snapshots from the repository but not write to the repository or create snapshots in it.

    Only a cluster with write access can create snapshots in the repository. All other clusters connected to the repository should have the `readonly` parameter set to `true`.

    If `false`, the cluster can write to the repository and create snapshots in it. Defaults to `false`.

    ::::{important}
    If you register the same snapshot repository with multiple clusters, only one cluster should have write access to the repository. Having multiple clusters write to the repository at the same time risks corrupting the contents of the repository.

    ::::


`server_side_encryption`
:   When set to `true` files are encrypted on server side using AES256 algorithm. Defaults to `false`.

`buffer_size`
:   ([byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Minimum threshold below which the chunk is uploaded using a single request. Beyond this threshold, the S3 repository will use the [AWS Multipart Upload API](https://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html) to split the chunk into several parts, each of `buffer_size` length, and to upload each part in its own request. Note that setting a buffer size lower than `5mb` is not allowed since it will prevent the use of the Multipart API and may result in upload errors. It is also not possible to set a buffer size greater than `5gb` as it is the maximum upload size allowed by S3. Defaults to `100mb` or `5%` of JVM heap, whichever is smaller.

`max_multipart_parts`
:   (integer) The maximum number of parts that {{es}} will write during a multipart upload of a single object. Files which are larger than `buffer_size × max_multipart_parts` will be chunked into several smaller objects. {{es}} may also split a file across multiple objects to satisfy other constraints such as the `chunk_size` limit. Defaults to `10000` which is the [maximum number of parts in a multipart upload in AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html).

`canned_acl`
:   The S3 repository supports all [S3 canned ACLs](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl) : `private`, `public-read`, `public-read-write`, `authenticated-read`, `log-delivery-write`, `bucket-owner-read`, `bucket-owner-full-control`. Defaults to `private`. You could specify a canned ACL using the `canned_acl` setting. When the S3 repository creates buckets and objects, it adds the canned ACL into the buckets and objects.

`storage_class`
:   Sets the S3 storage class for objects written to the repository. Values may be `standard`, `reduced_redundancy`, `standard_ia`, `onezone_ia` and `intelligent_tiering`. Defaults to `standard`. Refer to [S3 storage classes](docs-content://deploy-manage/tools/snapshot-and-restore/s3-repository.md#repository-s3-storage-classes) for more information.

`delete_objects_max_size`
:   (integer) Sets the maxmimum batch size, betewen 1 and 1000, used for `DeleteObjects` requests. Defaults to 1000 which is the maximum number supported by the [AWS DeleteObjects API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html).

`max_multipart_upload_cleanup_size`
:   (integer) Sets the maximum number of possibly-dangling multipart uploads to clean up in each batch of snapshot deletions. Defaults to `1000` which is the maximum number supported by the [AWS ListMultipartUploads API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html). If set to `0`, {{es}} will not attempt to clean up dangling multipart uploads.

`throttled_delete_retry.delay_increment`
:   ([time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) This value is used as the delay before the first retry and the amount the delay is incremented by on each subsequent retry. Default is 50ms, minimum is 0ms.

`throttled_delete_retry.maximum_delay`
:   ([time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) This is the upper bound on how long the delays between retries will grow to. Default is 5s, minimum is 0ms.

`throttled_delete_retry.maximum_number_of_retries`
:   (integer) Sets the number times to retry a throttled snapshot deletion. Defaults to `10`, minimum value is `0` which will disable retries altogether. Note that if retries are enabled in the Azure client, each of these retries comprises that many client-level retries.

`get_register_retry_delay`
:   ([time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Sets the time to wait before trying again if an attempt to read a [linearizable register](docs-content://deploy-manage/tools/snapshot-and-restore/s3-repository.md#repository-s3-linearizable-registers) fails. Defaults to `5s`.

`unsafely_incompatible_with_s3_conditional_writes` {applies_to}`stack: ga =9.2.3!, deprecated 9.2.4!+`
:   (boolean) {{es}} uses AWS S3's [support for conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) to protect against repository corruption. If your repository is based on a storage system which claims to be S3-compatible but does not accept conditional writes, set this setting to `true` to make {{es}} perform unconditional writes, bypassing the repository corruption protection, while you work with your storage supplier to address this incompatibility with AWS S3. Defaults to `false`.

::::{admonition} Overriding client settings in repository settings.
{applies_to}`stack: deprecated`
You may also specify all non-secure client settings directly in the repository settings. When you do, the client settings found in the repository settings are merged with those of the named client, with repository settings taking precedence. This behavior is deprecated and will be removed in a future version. Define client settings in `elasticsearch.yml` or the keystore instead.

For example, the following overrides the `endpoint` from the named client:

```console
PUT _snapshot/my_s3_repository
{
  "type": "s3",
  "settings": {
    "client": "my-client",
    "bucket": "my-bucket",
    "endpoint": "my.s3.endpoint"
  }
}
```
::::
