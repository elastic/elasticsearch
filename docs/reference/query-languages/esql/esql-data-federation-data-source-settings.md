---
navigation_title: "Data source settings"
description: "Reference for ES|QL Data Federation data source settings, including Amazon S3 connection, endpoint, and authentication settings."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Data source settings reference for {{esql}} Data Federation

Data source settings control how {{es}} connects and authenticates to an external storage system. Add settings to the `settings` object when you [create or update a data source](esql-data-federation-sources.md#create-or-update-a-data-source), or fill in the matching fields in the **Connect data source** flyout in {{kib}}. The available settings depend on the data source `type`.

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Amazon S3 [s3]

The following settings apply to data sources with `type` set to `s3`.

### Connection settings [s3-connection-settings]

These settings control which endpoint {{es}} sends S3 requests to and how it addresses buckets.

$$$endpoint$$$

`endpoint`
:   An Amazon S3 endpoint override, for example `https://s3.us-east-1.amazonaws.com`.

    - **Default:** None. The endpoint is resolved from the region.
    - **Valid values:**
      - {applies_to}`stack: experimental 9.6+` An absolute URL naming a supported AWS S3 endpoint. Refer to [S3 endpoint requirements](#s3-endpoint-requirements).
      - {applies_to}`stack: experimental =9.5` Any endpoint URL. The value isn't validated.
    - **Related:** `addressing_style`, `region`

    Omit `endpoint` to resolve the endpoint from the region, which is the recommended configuration.

    $$$s3-endpoint-requirements$$$
    ::::{dropdown} S3 endpoint requirements
    :applies_to: stack: experimental 9.6+
    The `endpoint` value must be an absolute `https` URL naming a supported AWS S3 endpoint. The following endpoint forms are accepted. The first three are accepted in every AWS partition. The global form exists only in the commercial partition:

    - Regional: `https://s3.us-east-1.amazonaws.com`
    - Historical: `https://s3-us-west-2.amazonaws.com`
    - VPC interface: `https://bucket.vpce-0a1b2c3d.s3.us-east-1.vpce.amazonaws.com`
    - Global: `https://s3.amazonaws.com`

    A regional endpoint must name a region that the {{es}} version you are running knows about. A region added by AWS after that release is rejected until you upgrade, or until a node permits its host with the setting described below.

    :::{note}
    `https://s3.amazonaws.com` has no region. When you set `endpoint`, the SDK stops following cross-region redirects, so this global endpoint only reaches `us-east-1` buckets. Other regions get an error. Omit `endpoint` to let the SDK resolve the correct regional endpoint from the `region` setting.
    :::

    Every other AWS endpoint family is rejected, including FIPS endpoints, dual-stack endpoints, transfer acceleration, access points, object lambda, Outposts, the account-level control plane, the legacy `s3-external-1` alias, and S3 Express. A bucket-qualified endpoint such as `https://mybucket.s3.us-east-1.amazonaws.com` is also rejected: name the regional endpoint and let the bucket come from the dataset. So are plain `http`, a value without a scheme, and a host the URL syntax does not allow, such as an underscore or a non-numeric port.

    A node can permit additional hosts with the `esql.external.allowed_endpoint_hosts` node setting, a list of `host:port` patterns in `elasticsearch.yml` that defaults to empty. A host it names is also accepted over plain `http` for `endpoint`. The `sts_endpoint` setting always requires `https`.

    :::{warning}
    A data source created before these endpoint restrictions were introduced keeps working for queries, but updating it requires an endpoint that passes the validation described above.
    :::
    ::::

$$$addressing-style$$$

`addressing_style` {applies_to}`stack: experimental 9.6+`
:   The URL addressing style for S3 requests.

    - **Default:** `auto`
    - **Valid values:**
      - `auto`: Uses path-style when `endpoint` is set, and the SDK default otherwise.
      - `path`: Always uses path-style.
      - `virtual_hosted`: Lets the SDK decide. Bare-IP endpoints fall back to path-style.
    - **Related:** `endpoint`

    Because `auto` resolves to path-style whenever `endpoint` is set, set `virtual_hosted` if reads through a VPC interface endpoint fail with an addressing error.

$$$region$$$

`region` {applies_to}`stack: deprecated 9.6+, experimental =9.5`
:   The AWS region used for the S3 client.

    {applies_to}`stack: experimental 9.6+` The `region` setting on a data source is deprecated and has no effect. Set `region` in the [dataset settings](esql-data-federation-dataset-settings.md#amazon-s3-region) instead, or omit it to let {{es}} detect the region automatically. When no `endpoint` is set, the SDK redirects transparently. When one is set, {{es}} issues a `HeadBucket` probe on the first request and caches the discovered region. The cache is cleared after a few minutes without requests, and the next request discovers the region again.

    {applies_to}`stack: experimental =9.5` Set `region` on the data source. Datasets don't accept a `region` setting.

### Authentication settings [s3-authentication-settings]

These settings select the [authentication model](esql-data-federation-sources.md#authentication) and supply the values it needs. For the IAM permissions the configured identity needs, refer to [grant read access in Amazon S3](esql-data-federation-s3.md#s3-permissions).

$$$auth$$$

`auth`
:   The authentication model the data source uses.

    - **Default:** `auto`
    - **Valid values:**
      - `auto`: Infers the model from the other settings. Federated identity settings such as `role_arn` select `federated_identity`. Otherwise, `access_key` and `secret_key` select `static_credentials`. A data source with neither is rejected. `auto` never selects `anonymous` or `managed_identity`, so set those explicitly.
      - `anonymous`
      - `static_credentials`
      - `managed_identity`
      - `federated_identity`

    The **Requires** line on each of the following settings names the model the setting belongs to. When `auth` is `auto` or omitted, it refers to the model {{es}} infers.

$$$access-key$$$

`access_key`
:   The AWS access key ID.

    - **Default:** None
    - **Requires:** `auth` set to `static_credentials`
    - **Related:** `secret_key`, `session_token`

$$$secret-key$$$

`secret_key`
:   The AWS secret access key.

    - **Default:** None
    - **Requires:** `auth` set to `static_credentials`
    - **Related:** `access_key`, `session_token`

$$$session-token$$$

`session_token`
:   The AWS session token for temporary security credentials issued by AWS STS.

    - **Default:** None
    - **Requires:** `auth` set to `static_credentials`, with `access_key` and `secret_key` from the same temporary credentials
    - **Related:** `access_key`, `secret_key`

    Temporary credentials expire. When they do, requests from the data source fail until you update all three values with new credentials.

$$$role-arn$$$

`role_arn`
:   The ARN of the IAM role {{es}} assumes through STS.

    - **Default:** None. Required when `auth` is set to `federated_identity`.
    - **Requires:** `auth` set to `federated_identity`
    - **Related:** `jwt_audience`, `role_session_name`, `sts_endpoint`, `sts_region`

$$$jwt-audience$$$

`jwt_audience`
:   Overrides the JWT audience claim sent to STS.

    - **Default:** `sts.amazonaws.com`
    - **Requires:** `auth` set to `federated_identity`
    - **Related:** `role_arn`

$$$role-session-name$$$

`role_session_name`
:   A label for the assumed-role session.

    - **Default:** `elasticsearch-esql-datasource`
    - **Requires:** `auth` set to `federated_identity`
    - **Related:** `role_arn`

$$$sts-endpoint$$$

`sts_endpoint` {applies_to}`stack: experimental 9.6+`
:   An STS endpoint override, for example `https://sts.us-east-1.amazonaws.com`.

    - **Default:** None
    - **Valid values:** An absolute `https` URL that meets the same [S3 endpoint requirements](#s3-endpoint-requirements) as `endpoint`, but for STS hosts
    - **Requires:** `auth` set to `federated_identity`
    - **Related:** `sts_region`

    Any host permitted through `esql.external.allowed_endpoint_hosts` receives the node's OIDC token, so only add hosts on trusted network paths.

$$$sts-region$$$

`sts_region`
:   The AWS region of the STS endpoint.

    - **Default:**
      - {applies_to}`stack: experimental 9.6+` The dataset's `region` setting, or `us-east-1` if the dataset has no region
      - {applies_to}`stack: experimental =9.5` The data source's `region` setting, or `us-east-1` if no region is set
    - **Requires:** `auth` set to `federated_identity`
    - **Related:** `sts_endpoint`, `region`

    {applies_to}`stack: experimental 9.6+` A `region` set on the data source no longer applies to STS. If a `federated_identity` data source relied on it, set `sts_region` on the data source, or `region` on each dataset, to keep calling STS in that region.
