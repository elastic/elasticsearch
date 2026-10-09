---
navigation_title: "Amazon S3"
description: "Connect ES|QL Data Federation to Amazon S3. Choose an authentication model and grant the IAM permissions Elasticsearch needs to read your data."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Amazon S3 data sources for {{esql}} Data Federation

An `s3` [data source](esql-data-federation-sources.md) reads files from Amazon S3. This page compares the authentication models, lists the AWS Identity and Access Management (IAM) permissions {{es}} needs to read your data, and explains how to troubleshoot access errors. For step-by-step setup, follow the guide for your authentication model.

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Choose an authentication model [s3-auth-models]

Each authentication model has its own setup guide:

| Model | `auth` value | Setup |
|---|---|---|
| Static credentials | `static_credentials` | [Connect with static credentials](esql-data-federation-static-credentials.md) |
| Federated identity | `federated_identity` | [Connect with federated identity](esql-data-federation-federated-identity.md) |
| Anonymous | `anonymous` | [Quickstart](esql-data-federation-quickstart.md) |
| Managed identity | `managed_identity` | [Authentication models](esql-data-federation-sources.md#authentication) |

For a description of each model and where it's available, refer to [authentication models](esql-data-federation-sources.md#authentication). For every setting an `s3` data source accepts, refer to the [data source settings reference](esql-data-federation-data-source-settings.md#s3).

## Grant read access [s3-permissions]

{{es}} reads your data as the IAM identity the data source uses: the IAM user that owns the access key, the IAM role that {{es}} assumes, or the node's own role. Attach a policy to that identity that allows the following actions:

| Action | Resource | When it's required |
|---|---|---|
| `s3:GetObject` | The objects the dataset reads, for example `arn:aws:s3:::<bucket-name>/<path>/*` | Always |
| `s3:ListBucket` | The bucket, for example `arn:aws:s3:::<bucket-name>` | When a dataset's resource is a prefix or a glob pattern rather than a single file |
| `kms:Decrypt` | The AWS Key Management Service (AWS KMS) key that encrypts the objects | When the objects use server-side encryption with AWS KMS keys (SSE-KMS) |

Scope the `s3:GetObject` resource to the prefixes your datasets use to grant the least access needed. When you add a dataset that reads from a new bucket or prefix, extend the policy to cover it. For a complete policy and AWS CLI commands, refer to the setup guide for your authentication model.

For `anonymous` data sources, the bucket policy must allow public access to the same actions.

### Check access [s3-check-access]

To confirm the data source can read a dataset, run a query against the dataset. Creating a dataset doesn't contact S3.

{applies_to}`stack: experimental 9.6+` The [test connection](esql-data-federation-sources.md#test-a-connection) endpoint confirms that the data source's credentials are valid, but not that they can read a particular bucket or path. For `s3` data sources it calls `ListBuckets`, so credentials scoped to specific buckets, like the policy above, return `untestable` rather than `success`. A `failure` result means the request couldn't reach S3 or S3 rejected the credentials, for example because a key was deleted or rotated.

### Troubleshoot access errors [s3-access-errors]

{{es}} doesn't check permissions when you create a dataset. A missing permission shows up as an error the first time you query the dataset:

`Access denied reading [<file>]` {applies_to}`stack: experimental 9.6+`
:   The identity can't read an object. Allow `s3:GetObject` on the object's path. If the message names a `kms:` action, the object is encrypted with a KMS key the identity can't use. Allow that action on the key.

`Access denied listing objects in the configured path` {applies_to}`stack: experimental 9.6+`
:   The identity can't list the bucket. Allow `s3:ListBucket` on the bucket, or change the dataset's resource to an exact file path.

`Access denied listing objects in bucket [<bucket>] with prefix [<prefix>]` {applies_to}`stack: experimental =9.5`
:   The identity can't list the bucket. Allow `s3:ListBucket` on the bucket.
