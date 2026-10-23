---
navigation_title: "Data sources"
description: "Connect Elasticsearch to external storage with ES|QL Data Federation. Create and manage data sources, choose an authentication model, and grant read access to your data."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Connect external data sources for {{esql}} Data Federation

:::{include} _snippets/data-federation/experimental-warning.md
:::

A data source defines the connection to an external storage system. It stores the connection type, endpoint, and credentials. A data source defines how to connect, not what data to query. One data source can serve many [datasets](esql-data-federation-datasets.md). When credentials rotate, you update the data source in one place without touching the datasets that reference it.

## Supported data source types

The following data source types are supported:

:::{include} _snippets/data-federation/supported-data-source-types.md
:::

:::{note}
Other S3-compatible services have not been validated and are not supported.
:::

## Manage data sources

You can manage data sources in two ways:

[Kibana UI](esql-data-federation-manage-data-sources-ui.md)
:   Connect data sources from a form, and view, edit, and delete them from the **Data sources** tab.

[API](esql-data-federation-manage-data-sources-api.md)
:   Create, get, list, delete, and test data sources with the `/_query/data_source` endpoint.

## Data source settings

Settings vary by data source type. For every setting each type accepts, refer to the [data source settings reference](esql-data-federation-data-source-settings.md).

## Authentication

A data source authenticates to its store with one of the following models. The models are mutually exclusive on a data source.

| Model | `auth` value | Description |
|---|---|---|
| Static credentials | `static_credentials` | A fixed access key and secret key. The common form for a service account. To set one up, refer to [connect with static credentials](esql-data-federation-static-credentials.md). |
| Anonymous | `anonymous` | For public data that needs no credentials. The [quickstart](esql-data-federation-quickstart.md) walks through this method. |
| Federated identity | `federated_identity` | Keyless. {{es}} exchanges a short-lived OIDC token for temporary AWS credentials via STS, so no static keys are stored. To set it up, refer to [connect with federated identity](esql-data-federation-federated-identity.md).<br><br>Available on {{ech}}, except on IBM Cloud and in special environments such as AWS GovCloud. Not available on self-managed, ECE, or ECK. |
| Managed identity | `managed_identity` | Keyless. Uses the {{es}} node's own cloud identity, for example an EC2 instance IAM role. Operator-only and API-only, and not available in serverless.<br><br>Requires the [`esql.external.managed_identity.enabled`](esql-data-federation-cluster-settings.md#esql-external-managed-identity-enabled) cluster setting. |

:::{warning}
Managed identity uses the cloud identity attached to each {{es}} node (for example, an IAM role on EC2 or a service account on GKE). Different nodes might have different identities, and the node that performs the connection is not guaranteed.

You are responsible for configuring cloud IAM so that every node's identity has the required permissions on the target bucket. This model is best suited for single-cloud, single-tenant deployments where node identities are uniform.
:::

## Grant read access to your data [grant-read-access]

A data source reads from external storage as a single cloud identity, and your cloud provider controls what that identity can read. For Amazon S3, the identity is the IAM user that owns the access key, the IAM role that {{es}} assumes, or the node's own IAM role. {{es}} doesn't check these permissions when you create a data source or a dataset. A missing permission shows up as an access error the first time you query a dataset.

The identity needs permission to read the files in every dataset that uses the data source. If a dataset's resource is a prefix or a glob pattern, the identity also needs permission to list objects in that location. Because one data source can serve many datasets, check its permissions whenever you add a dataset that reads from a new location.

To grant read access for each data source type, refer to:

- **Amazon S3**: Allow `s3:GetObject` on the dataset's objects, and `s3:ListBucket` on the bucket when a dataset's resource is a prefix or a glob pattern. For a complete policy and troubleshooting, refer to [grant read access in Amazon S3](esql-data-federation-s3.md#s3-permissions).

## Audit logging

With [audit logging enabled](docs-content://deploy-manage/security/logging-configuration/enabling-audit-logs.md),
creating, updating, and deleting data sources generates `security_config_change` events. Refer to
[put_data_source](../../elasticsearch/elasticsearch-audit-events.md#event-put-data-source), and
[delete_data_source](../../elasticsearch/elasticsearch-audit-events.md#event-delete-data-source).
