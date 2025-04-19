---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/service-tokens-command.html
---

# elasticsearch-service-tokens [service-tokens-command]

Use the `elasticsearch-service-tokens` command to create, list, and delete file-based service account tokens.


## Synopsis [_synopsis_9]

```shell
bin/elasticsearch-service-tokens
([create <service_account_principal> <token_name>]) |
([list] [<service_account_principal>]) |
([delete <service_account_principal> <token_name>])
```


## Description [_description_16]

::::{note}
The recommended way to manage [service tokens](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/service-accounts.md#service-accounts-tokens) is via the [Create service account tokens](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-service-token) API. File based tokens are intended for use with orchestrators such as [{{ece}}](docs-content://deploy-manage/deploy/cloud-enterprise.md) and [{{eck}}](docs-content://deploy-manage/deploy/cloud-on-k8s.md)
::::


This command creates a `service_tokens` file in the `$ES_HOME/config` directory when you create the first service account token. This file does not exist by default. {{es}} monitors this file for changes and dynamically reloads it.

This command only makes changes to the `service_tokens` file on the local node. If the service token will be used to authenticate requests against multiple nodes in the cluster then you must copy the `service_tokens` file to each node.

See [service accounts](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/service-accounts.md) for further information about the behaviour of service accounts and the management of service tokens.

::::{important}
To ensure that {{es}} can read the service account token information at startup, run `elasticsearch-service-tokens` as the same user you use to run {{es}}. Running this command as `root` or some other user updates the permissions for the `service_tokens` file and prevents {{es}} from accessing it.
::::



## Parameters [service-tokens-command-parameters]

### `create`

Creates a service account token for the specified service account.

**Properties of `create`**:

`<service_account_principal>`
:   (Required, string) Service account principal that takes the format of `<namespace>/<service>`, where the `namespace` is a top-level grouping of service accounts, and `service` is the name of the service. For example, `elastic/fleet-server`.

    The service account principal must match a known service account.

`<token_name>`
:   (Required, string) An identifier for the token name.

    Token names must be at least 1 and no more than 256 characters. They can contain alphanumeric characters (`a-z`, `A-Z`, `0-9`), dashes (`-`), and underscores (`_`), but cannot begin with an underscore.

    :::::{note}
    Token names must be unique in the context of the associated service account.
    :::::

### `list`

Lists all service account tokens defined in the `service_tokens` file. If you specify a service account principal, the command lists only the tokens that belong to the specified service account.

**Properties of `list`**:

`<service_account_principal>`
:   (Optional, string) Service account principal that takes the format of `<namespace>/<service>`, where the `namespace` is a top-level grouping of service accounts, and `service` is the name of the service. For example, `elastic/fleet-server`.

    The service account principal must match a known service account.

### `delete`

Deletes a service account token for the specified service account.

**Properties of `delete`**:

`<service_account_principal>`
:   (Required, string) Service account principal that takes the format of `<namespace>/<service>`, where the `namespace` is a top-level grouping of service accounts, and `service` is the name of the service. For example, `elastic/fleet-server`.

    The service account principal must match a known service account.

`<token_name>`
:   (Required, string) Name of an existing token.


## Examples [_examples_21]

The following command creates a service account token named `my-token` for the `elastic/fleet-server` service account.

```shell
bin/elasticsearch-service-tokens create elastic/fleet-server my-token
```

The output is a bearer token, which is a Base64 encoded string.

```shell
SERVICE_TOKEN elastic/fleet-server/my-token = AAEAAWVsYXN0aWM...vZmxlZXQtc2VydmVyL3Rva2VuMTo3TFdaSDZ
```

Use this bearer token to authenticate with your {{es}} cluster.

```shell
curl -H "Authorization: Bearer AAEAAWVsYXN0aWM...vZmxlZXQtc2VydmVyL3Rva2VuMTo3TFdaSDZ" http://localhost:9200/_cluster/health
```

::::{note}
If your node has `xpack.security.http.ssl.enabled` set to `true`, then you must specify `https` in the request URL.
::::


The following command lists all service account tokens that are defined in the `service_tokens` file.

```shell
bin/elasticsearch-service-tokens list
```

A list of all service account tokens displays in your terminal:

```txt
elastic/fleet-server/my-token
elastic/fleet-server/another-token
```

The following command deletes the `my-token` service account token for the `elastic/fleet-server` service account:

```shell
bin/elasticsearch-service-tokens delete elastic/fleet-server my-token
```

