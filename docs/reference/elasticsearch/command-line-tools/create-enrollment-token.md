---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/create-enrollment-token.html
---

# elasticsearch-create-enrollment-token [create-enrollment-token]

The `elasticsearch-create-enrollment-token` command creates enrollment tokens for {{es}} nodes and {{kib}} instances.


## Synopsis [_synopsis_3]

```shell
bin/elasticsearch-create-enrollment-token
[-f, --force] [-h, --help] [-E <KeyValuePair>] [-s, --scope] [--url]
```


## Description [_description_10]

::::{note}
`elasticsearch-create-enrollment-token` can only be used with {{es}} clusters that have been [auto-configured for security](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md).
::::


Use this command to create enrollment tokens, which you can use to enroll new {{es}} nodes to an existing cluster or configure {{kib}} instances to communicate with an existing {{es}} cluster that has security features enabled. The command generates (and subsequently removes) a temporary user in the [file realm](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/file-based.md) to run the request that creates enrollment tokens.

::::{important}
You cannot use this tool if the file realm is disabled in your `elasticsearch.yml` file.
::::


This command uses an HTTP connection to connect to the cluster and run the user management requests. The command automatically attempts to establish the connection over HTTPS by using the `xpack.security.http.ssl` settings in the `elasticsearch.yml` file. If you do not use the default configuration directory, ensure that the `ES_PATH_CONF` environment variable returns the correct path before you run the `elasticsearch-create-enrollment-token` command. You can override settings in your `elasticsearch.yml` file by using the `-E` command option. For more information about debugging connection failures, see [Setup-passwords command fails due to connection failure](docs-content://troubleshoot/elasticsearch/security/trb-security-setup.md).


## Parameters [create-enrollment-token-parameters]

`-E <KeyValuePair>`
:   Configures a standard {{es}} or {{xpack}} setting.

`-f, --force`
:   Forces the command to run against an unhealthy cluster.

`-h, --help`
:   Returns all of the command parameters.

`-s, --scope`
:   Specifies the scope of the generated token. Supported values are `node` and `kibana`.

`--url`
:   Specifies the base URL (hostname and port of the local node) that the tool uses to submit API requests to {{es}}. The default value is determined from the settings in your `elasticsearch.yml` file. If `xpack.security.http.ssl.enabled`  is set to `true`, you must specify an HTTPS URL.


## Examples [_examples_16]

The following command creates an enrollment token for enrolling an {{es}} node into a cluster:

```shell
bin/elasticsearch-create-enrollment-token -s node
```

The following command creates an enrollment token for enrolling a {{kib}} instance into a cluster. The specified URL indicates where the elasticsearch-create-enrollment-token tool attempts to reach the local {{es}} node:

```shell
bin/elasticsearch-create-enrollment-token -s kibana --url "https://172.0.0.3:9200"
```

