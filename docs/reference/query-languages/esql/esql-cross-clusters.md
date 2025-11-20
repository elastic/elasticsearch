---
navigation_title: Query across clusters
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-cross-clusters.html
applies_to:
  stack: preview 9.0, ga 9.1
  serverless: unavailable
products:
  - id: elasticsearch
---


# Use ES|QL across clusters [esql-cross-clusters]

With {{esql}}, you can execute a single query across multiple clusters.


## Prerequisites [esql-ccs-prerequisites]

* {{ccs-cap}} requires remote clusters. To set up remote clusters, see [*Remote clusters*](docs-content://deploy-manage/remote-clusters.md).

    To ensure your remote cluster configuration supports {{ccs}}, see [Supported {{ccs}} configurations](docs-content://solutions/search/cross-cluster-search.md#ccs-supported-configurations).

* For full {{ccs}} capabilities, the local and remote cluster must be on the same [subscription level](https://www.elastic.co/subscriptions).
* The local coordinating node must have the [`remote_cluster_client`](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#remote-node) node role.
* If you use [sniff mode](docs-content:///deploy-manage/remote-clusters/remote-clusters-self-managed.md#sniff-mode), the local coordinating node must be able to connect to seed and gateway nodes on the remote cluster.

    We recommend using gateway nodes capable of serving as coordinating nodes. The seed nodes can be a subset of these gateway nodes.

* If you use [proxy mode](docs-content:///deploy-manage/remote-clusters/remote-clusters-self-managed.md#proxy-mode), the local coordinating node must be able to connect to the configured `proxy_address`. The proxy at this address must be able to route connections to gateway and coordinating nodes on the remote cluster.
* {{ccs-cap}} requires different security privileges on the local cluster and remote cluster. See [Configure privileges for {{ccs}}](docs-content://deploy-manage/remote-clusters/remote-clusters-cert.md#remote-clusters-privileges-ccs) and [*Remote clusters*](docs-content://deploy-manage/remote-clusters.md).


## Security model [esql-ccs-security-model]

{{es}} supports two security models for cross-cluster search (CCS):

* [TLS certificate authentication](#esql-ccs-security-model-certificate)
* [API key authentication](#esql-ccs-security-model-api-key)

::::{tip}
To check which security model is being used to connect your clusters, run `GET _remote/info`. If you’re using the API key authentication method, you’ll see the `"cluster_credentials"` key in the response.

::::



### TLS certificate authentication [esql-ccs-security-model-certificate]

::::{admonition} Deprecated in 9.0.0.
:class: warning

Use [API key authentication](#esql-ccs-security-model-api-key) instead.
::::


TLS certificate authentication secures remote clusters with mutual TLS. This could be the preferred model when a single administrator has full control over both clusters. We generally recommend that roles and their privileges be identical in both clusters.

Refer to [TLS certificate authentication](docs-content://deploy-manage/remote-clusters/remote-clusters-cert.md) for prerequisites and detailed setup instructions.


### API key authentication [esql-ccs-security-model-api-key]

The following information pertains to using {{esql}} across clusters with the [**API key based security model**](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md). You’ll need to follow the steps on that page for the **full setup instructions**. This page only contains additional information specific to {{esql}}.

API key based cross-cluster search (CCS) enables more granular control over allowed actions between clusters. This may be the preferred model when you have different administrators for different clusters and want more control over who can access what data. In this model, cluster administrators must explicitly define the access given to clusters and users.

You will need to:

* Create an API key on the **remote cluster** using the [Create cross-cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) API or using the [Kibana API keys UI](docs-content://deploy-manage/api-keys/elasticsearch-api-keys.md).
* Add the API key to the keystore on the **local cluster**, as part of the steps in [configuring the local cluster](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md#remote-clusters-security-api-key-local-actions). All cross-cluster requests from the local cluster are bound by the API key’s privileges.

Using {{esql}} with the API key based security model requires some additional permissions that may not be needed when using the traditional query DSL based search. The following example API call creates a role that can query remote indices using {{esql}} when using the API key based security model. The final privilege, `remote_cluster`, is required to allow remote enrich operations.

```console
POST /_security/role/remote1
{
  "cluster": ["cross_cluster_search"], <1>
  "indices": [
    {
      "names" : [""], <2>
      "privileges": ["read"]
    }
  ],
  "remote_indices": [ <3>
    {
      "names": [ "logs-*" ],
      "privileges": [ "read","read_cross_cluster" ], <4>
      "clusters" : ["my_remote_cluster"] <5>
    }
  ],
   "remote_cluster": [ <6>
        {
            "privileges": [
                "monitor_enrich"
            ],
            "clusters": [
                "my_remote_cluster"
            ]
        }
    ]
}
```

1. The `cross_cluster_search` cluster privilege is required for the *local* cluster.
2. Typically, users will have permissions to read both local and remote indices. However, for cases where the role is intended to ONLY search the remote cluster, the `read` permission is still required for the local cluster. To provide read access to the local cluster, but disallow reading any indices in the local cluster, the `names` field may be an empty string.
3. The indices allowed read access to the remote cluster. The configured [cross-cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) must also allow this index to be read.
4. The `read_cross_cluster` privilege is always required when using {{esql}} across clusters with the API key based security model.
5. The remote clusters to which these privileges apply. This remote cluster must be configured with a [cross-cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) and connected to the remote cluster before the remote index can be queried. Verify connection using the [Remote cluster info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-remote-info) API.
6. Required to allow remote enrichment. Without this, the user cannot read from the `.enrich` indices on the remote cluster. The `remote_cluster` security privilege was introduced in version **8.15.0**.


You will then need a user or API key with the permissions you created above. The following example API call creates a user with the `remote1` role.

```console
POST /_security/user/remote_user
{
  "password" : "<PASSWORD>",
  "roles" : [ "remote1" ]
}
```

Remember that all cross-cluster requests from the local cluster are bound by the cross cluster API key’s privileges, which are controlled by the remote cluster’s administrator.

::::{tip}
Cross cluster API keys created in versions prior to 8.15.0 will need to replaced or updated to add the new permissions required for {{esql}} with ENRICH.

::::



## Remote cluster setup [ccq-remote-cluster-setup]

Once the security model is configured, you can add remote clusters.

The following [cluster update settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) API request adds three remote clusters: `cluster_one`, `cluster_two`, and `cluster_three`.

```console
PUT _cluster/settings
{
  "persistent": {
    "cluster": {
      "remote": {
        "cluster_one": {
          "seeds": [
            "35.238.149.1:9300"
          ],
          "skip_unavailable": true
        },
        "cluster_two": {
          "seeds": [
            "35.238.149.2:9300"
          ],
          "skip_unavailable": false
        },
        "cluster_three": {  <1>
          "seeds": [
            "35.238.149.3:9300"
          ]
        }
      }
    }
  }
}
```
% TEST[setup:host]
% TEST[s/35.238.149.\d+:930\d+/\${transport_host}/]
% end::ccs-remote-cluster-setup[]

1. Since `skip_unavailable` was not set on `cluster_three`, it uses the default of `true`. See the [Optional remote clusters](#ccq-skip-unavailable-clusters) section for details.



## Query across multiple clusters [ccq-from]

In the `FROM` command, specify data streams and indices on remote clusters using the format `<remote_cluster_name>:<target>`. For instance, the following {{esql}} request queries the `my-index-000001` index on a single remote cluster named `cluster_one`:

```esql
FROM cluster_one:my-index-000001
| LIMIT 10
```

Similarly, this {{esql}} request queries the `my-index-000001` index from three clusters:

* The local ("querying") cluster
* Two remote clusters, `cluster_one` and `cluster_two`

```esql
FROM my-index-000001,cluster_one:my-index-000001,cluster_two:my-index-000001
| LIMIT 10
```

Likewise, this {{esql}} request queries the `my-index-000001` index from all remote clusters (`cluster_one`, `cluster_two`, and `cluster_three`):

```esql
FROM *:my-index-000001
| LIMIT 10
```


## Cross-cluster metadata [ccq-cluster-details]

Using the `"include_ccs_metadata": true` option, users can request that ES|QL {{ccs}} responses include metadata about the search on each cluster (when the response format is JSON). Here we show an example using the async search endpoint. {{ccs-cap}} metadata is also present in the synchronous search endpoint response when requested. If the search returns partial results and there are partial shard or remote cluster failures, `_clusters` metadata containing the failures will be included in the response regardless of the `include_ccs_metadata` parameter.

```console
POST /_query/async?format=json
{
  "query": """
    FROM my-index-000001,cluster_one:my-index-000001,cluster_two:my-index*
    | STATS COUNT(http.response.status_code) BY user.id
    | LIMIT 2
  """,
  "include_ccs_metadata": true
}
```
% TEST[setup:my_index]
% TEST[s/cluster_one:my-index-000001,cluster_two:my-index//]

Which returns:

```console-result
{
  "is_running": false,
  "took": 42,  <1>
  "is_partial": false, <7>
  "columns" : [
    {
      "name" : "COUNT(http.response.status_code)",
      "type" : "long"
    },
    {
      "name" : "user.id",
      "type" : "keyword"
    }
  ],
  "values" : [
    [4, "elkbee"],
    [1, "kimchy"]
  ],
  "_clusters": {  <2>
    "total": 3,
    "successful": 3,
    "running": 0,
    "skipped": 0,
    "partial": 0,
    "failed": 0,
    "details": { <3>
      "(local)": { <4>
        "status": "successful",
        "indices": "blogs",
        "took": 41,  <5>
        "_shards": { <6>
          "total": 13,
          "successful": 13,
          "skipped": 0,
          "failed": 0
        }
      },
      "cluster_one": {
        "status": "successful",
        "indices": "cluster_one:my-index-000001",
        "took": 38,
        "_shards": {
          "total": 4,
          "successful": 4,
          "skipped": 0,
          "failed": 0
        }
      },
      "cluster_two": {
        "status": "successful",
        "indices": "cluster_two:my-index*",
        "took": 40,
        "_shards": {
          "total": 18,
          "successful": 18,
          "skipped": 1,
          "failed": 0
        }
      }
    }
  }
}
```
% TEST[skip: cross-cluster testing env not set up]

1. How long the entire search (across all clusters) took, in milliseconds.
2. This section of counters shows all possible cluster search states and how many cluster searches are currently in that state. The clusters can have one of the following statuses: **running**, **successful** (searches on all shards were successful), **skipped** (the search failed on a cluster marked with `skip_unavailable`=`true`), **failed** (the search failed on a cluster marked with `skip_unavailable`=`false`) or **partial** (the search was [interrupted](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-esql) before finishing or has partially failed).
3. The `_clusters/details` section shows metadata about the search on each cluster.
4. If you included indices from the local cluster you sent the request to in your {{ccs}}, it is identified as "(local)".
5. How long (in milliseconds) the search took on each cluster. This can be useful to determine which clusters have slower response times than others.
6. The shard details for the search on that cluster, including a count of shards that were skipped due to the can-match phase results. Shards are skipped when they cannot have any matching data and therefore are not included in the full ES|QL query.
7. The `is_partial` field is set to `true` if the search has partial results for any reason, for example due to partial shard failures,
failures in remote clusters, or if the async query was stopped by calling the [async query stop API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-esql).

The cross-cluster metadata can be used to determine whether any data came back from a cluster. For instance, in the query below, the wildcard expression for `cluster-two` did not resolve to a concrete index (or indices). The cluster is, therefore, marked as *skipped* and the total number of shards searched is set to zero.

```console
POST /_query/async?format=json
{
  "query": """
    FROM cluster_one:my-index*,cluster_two:logs*
    | STATS COUNT(http.response.status_code) BY user.id
    | LIMIT 2
  """,
  "include_ccs_metadata": true
}
```
% TEST[continued]
% TEST[s/cluster_one:my-index\*,cluster_two:logs\*/my-index-000001/]

Which returns:

```console-result
{
  "is_running": false,
  "took": 55,
  "is_partial": true, <3>
  "columns": [
     ...
  ],
  "values": [
     ...
  ],
  "_clusters": {
    "total": 2,
    "successful": 1,
    "running": 0,
    "skipped": 1, <1>
    "partial": 0,
    "failed": 0,
    "details": {
      "cluster_one": {
        "status": "successful",
        "indices": "cluster_one:my-index*",
        "took": 38,
        "_shards": {
          "total": 4,
          "successful": 4,
          "skipped": 0,
          "failed": 0
        }
      },
      "cluster_two": {
        "status": "skipped", <1>
        "indices": "cluster_two:logs*",
        "took": 0,
        "_shards": {
          "total": 0, <2>
          "successful": 0,
          "skipped": 0,
          "failed": 0
        }
      }
    }
  }
}
```
% TEST[skip: cross-cluster testing env not set up]

1. This cluster is marked as *skipped*, since there were no matching indices on that cluster.
2. Indicates that no shards were searched (due to not having any matching indices).
3. Since one of the clusters is skipped, the search result is marked as partial.



## Enrich across clusters [ccq-enrich]

Enrich in {{esql}} across clusters operates similarly to [local enrich](commands/enrich.md). If the enrich policy and its enrich indices are consistent across all clusters, simply write the enrich command as you would without remote clusters. In this default mode, {{esql}} can execute the enrich command on either the local cluster or the remote clusters, aiming to minimize computation or inter-cluster data transfer. Ensuring that the policy exists with consistent data on both the local cluster and the remote clusters is critical for ES|QL to produce a consistent query result.

::::{tip}
Enrich in {{esql}} across clusters using the API key based security model was introduced in version **8.15.0**. Cross cluster API keys created in versions prior to 8.15.0 will need to replaced or updated to use the new required permissions. Refer to the example in the [API key authentication](#esql-ccs-security-model-api-key) section.

::::


In the following example, the enrich with `hosts` policy can be executed on either the local cluster or the remote cluster `cluster_one`.

```esql
FROM my-index-000001,cluster_one:my-index-000001
| ENRICH hosts ON ip
| LIMIT 10
```

Enrich with an {{esql}} query against remote clusters only can also happen on the local cluster. This means the below query requires the `hosts` enrich policy to exist on the local cluster as well.

```esql
FROM cluster_one:my-index-000001,cluster_two:my-index-000001
| LIMIT 10
| ENRICH hosts ON ip
```


### Enrich with coordinator mode [esql-enrich-coordinator]

{{esql}} provides the enrich `_coordinator` mode to force {{esql}} to execute the enrich command on the local cluster. This mode should be used when the enrich policy is not available on the remote clusters or maintaining consistency of enrich indices across clusters is challenging.

```esql
FROM my-index-000001,cluster_one:my-index-000001
| ENRICH _coordinator:hosts ON ip
| SORT host_name
| LIMIT 10
```

::::{important}
Enrich with the `_coordinator` mode usually increases inter-cluster data transfer and workload on the local cluster.

::::



### Enrich with remote mode [esql-enrich-remote]

{{esql}} also provides the enrich `_remote` mode to force {{esql}} to execute the enrich command independently on each remote cluster where the target indices reside. This mode is useful for managing different enrich data on each cluster, such as detailed information of hosts for each region where the target (main) indices contain log events from these hosts.

In the below example, the `hosts` enrich policy is required to exist on all remote clusters: the `querying` cluster (as local indices are included), the remote cluster `cluster_one`, and `cluster_two`.

```esql
FROM my-index-000001,cluster_one:my-index-000001,cluster_two:my-index-000001
| ENRICH _remote:hosts ON ip
| SORT host_name
| LIMIT 10
```

A `_remote` enrich cannot be executed after a [`STATS`](commands/stats-by.md) command. The following example would result in an error:

```esql
FROM my-index-000001,cluster_one:my-index-000001,cluster_two:my-index-000001
| STATS COUNT(*) BY ip
| ENRICH _remote:hosts ON ip
| SORT host_name
| LIMIT 10
```


### Multiple enrich commands [esql-multi-enrich]

You can include multiple enrich commands in the same query with different modes. {{esql}} will attempt to execute them accordingly. For example, this query performs two enriches, first with the `hosts` policy on any cluster and then with the `vendors` policy on the local cluster.

```esql
FROM my-index-000001,cluster_one:my-index-000001,cluster_two:my-index-000001
| ENRICH hosts ON ip
| ENRICH _coordinator:vendors ON os
| LIMIT 10
```

A `_remote` enrich command can’t be executed after a `_coordinator` enrich command. The following example would result in an error.

```esql
FROM my-index-000001,cluster_one:my-index-000001,cluster_two:my-index-000001
| ENRICH _coordinator:hosts ON ip
| ENRICH _remote:vendors ON os
| LIMIT 10
```


## Excluding clusters or indices from {{esql}} query [ccq-exclude]

To exclude an entire cluster, prefix the cluster alias with a minus sign in the `FROM` command, for example: `-my_cluster:*`:

```esql
FROM my-index-000001,cluster*:my-index-000001,-cluster_three:*
| LIMIT 10
```

To exclude a specific remote index, prefix the index with a minus sign in the `FROM` command, such as `my_cluster:-my_index`:

```esql
FROM my-index-000001,cluster*:my-index-*,cluster_three:-my-index-000001
| LIMIT 10
```


## Skipping problematic remote clusters [ccq-skip-unavailable-clusters]

{{ccs-cap}} for {{esql}} behavior when there are problems connecting to or running query on remote clusters differs between versions.

::::{tab-set}

:::{tab-item} 9.1
Remote clusters are configured with the `skip_unavailable: true` setting by default. With this setting, clusters are marked as `skipped` or `partial` rather than causing queries to fail in the following scenarios:

* The remote cluster is disconnected from the querying cluster, either before or during the query execution.
* The remote cluster does not have the requested index, or it is not accessible due to security settings.
* An error happened while processing the query on the remote cluster.

The `partial` status means the remote query either has errors or was interrupted by an explicit user action, but some data may be returned.

Queries will still fail when `skip_unavailable` is set `true`, if none of the specified indices exist. For example, the
following queries will fail:

```esql
FROM cluster_one:missing-index | LIMIT 10
FROM cluster_one:missing-index* | LIMIT 10
FROM cluster_one:missing-index*,cluster_two:missing-index | LIMIT 10
```
:::

:::{tab-item} 9.0
If a remote cluster disconnects from the querying cluster, {{ccs}} for {{esql}} will set it to `skipped`
and continue the query with other clusters, unless the remote cluster's `skip_unavailable` setting is set to `false`,
in which case the query will fail.
:::

::::

## Query across clusters during an upgrade [ccq-during-upgrade]

You can still search a remote cluster while performing a rolling upgrade on the local cluster. However, the local coordinating node’s "upgrade from" and "upgrade to" version must be compatible with the remote cluster’s gateway node.

::::{warning}
Running multiple versions of {{es}} in the same cluster beyond the duration of an upgrade is not supported.
::::


For more information about upgrades, see [Upgrading {{es}}](docs-content://deploy-manage/upgrade/deployment-or-cluster.md).
