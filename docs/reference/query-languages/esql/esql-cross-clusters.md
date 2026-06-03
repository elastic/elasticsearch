---
navigation_title: Query across clusters
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-cross-clusters.html
applies_to:
  stack: preview =9.0, ga 9.1+
  serverless: unavailable
products:
  - id: elasticsearch
description: Run a single ES|QL query across multiple Elasticsearch clusters. Covers role and user setup, query syntax, cross-cluster metadata, and enrich and skip-unavailable behavior.
---


# Use ES|QL across clusters [esql-cross-clusters]

With {{esql}}, you can execute a single query across multiple clusters.

::::{note}
This page covers remote clusters and {{ccs}}, which are not available in {{serverless-full}}. In {{serverless-short}}, you can use {{cps}} instead. To learn how to query across multiple {{serverless-short}} projects using {{cps-init}}, see [](esql-cross-serverless-projects.md).
::::

## Prerequisites [esql-ccs-prerequisites]

* {{esql}} {{ccs}} requires an [Enterprise subscription](https://www.elastic.co/subscriptions) on both the local (querying) cluster and every remote cluster.
* [Remote clusters](docs-content://deploy-manage/remote-clusters.md) must be configured before running {{esql}} across clusters.

    The destination cluster must be configured as a remote cluster on the local cluster, and the remote cluster connection must be correctly established. For setup instructions, refer to [Set up remote clusters](docs-content://deploy-manage/remote-clusters.md#setup).

* The local node receiving the query must have the [`remote_cluster_client`](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#remote-node) node role to connect to the remote clusters.
* {{esql}} across clusters requires the remote cluster connection to use the [API key-based security model](docs-content://deploy-manage/remote-clusters/security-models.md#api-key).

    To verify which security model is active, run `GET _remote/info`. When API key authentication is in use, the response includes `"cluster_credentials"`.
* For supported version pairings, see [Supported {{ccs}} configurations](docs-content://explore-analyze/cross-cluster-search.md#ccs-supported-configurations).


## Configure roles and users [esql-ccs-security-model-api-key]

{{esql}} {{ccs}} requires some additional permissions beyond a standard Query DSL search. The following example creates a role that can query remote indices using {{esql}}. The final `remote_cluster` privilege is required for remote enrich operations.

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
2. {{esql}} authorizes the query at the local node before resolving which clusters it will target. A role used only for remote queries must therefore still grant `read` in the local `indices` block. Setting `names` to an empty string satisfies that check while matching no real index, so the user cannot read any local data. This requirement is specific to {{esql}}; classic {{ccs}} authorizes per resolved index and does not need a local grant.
3. The indices allowed read access to the remote cluster. The configured [cross-cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) must also allow this index to be read.
4. The `read_cross_cluster` privilege is always required when using {{esql}} across clusters with the API key-based security model.
5. The remote clusters to which these privileges apply. This remote cluster must be configured with a [cross-cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) and connected to the local cluster before the remote index can be queried. Verify connection using the [Remote cluster info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-remote-info) API.
6. Required to allow remote enrichment. Without this, the user cannot read from the `.enrich` indices on the remote cluster. The `remote_cluster` security privilege was introduced in version **8.15.0**.


You then need a user or API key with the permissions you just created. The following example API call creates a user with the `remote1` role.

```console
POST /_security/user/remote_user
{
  "password" : "<PASSWORD>",
  "roles" : [ "remote1" ]
}
```

For the full reference of cross-cluster role privileges across all deployment types, refer to [Configure privileges for {{ccs}}](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md#_configure_privileges_for_ccs).

::::{note}
All cross-cluster requests from the local cluster are bound by the cross-cluster API key's privileges, which are controlled by the remote cluster's administrator. Local roles can only further reduce these permissions; they cannot increase access beyond what the API key allows.

For example, if the remote cluster's administrator creates a cross-cluster API key that excludes the indices you need, the role you defined on the local cluster cannot grant access to them.
::::

## Query across multiple clusters [ccq-from]

In the examples that follow, `cluster_one`, `cluster_two`, and `cluster_three` represent remote clusters that you've already configured on the local cluster where the query runs. The cluster name in each `FROM` clause is the alias you assigned during remote cluster setup.

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

Using the `"include_ccs_metadata": true` option, you can request that ES|QL {{ccs}} responses include metadata about the search on each cluster (when the response format is JSON). Here we show an example using the async search endpoint. {{ccs-cap}} metadata is also present in the synchronous search endpoint response when requested. If the search returns partial results and there are partial shard or remote cluster failures, `_clusters` metadata containing the failures is included in the response regardless of the `include_ccs_metadata` parameter.

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

You can use the cross-cluster metadata to determine whether any data came back from a cluster. For instance, in the query below, the wildcard expression for `cluster_two` did not resolve to a concrete index (or indices). The cluster is, therefore, marked as *skipped* and the total number of shards searched is set to zero.

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

For more on partial results and how cluster status is determined when failures occur, see [{{ccs-cap}} failures](docs-content://explore-analyze/cross-cluster-search.md#cross-cluster-search-failures).


## Enrich across clusters [ccq-enrich]

Enrich in {{esql}} across clusters operates similarly to [local enrich](commands/enrich.md). If the enrich policy and its enrich indices are consistent across all clusters, write the enrich command as you would without remote clusters. In this default mode, {{esql}} can execute the enrich command on either the local cluster or the remote clusters, aiming to minimize computation or inter-cluster data transfer. Ensuring that the policy exists with consistent data on both the local cluster and the remote clusters is critical for ES|QL to produce a consistent query result.

::::{tip}

Cross-cluster API keys created in versions prior to 8.15 must be replaced or updated to use the new required permissions for {{esql}} cross-cluster enrich with the API key-based security model. Refer to the example in the [](#esql-ccs-security-model-api-key) section.

::::


In the following example, the enrich with `hosts` policy can be executed on either the local cluster or the remote cluster `cluster_one`.

```esql
FROM my-index-000001,cluster_one:my-index-000001
| ENRICH hosts ON ip
| LIMIT 10
```

Enrich with an {{esql}} query against remote clusters only can also happen on the local cluster. This means the following query requires the `hosts` enrich policy to exist on the local cluster as well.

```esql
FROM cluster_one:my-index-000001,cluster_two:my-index-000001
| LIMIT 10
| ENRICH hosts ON ip
```


### Enrich with coordinator mode [esql-enrich-coordinator]

{{esql}} provides the enrich `_coordinator` mode to force {{esql}} to execute the enrich command on the local cluster. Use this mode when the enrich policy is not available on the remote clusters or maintaining consistency of enrich indices across clusters is challenging.

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

In the following example, the `hosts` enrich policy is required to exist on all remote clusters: the "querying" cluster (as local indices are included), the remote cluster `cluster_one`, and `cluster_two`.

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

You can include multiple enrich commands in the same query with different modes. {{esql}} attempts to execute them accordingly. For example, this query performs two enrich commands, first with the `hosts` policy on any cluster and then with the `vendors` policy on the local cluster.

```esql
FROM my-index-000001,cluster_one:my-index-000001,cluster_two:my-index-000001
| ENRICH hosts ON ip
| ENRICH _coordinator:vendors ON os
| LIMIT 10
```

A `_remote` enrich command can't be executed after a `_coordinator` enrich command. The following example would result in an error.

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

{applies_to}`stack: ga 9.5` The form `-my_cluster:my_index` is also accepted as an alternative for `my_cluster:-my_index`. For example, the following query is equivalent to the one above:

```esql
FROM my-index-000001,cluster*:my-index-*,-cluster_three:my-index-000001
| LIMIT 10
```

The two forms have different semantics: `-my_cluster:*` is a *cluster-level* exclusion that requires the cluster to have been included by a preceding expression (`-cluster_three:*` on its own is rejected), while `-my_cluster:<my_index>` is an *index-level* exclusion equivalent to `my_cluster:-<my_index>` and may appear standalone.


## Skipping problematic remote clusters [ccq-skip-unavailable-clusters]

{{ccs-cap}} for {{esql}} behavior when there are problems connecting to or running a query on remote clusters differs between versions.

::::{applies-switch}

:::{applies-item} stack: ga 9.1+
Remote clusters are configured with the `skip_unavailable: true` setting by default. With this setting, clusters are marked as `skipped` or `partial` rather than causing queries to fail in the following scenarios:

* The remote cluster is disconnected from the querying cluster, either before or during the query execution.
* The remote cluster does not have the requested index, or it is not accessible due to security settings.
* An error happened while processing the query on the remote cluster.

The `partial` status means the remote query either has errors or was interrupted by an explicit user action, but some data can be returned.

Even when `skip_unavailable` is set to `true`, queries fail if none of the specified indices exist. For example, the following queries fail:

```esql
FROM cluster_one:missing-index | LIMIT 10
FROM cluster_one:missing-index* | LIMIT 10
FROM cluster_one:missing-index*,cluster_two:missing-index | LIMIT 10
```
:::

:::{applies-item} stack: preview =9.0
If a remote cluster disconnects from the querying cluster, {{ccs}} for {{esql}} sets it to `skipped` and continues the query with other clusters, unless the remote cluster's `skip_unavailable` setting is set to `false`, in which case the query fails.
:::

::::

For broader context on the `skip_unavailable` setting, including how it interacts with `allow_no_indices` and `ignore_unavailable`, see [Optional remote clusters](docs-content://explore-analyze/cross-cluster-search.md#skip-unavailable-clusters).

## Query across clusters during an upgrade [ccq-during-upgrade]

You can still search a remote cluster while performing a rolling upgrade on the local cluster. However, the local node receiving the query must have an "upgrade from" and "upgrade to" version that is compatible with the remote cluster's gateway node.

::::{warning}
Running multiple versions of {{es}} in the same cluster beyond the duration of an upgrade is not supported.
::::


For more information about upgrades, see [Upgrading {{es}}](docs-content://deploy-manage/upgrade/deployment-or-cluster.md).


## Query across {{serverless-short}} projects [ccq-cps]

```{applies_to}
serverless: preview
```

You can use cross-project search (CPS) to query across multiple linked serverless projects. To learn more, refer to [Query across {{serverless-short}} projects](esql-cross-serverless-projects.md).
