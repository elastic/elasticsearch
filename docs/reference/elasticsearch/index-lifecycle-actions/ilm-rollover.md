---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-rollover.html
---

# Rollover [ilm-rollover]

Phases allowed: hot.

Rolls over a target to a new index when the existing index satisfies the specified rollover conditions.

::::{note}
When an index is rolled over, the previous index’s age is updated to reflect the rollover time. This date, rather than the index’s `creation_date`, is used in {{ilm}} `min_age` phase calculations. [Learn more](docs-content://troubleshoot/elasticsearch/index-lifecycle-management-errors.md#min-age-calculation).

::::


::::{important}
If the rollover action is used on a [follower index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-follow), policy execution waits until the leader index rolls over (or is [otherwise marked complete](docs-content://manage-data/lifecycle/index-lifecycle-management/skip-rollover.md)), then converts the follower index into a regular index with the [Unfollow action](/reference/elasticsearch/index-lifecycle-actions/ilm-unfollow.md).
::::


A rollover target can be a [data stream](docs-content://manage-data/data-store/data-streams.md) or an [index alias](docs-content://manage-data/data-store/aliases.md). When targeting a data stream, the new index becomes the data stream’s write index and its generation is incremented.

To roll over an index alias, the alias and its write index must meet the following conditions:

* The index name must match the pattern *^.*-\\d+$*, for example (`my-index-000001`).
* The `index.lifecycle.rollover_alias` must be configured as the alias to roll over.
* The index must be the [write index](docs-content://manage-data/data-store/aliases.md#write-index) for the alias.

For example, if `my-index-000001` has the alias `my_data`, the following settings must be configured.

```console
PUT my-index-000001
{
  "settings": {
    "index.lifecycle.name": "my_policy",
    "index.lifecycle.rollover_alias": "my_data"
  },
  "aliases": {
    "my_data": {
      "is_write_index": true
    }
  }
}
```

## Options [ilm-rollover-options]

A rollover action must specify at least one `max_*` condition, it may include zero or more `min_*` conditions. An empty rollover action is invalid.

The index will roll over once any `max_*` condition is satisfied and all `min_*` conditions are satisfied. Note, however, that empty indices are not rolled over by default.

`max_age`
:   (Optional,  [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Triggers rollover after the maximum elapsed time from index creation is reached. The elapsed time is always calculated since the index creation time, even if the index origination date is configured to a custom date, such as when using the [index.lifecycle.parse_origination_date](/reference/elasticsearch/configuration-reference/index-lifecycle-management-settings.md#index-lifecycle-parse-origination-date) or [index.lifecycle.origination_date](/reference/elasticsearch/configuration-reference/index-lifecycle-management-settings.md#index-lifecycle-origination-date) settings.

`max_docs`
:   (Optional, integer) Triggers rollover after the specified maximum number of documents is reached. Documents added since the last refresh are not included in the document count. The document count does **not** include documents in replica shards.

`max_size`
:   (Optional, [byte units](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Triggers rollover when the index reaches a certain size. This is the total size of all primary shards in the index. Replicas are not counted toward the maximum index size.

    ::::{tip}
    To see the current index size, use the [_cat indices](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-indices) API. The `pri.store.size` value shows the combined size of all primary shards.
    ::::


`max_primary_shard_size`
:   (Optional, [byte units](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Triggers rollover when the largest primary shard in the index reaches a certain size. This is the maximum size of the primary shards in the index. As with `max_size`, replicas are ignored.

    ::::{tip}
    To see the current shard size, use the [_cat shards](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-shards) API. The `store` value shows the size each shard, and `prirep` indicates whether a shard is a primary (`p`) or a replica (`r`).
    ::::


`max_primary_shard_docs`
:   (Optional, integer) Triggers rollover when the largest primary shard in the index reaches a certain number of documents. This is the maximum docs of the primary shards in the index. As with `max_docs`, replicas are ignored.

    ::::{tip}
    To see the current shard docs, use the [_cat shards](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-shards) API. The `docs` value shows the number of documents each shard.
    ::::


`min_age`
:   (Optional,  [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Prevents rollover until after the minimum elapsed time from index creation is reached. See notes on `max_age`.

`min_docs`
:   (Optional, integer) Prevents rollover until after the specified minimum number of documents is reached. See notes on `max_docs`.

`min_size`
:   (Optional, [byte units](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Prevents rollover until the index reaches a certain size. See notes on `max_size`.

`min_primary_shard_size`
:   (Optional, [byte units](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Prevents rollover until the largest primary shard in the index reaches a certain size. See notes on `max_primary_shard_size`.

`min_primary_shard_docs`
:   (Optional, integer) Prevents rollover until the largest primary shard in the index reaches a certain number of documents. See notes on `max_primary_shard_docs`.

::::{important}
Empty indices will not be rolled over, even if they have an associated `max_age` that would otherwise result in a roll over occurring. A policy can override this behavior, and explicitly opt in to rolling over empty indices, by adding a `"min_docs": 0` condition. This can also be disabled on a cluster-wide basis by setting `indices.lifecycle.rollover.only_if_has_documents` to `false`.
::::


::::{important}
The rollover action implicitly always rolls over a data stream or alias if one or more shards contain 200000000 or more documents. Normally a shard will reach 50GB long before it reaches 200M documents, but this isn’t the case for space efficient data sets. Search performance will very likely suffer if a shard contains more than 200M documents. This is the reason of the builtin limit.
::::



## Example [ilm-rollover-ex]

### Roll over based on largest primary shard size [ilm-rollover-primar-shardsize-ex]

This example rolls the index over when its largest primary shard is at least 50 gigabytes.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover" : {
            "max_primary_shard_size": "50gb"
          }
        }
      }
    }
  }
}
```


### Roll over based on index size [ilm-rollover-size-ex]

This example rolls the index over when it is at least 100 gigabytes.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover" : {
            "max_size": "100gb"
          }
        }
      }
    }
  }
}
```


### Roll over based on document count [_roll_over_based_on_document_count]

This example rolls the index over when it contains at least one hundred million documents.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover" : {
            "max_docs": 100000000
          }
        }
      }
    }
  }
}
```


### Roll over based on document count of the largest primary shard [_roll_over_based_on_document_count_of_the_largest_primary_shard]

This example rolls the index over when it contains at least ten million documents of the largest primary shard.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover" : {
            "max_primary_shard_docs": 10000000
          }
        }
      }
    }
  }
}
```


### Roll over based on index age [_roll_over_based_on_index_age]

This example rolls the index over if it was created at least 7 days ago.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover" : {
            "max_age": "7d"
          }
        }
      }
    }
  }
}
```


### Roll over using multiple conditions [_roll_over_using_multiple_conditions]

When you specify multiple rollover conditions, the index is rolled over when *any* of the `max_*` and *all* of the `min_*` conditions are met. This example rolls the index over if it is at least 7 days old or at least 100 gigabytes, but only as long as the index contains at least 1000 documents.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover" : {
            "max_age": "7d",
            "max_size": "100gb",
            "min_docs": 1000
          }
        }
      }
    }
  }
}
```


### Roll over while maintaining shard sizes [_roll_over_while_maintaining_shard_sizes]

This example rolls the index over when the primary shard size is at least 50gb, or when the index is at least 30 days old, but only as long as a primary shard is at least 1gb. For low-volume indices, this prevents the creation of many small shards.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover" : {
            "max_primary_shard_size": "50gb",
            "max_age": "30d",
            "min_primary_shard_size": "1gb"
          }
        }
      }
    }
  }
}
```


### Rollover condition blocks phase transition [_rollover_condition_blocks_phase_transition]

The rollover action only completes if one of its conditions is met. This means that any subsequent phases are blocked until rollover succeeds.

For example, the following policy deletes the index one day after it rolls over. It does not delete the index one day after it was created.

```console
PUT /_ilm/policy/rollover_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_size": "50gb"
          }
        }
      },
      "delete": {
        "min_age": "1d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}
```



