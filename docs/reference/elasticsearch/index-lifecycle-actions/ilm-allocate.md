---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-allocate.html
---

# Allocate [ilm-allocate]

Phases allowed: warm, cold.

Updates the index settings to change which nodes are allowed to host the index shards and change the number of replicas.

The allocate action is not allowed in the hot phase. The initial allocation for the index must be done manually or via [index templates](docs-content://manage-data/data-store/templates.md).

You can configure this action to modify both the allocation rules and number of replicas, only the allocation rules, or only the number of replicas. For more information about how {{es}} uses replicas for scaling, see [Get ready for production](docs-content://deploy-manage/production-guidance/elasticsearch-in-production-environments.md). See [Index-level shard allocation filtering](/reference/elasticsearch/index-settings/shard-allocation.md) for more information about controlling where {{es}} allocates shards of a particular index.

## Options [ilm-allocate-options]

You must specify the number of replicas or at least one `include`, `exclude`, or `require` option. An empty allocate action is invalid.

For more information about using custom attributes for shard allocation, refer to [](/reference/elasticsearch/index-settings/shard-allocation.md).

`number_of_replicas`
:   (Optional, integer) Number of replicas to assign to the index.

`total_shards_per_node`
:   (Optional, integer) The maximum number of shards for the index on a single {{es}} node. A value of `-1` is interpreted as unlimited. See [total shards](/reference/elasticsearch/index-settings/total-shards-per-node.md).

`include`
:   (Optional, object) Assigns an index to nodes that have at least *one* of the specified custom attributes.

`exclude`
:   (Optional, object) Assigns an index to nodes that have *none* of the specified custom attributes.

`require`
:   (Optional, object) Assigns an index to nodes that have *all* of the specified custom attributes.


## Example [ilm-allocate-ex]

The allocate action in the following policy changes the index’s number of replicas to `2`. No more than 200 shards for the index will be placed on any single node. Otherwise the index allocation rules are not changed.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "actions": {
          "allocate" : {
            "number_of_replicas" : 2,
            "total_shards_per_node" : 200
          }
        }
      }
    }
  }
}
```

### Assign index to nodes using a custom attribute [ilm-allocate-assign-index-attribute-ex]

The allocate action in the following policy assigns the index to nodes that have a `box_type` of *hot* or *warm*.

To designate a node’s `box_type`, you set a custom attribute in the node configuration. For example, set `node.attr.box_type: hot` in `elasticsearch.yml`. For more information, refer to [](/reference/elasticsearch/index-settings/shard-allocation.md#index-allocation-filters).

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "actions": {
          "allocate" : {
            "include" : {
              "box_type": "hot,warm"
            }
          }
        }
      }
    }
  }
}
```


### Assign index to nodes based on multiple attributes [ilm-allocate-assign-index-multi-attribute-ex]

The allocate action can also assign indices to nodes based on multiple node attributes. The following action assigns indices based on the `box_type` and `storage` node attributes.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "cold": {
        "actions": {
          "allocate" : {
            "require" : {
              "box_type": "cold",
              "storage": "high"
            }
          }
        }
      }
    }
  }
}
```


### Assign index to a specific node and update replica settings [ilm-allocate-assign-index-node-ex]

The allocate action in the following policy updates the index to have one replica per shard and be allocated to nodes that have a `box_type` of *cold*.

To designate a node’s `box_type`, you set a custom attribute in the node configuration. For example, set `node.attr.box_type: cold` in `elasticsearch.yml`. For more information, refer to [](/reference/elasticsearch/index-settings/shard-allocation.md#index-allocation-filters).

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "actions": {
          "allocate" : {
            "number_of_replicas": 1,
            "require" : {
              "box_type": "cold"
            }
        }
        }
      }
    }
  }
}
```



