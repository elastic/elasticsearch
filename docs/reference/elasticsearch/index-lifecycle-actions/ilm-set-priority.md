---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-set-priority.html
---

# Set priority [ilm-set-priority]

Phases allowed: hot, warm, cold.

Sets the [priority](/reference/elasticsearch/index-settings/recovery-prioritization.md) of the index as soon as the policy enters the hot, warm, or cold phase. Higher priority indices are recovered before indices with lower priorities following a node restart.

Generally, indexes in the hot phase should have the highest value and indexes in the cold phase should have the lowest values. For example: 100 for the hot phase, 50 for the warm phase, and 0 for the cold phase. Indices that don’t set this value have a default priority of 1.

## Options [ilm-set-priority-options]

`priority`
:   (Required, integer) The priority for the index. Must be 0 or greater. Set to `null` to remove the priority.


## Example [ilm-set-priority-ex]

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "actions": {
          "set_priority" : {
            "priority": 50
          }
        }
      }
    }
  }
}
```


