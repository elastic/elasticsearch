---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-wait-for-snapshot.html
---

# Wait for snapshot [ilm-wait-for-snapshot]

Phases allowed: delete.

Waits for the specified {{slm-init}} policy to be executed before removing the index. This ensures that a snapshot of the deleted index is available.

## Options [ilm-wait-for-snapshot-options]

`policy`
:   (Required, string) Name of the {{slm-init}} policy that the delete action should wait for.


## Example [ilm-wait-for-snapshot-ex]

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "delete": {
        "actions": {
          "wait_for_snapshot" : {
            "policy": "slm-policy-name"
          }
        }
      }
    }
  }
}
```


