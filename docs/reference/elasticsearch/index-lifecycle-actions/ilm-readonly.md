---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-readonly.html
---

# Read only [ilm-readonly]

Phases allowed: hot, warm, cold.

Makes the index data read-only; disables data write operations against the index.

To use the `readonly` action in the `hot` phase, the `rollover` action **must** be present. If no rollover action is configured, {{ilm-init}} will reject the policy.

## Options [ilm-read-only-options]

None.


## Example [ilm-read-only-ex]

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "actions": {
          "readonly" : { }
        }
      }
    }
  }
}
```


