---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-network-host.html
---

# GCE Network Host [discovery-gce-network-host]

When the `discovery-gce` plugin is installed, the following are also allowed as valid network host settings:

| GCE Host Value | Description |
| --- | --- |
| `_gce:privateIp:X_` | The private IP address of the machine for a given network interface. |
| `_gce:hostname_` | The hostname of the machine. |
| `_gce_` | Same as `_gce:privateIp:0_` (recommended). |

Examples:

```yaml
# get the IP address from network interface 1
network.host: _gce:privateIp:1_
# Using GCE internal hostname
network.host: _gce:hostname_
# shortcut for _gce:privateIp:0_ (recommended)
network.host: _gce_
```

## How to start (short story) [discovery-gce-usage-short]

* Create Google Compute Engine instance (with compute rw permissions)
* Install Elasticsearch
* Install Google Compute Engine Cloud plugin
* Modify `elasticsearch.yml` file
* Start Elasticsearch


