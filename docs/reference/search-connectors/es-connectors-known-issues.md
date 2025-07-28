---
navigation_title: "Known issues"
---

# Connector known issues [es-connectors-known-issues]

:::{important}
Enterprise Search is not available in {{stack}} 9.0+.
:::

## Connector service [es-connectors-known-issues-connector-service]

The connector service has the following known issues:

* **OOM errors when syncing large database tables**

    Syncs after the initial sync can cause out-of-memory (OOM) errors when syncing large database tables. This occurs because database connectors load and store IDs in memory. For tables with millions of records, this can lead to memory exhaustion if the connector service has insufficient RAM.

    To mitigate this issue, you can:

    * **Increase RAM allocation**:

        * **Self-managed**: Increase RAM allocation for the machine/container running the connector service.

            ::::{dropdown} RAM sizing guidelines
            The following table shows the estimated RAM usage for loading IDs into memory.

            | **Number of IDs** | **Memory Usage in MB (2X buffer)** |
            | --- | --- |
            | 1,000,000 | ≈ 45.78 MB |
            | 10,000,000 | ≈ 457.76 MB |
            | 50,000,000 | ≈ 2288.82 MB (≈ 2.29 GB) |
            | 100,000,000 | ≈ 4577.64 MB (≈ 4.58 GB) |

            ::::

    * **Optimize** [**sync rules**](/reference/search-connectors/es-sync-rules.md):

        * Review and optimize sync rules to filter and reduce data retrieved from the source before syncing.

%    * **Use a self-managed connector** instead of a managed connector:

%        * Because self-managed connectors run on your infrastructure, they are not subject to the same RAM limitations of the Enterprise Search node.

* **Upgrades from deployments running on versions earlier than 8.9.0 can cause sync job failures**

    Due to a bug, the `job_type` field mapping will be missing after upgrading from deployments running on versions earlier than 8.9.0. Sync jobs won’t be displayed in the Kibana UI (job history) and the connector service won’t be able to start new sync jobs. **This will only occur if you have previously scheduled sync jobs.**

    To resolve this issue, you can manually add the missing field with the following command and trigger a sync job:

    ```console
    PUT .elastic-connectors-sync-jobs-v1/_mapping
    {
      "properties": {
        "job_type": {
          "type": "keyword"
        }
      }
    }
    ```

* **The connector service will fail to sync when the connector tries to fetch more more than 2,147,483,647 (*2^31-1*) documents from a data source**

    A workaround is to manually partition the data to be synced using multiple search indices.

* **Custom scheduling might break when upgrading from version 8.6 or earlier.**

    If you encounter the error `'custom_schedule_triggered': undefined method 'each' for nil:NilClass (NoMethodError)`, it means the custom scheduling feature migration failed. You can use the following manual workaround:

    ```console
    POST /.elastic-connectors/_update/connector-id
    {
      "doc": {
        "custom_scheduling": {}
      }
    }
    ```

    This error can appear on Connectors or Crawlers that aren’t the cause of the issue. If the error continues, try running the above command for every document in the `.elastic-connectors` index.

* **Connectors upgrading from 8.7 or earlier can be missing configuration fields**

    A connector that was created prior to 8.8 can sometimes be missing configuration fields. This is a known issue for the MySQL connector but could also affect other connectors.

    If the self-managed connector raises the error `Connector for <connector_id> has missing configuration fields: <field_a>, <field_b>...`, you can resolve the error by manually adding the missing configuration fields via the Dev Tools. Only the following two field properties are required, as the rest will be autopopulated by the self-managed connector:

    * `type`: one of `str`, `int`, `bool`, or `list`
    * `value`: any value, as long as it is of the correct `type` (`list` type values should be saved as comma-separated strings)

        ```console
        POST /.elastic-connectors/_update/connector_id
        {
          "doc" : {
            "configuration": {
              "field_a": {
                "type": "str",
                "value": ""
              },
              "field_b": {
                "type": "bool",
                "value": false
              },
              "field_c": {
                "type": "int",
                "value": 1
              },
              "field_d": {
                "type": "list",
                "value": "a,b"
              }
            }
          }
        }
        ```

* **Python connectors that upgraded from 8.7.1 will report document volumes in gigabytes (GB) instead of megabytes (MB)**

    As a result, true document volume will be under-reported by a factor of 1024.


## Individual connector known issues [es-connectors-known-issues-specific]

Individual connectors may have additional known issues. Refer to [each connector’s reference documentation](/reference/search-connectors/index.md) for connector-specific known issues.