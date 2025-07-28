---
navigation_title: "Reindex data stream"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/master/data-stream-reindex-api.html
applies_to:
  stack: all
---

# Reindex data stream API [data-stream-reindex-api]


::::{admonition} New API reference
For the most up-to-date API details, refer to [Migration APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-migration).

::::


::::{tip}
These APIs are designed for indirect use by {{kib}}'s **Upgrade Assistant**. We strongly recommend you use the **Upgrade Assistant** to upgrade from 8.17 to {{version}}. For upgrade instructions, refer to [Upgrading to Elastic {{version}}](docs-content://deploy-manage/upgrade/deployment-or-cluster.md).
::::


The reindex data stream API is used to upgrade the backing indices of a data stream to the most recent major version. It works by reindexing each backing index into a new index, then replacing the original backing index with its replacement and deleting the original backing index. The settings and mappings from the original backing indices are copied to the resulting backing indices.

This api runs in the background because reindexing all indices in a large data stream is expected to take a large amount of time and resources. The endpoint will return immediately and a persistent task will be created to run in the background. The current status of the task can be checked with the [reindex status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-migrate-reindex-status). This status will be available for 24 hours after the task completes, whether it finished successfully or failed. If the status is still available for a task, the task must be cancelled before it can be re-run. A running or recently completed data stream reindex task can be cancelled using the [reindex cancel API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-cancel-migrate-reindex).

## {{api-request-title}} [data-stream-reindex-api-request]

`POST /_migration/reindex`


## {{api-prereq-title}} [data-stream-reindex-api-prereqs]

* If the {{es}} {{security-features}} are enabled, you must have the `manage` [index privilege](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/elasticsearch-privileges.md#privileges-list-indices) for the data stream.


## {{api-request-body-title}} [data-stream-reindex-body]

`source`
:   `index`
:   (Required, string) The name of the data stream to upgrade.


`mode`
:   (Required, enum) Set to `upgrade` to upgrade the data stream in-place, using the same source and destination data stream. Each out-of-date backing index will be reindexed. Then the new backing index is swapped into the data stream and the old index is deleted. Currently, the only allowed value for this parameter is `upgrade`.


## Settings [reindex-data-stream-api-settings]

You can use the following settings to control the behavior of the reindex data stream API:

$$$migrate_max_concurrent_indices_reindexed_per_data_stream-setting$$$
`migrate.max_concurrent_indices_reindexed_per_data_stream` ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The number of backing indices within a given data stream which will be reindexed concurrently. Defaults to `1`.

$$$migrate_data_stream_reindex_max_request_per_second-setting$$$
`migrate.data_stream_reindex_max_request_per_second` ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The average maximum number of documents within a given backing index to reindex per second. Defaults to `1000`, though can be any decimal number greater than `0`. To remove throttling, set to `-1`. This setting can be used to throttle the reindex process and manage resource usage. Consult the [reindex throttle docs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex#docs-reindex-throttle) for more information.


## {{api-examples-title}} [reindex-data-stream-api-example]

Assume we have a data stream `my-data-stream` with the following backing indices, all of which have index major version 7.x.

* .ds-my-data-stream-2025.01.23-000001
* .ds-my-data-stream-2025.01.23-000002
* .ds-my-data-stream-2025.01.23-000003

Let’s also assume that `.ds-my-data-stream-2025.01.23-000003` is the write index. If {{es}} is version 8.x and we wish to upgrade to major version 9.x, the version 7.x indices must be upgraded in preparation. We can use this API to reindex a data stream with version 7.x backing indices and make them version 8 backing indices.

Start by calling the API:

$$$reindex-data-stream-start$$$

```console
POST _migration/reindex
{
    "source": {
        "index": "my-data-stream"
    },
    "mode": "upgrade"
}
```

As this task runs in the background this API will return immediately. The task will do the following.

First, the data stream is rolled over. So that no documents are lost during the reindex, we add [write blocks](/reference/elasticsearch/index-settings/index-block.md) to the existing backing indices before reindexing them. Since a data stream’s write index cannot have a write block, the data stream is must be rolled over. This will produce a new write index, `.ds-my-data-stream-2025.01.23-000004`; which has an 8.x version and thus does not need to be upgraded.

Once the data stream has a write index with an 8.x version we can proceed with reindexing the old indices. For each of the version 7.x indices, we now do the following:

* Add a write block to the source index to guarantee that no writes are lost.
* Open the source index if it is closed.
* Delete the destination index if one exists. This is done in case we are retrying after a failure, so that we start with a fresh index.
* Create the destination index using the [create from source API](/reference/elasticsearch/rest-apis/create-index-from-source.md). This copies the settings and mappings from the old backing index to the new backing index.
* Use the [reindex API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) to copy the contents of the old backing index to the new backing index.
* Close the destination index if the source index was originally closed.
* Replace the old index in the data stream with the new index, using the [modify data streams API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-modify-data-stream).
* Finally, the old backing index is deleted.

By default only one backing index will be processed at a time. This can be modified using the [`migrate_max_concurrent_indices_reindexed_per_data_stream-setting` setting](#migrate_max_concurrent_indices_reindexed_per_data_stream-setting).

While the reindex data stream task is running, we can inspect the current status using the [reindex status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-migrate-reindex-status):

```console
GET /_migration/reindex/my-data-stream/_status
```

For the above example, the following would be a possible status:

```console-result
{
  "start_time_millis": 1737676174349,
  "complete": false,
  "total_indices_in_data_stream": 4,
  "total_indices_requiring_upgrade": 3,
  "successes": 0,
  "in_progress": [
    {
      "index": ".ds-my-data-stream-2025.01.23-000001",
      "total_doc_count": 10000000,
      "reindexed_doc_count": 999999
    }
  ],
  "pending": 2,
  "errors": []
}
```

This output means that the first backing index, `.ds-my-data-stream-2025.01.23-000001`, is currently being processed, and none of the backing indices have yet completed. Notice that `total_indices_in_data_stream` has a value of `4`, because after the rollover, there are 4 indices in the data stream. But the new write index has an 8.x version, and thus doesn’t need to be reindexed, so `total_indices_requiring_upgrade` is only 3.

### Cancelling and Restarting [reindex-data-stream-cancel-restart]

The [reindex datastream settings](#reindex-data-stream-api-settings) provide a few ways to control the performance and resource usage of a reindex task. This example shows how we can stop a running reindex task, modify the settings, and restart the task.

Continuing with the above example, assume the reindexing task has not yet completed, and the [reindex status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-migrate-reindex-status) returns the following:

```console-result
{
  "start_time_millis": 1737676174349,
  "complete": false,
  "total_indices_in_data_stream": 4,
  "total_indices_requiring_upgrade": 3,
  "successes": 1,
  "in_progress": [
    {
      "index": ".ds-my-data-stream-2025.01.23-000002",
      "total_doc_count": 10000000,
      "reindexed_doc_count": 1000
    }
  ],
  "pending": 1,
  "errors": []
}
```

Let’s assume the task has been running for a long time. By default, we throttle how many requests the reindex operation can execute per second. This keeps the reindex process from consuming too many resources. But the default value of `1000` request per second will not be correct for all use cases. The [`migrate.data_stream_reindex_max_request_per_second` setting](#migrate_data_stream_reindex_max_request_per_second-setting) can be used to increase or decrease the number of requests per second, or to remove the throttle entirely.

Changing this setting won’t have an effect on the backing index that is currently being reindexed. For example, changing the setting won’t have an effect on `.ds-my-data-stream-2025.01.23-000002`, but would have an effect on the next backing index.

But in the above status, `.ds-my-data-stream-2025.01.23-000002` has values of 1000 and 10M for the `reindexed_doc_count` and `total_doc_count`, respectively. This means it has only reindexed 0.01% of the documents in the index. It might be a good time to cancel the run and optimize some settings without losing much work. So we call the [cancel API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-cancel-migrate-reindex):

```console
POST /_migration/reindex/my-data-stream/_cancel
```

Now we can use the [update cluster settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) to increase the throttle:

```console
PUT /_cluster/settings
{
  "persistent" : {
    "migrate.data_stream_reindex_max_request_per_second" : 10000
  }
}
```

The [original reindex command](#reindex-data-stream-start) can now be used to restart reindexing. Because the first backing index, `.ds-my-data-stream-2025.01.23-000001`, has already been reindexed and thus is already version 8.x, it will be skipped. The task will start by reindexing `.ds-my-data-stream-2025.01.23-000002` again from the beginning.

Later, once all the backing indices have finished, the [reindex status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-migrate-reindex-status) will return something like the following:

```console-result
{
  "start_time_millis": 1737676174349,
  "complete": true,
  "total_indices_in_data_stream": 4,
  "total_indices_requiring_upgrade": 2,
  "successes": 2,
  "in_progress": [],
  "pending": 0,
  "errors": []
}
```

Notice that the value of `total_indices_requiring_upgrade` is `2`, unlike the previous status, which had a value of `3`. This is because `.ds-my-data-stream-2025.01.23-000001` was upgraded before the task cancellation. After the restart, the API sees that it does not need to be upgraded, thus does not include it in `total_indices_requiring_upgrade` or `successes`, despite the fact that it upgraded successfully.

The completed status will be accessible from the status API for 24 hours after completion of the task.

We can now check the data stream to verify that indices were upgraded:

```console
GET _data_stream/my-data-stream?filter_path=data_streams.indices.index_name
```

which returns:

```console-result
{
  "data_streams": [
    {
      "indices": [
        {
          "index_name": ".migrated-ds-my-data-stream-2025.01.23-000003"
        },
        {
          "index_name": ".migrated-ds-my-data-stream-2025.01.23-000002"
        },
        {
          "index_name": ".migrated-ds-my-data-stream-2025.01.23-000001"
        },
        {
          "index_name": ".ds-my-data-stream-2025.01.23-000004"
        }
      ]
    }
  ]
}
```

Index `.ds-my-data-stream-2025.01.23-000004` is the write index and didn’t need to be upgraded because it was created with version 8.x. The other three backing indices are now prefixed with `.migrated` because they have been upgraded.

We can now check the indices and verify that they have version 8.x:

```console
GET .migrated-ds-my-data-stream-2025.01.23-000001?human&filter_path=*.settings.index.version.created_string
```

which returns:

```console-result
{
  ".migrated-ds-my-data-stream-2025.01.23-000001": {
    "settings": {
      "index": {
        "version": {
          "created_string": "8.18.0"
        }
      }
    }
  }
}
```


### Handling Failures [reindex-data-stream-handling-failure]

Since the reindex data stream API runs in the background, failure information can be obtained through the [reindex status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-migrate-reindex-status). For example, if the backing index `.ds-my-data-stream-2025.01.23-000002` was accidentally deleted by a user, we would see a status like the following:

```console-result
{
  "start_time_millis": 1737676174349,
  "complete": false,
  "total_indices_in_data_stream": 4,
  "total_indices_requiring_upgrade": 3,
  "successes": 1,
  "in_progress": [],
  "pending": 1,
  "errors": [
    {
      "index": ".ds-my-data-stream-2025.01.23-000002",
      "message": "index [.ds-my-data-stream-2025.01.23-000002] does not exist"
    }
  ]
}
```

Once the issue has been fixed, the failed reindex task can be re-run. First, the failed run’s status must be cleared using the [reindex cancel API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-cancel-migrate-reindex). Then the [original reindex command](#reindex-data-stream-start) can be called to pick up where it left off.
