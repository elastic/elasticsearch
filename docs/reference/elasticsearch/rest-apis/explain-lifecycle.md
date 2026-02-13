---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/8.19/ilm-explain-lifecycle.html
applies_to:
  stack: all
navigation_title: Understand the lifecycle status
---

# Use the explain lifecycle API to understand the index lifecycle status [explain-lifecycle-api]

The [explain lifecycle API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-explain-lifecycle) retrieves the current lifecycle status for one or more indices. For data streams, the API retrieves the lifecycle status for the stream’s backing indices, including the current phase, action, step, and any failures.

## Get the lifecycle status of an index

The following example retrieves the lifecycle state of `my-index-000001`:

<!--

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "min_age": "10d",
        "actions": {
          "forcemerge": {
            "max_num_segments": 1
          }
        }
      },
      "delete": {
        "min_age": "30d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}

PUT my-index-000001
{
  "settings": {
    "index.lifecycle.name": "my_policy",
    "index.number_of_replicas": 0
  }
}

GET /_cluster/health?wait_for_status=green&timeout=10s
```
% TEST

-->

```console
GET my-index-000001/_ilm/explain?human
```

When management of the index is first taken over by {{ilm-init}}, `explain` shows that the index is managed and in the `new` phase:

```console-result
{
  "indices": {
    "my-index-000001": {
      "index": "my-index-000001",
      "index_creation_date_millis": 1538475653281,        <1>
      "index_creation_date": "2018-10-15T13:45:21.981Z",
      "time_since_index_creation": "15s",                 <2>
      "managed": true,                                    <3>
      "policy": "my_policy",                              <4>
      "lifecycle_date_millis": 1538475653281,             <5>
      "lifecycle_date": "2018-10-15T13:45:21.981Z",
      "age": "15s",                                       <6>
      "phase": "new",
      "phase_time_millis": 1538475653317,                 <7>
      "phase_time": "2018-10-15T13:45:22.577Z",
      "action": "complete"
      "action_time_millis": 1538475653317,                <8>
      "action_time": "2018-10-15T13:45:22.577Z",
      "step": "complete",
      "step_time_millis": 1538475653317,                  <9>
      "step_time": "2018-10-15T13:45:22.577Z"
    }
  }
}
```
% TESTRESPONSE[skip:no way to know if we will get this response immediately]

1. When the index was created. This timestamp is used to determine when to roll over the index.
2. The time since the index creation (used for calculating when to rollover the index via the `max_age`).
3. Shows if the index is being managed by {{ilm-init}}. If the index is not managed by {{ilm-init}} the other fields will not be shown.
4. The name of the policy which {{ilm-init}} is using for this index.
5. The timestamp used for the `min_age`.
6. The age of the index (used for calculating when to enter the next phase).
7. When the index entered the current phase.
8. When the index entered the current action.
9. When the index entered the current step.

## View the phase definition applied to an index

Once the policy is running on the index, the response includes a `phase_execution` object that shows the definition of the current phase. Changes to the underlying policy will not affect this index until the current phase completes.

```console-result
{
  "indices": {
    "test-000069": {
      "index": "test-000069",
      "index_creation_date_millis": 1538475653281,
      "time_since_index_creation": "25.14s",
      "managed": true,
      "policy": "my_lifecycle3",
      "lifecycle_date_millis": 1538475653281,
      "lifecycle_date": "2018-10-15T13:45:21.981Z",
      "age": "25.14s",
      "phase": "hot",
      "phase_time_millis": 1538475653317,
      "phase_time": "2018-10-15T13:45:22.577Z",
      "action": "rollover",
      "action_time_millis": 1538475653317,
      "action_time": "2018-10-15T13:45:22.577Z",
      "step": "attempt-rollover",
      "step_time_millis": 1538475653317,
      "step_time": "2018-10-15T13:45:22.577Z",
      "phase_execution": {
        "policy": "my_lifecycle3",
        "phase_definition": { <1>
          "min_age": "0ms",
          "actions": {
            "rollover": {
              "max_age": "30s",
              "max_primary_shard_docs": 200000000, <2>
              "min_docs": 1
            }
          }
        },
        "version": 3, <3>
        "modified_date": "2018-10-15T13:21:41.576Z", <4>
        "modified_date_in_millis": 1539609701576 <5>
      }
    }
  }
}
```
% TESTRESPONSE[skip:not possible to get the cluster into this state in a docs test]

1. The JSON phase definition loaded from the specified policy when the index entered this phase
2. The rollover action includes the default `max_primary_shard_docs` and `min_docs` conditions. See [ILM rollover options](../index-lifecycle-actions/ilm-rollover.md#ilm-rollover-options) for more information.
3. The version of the policy that was loaded
4. The date the loaded policy was last modified
5. The epoch time when the loaded policy was last modified

## Check the status of a running step

If {{ilm-init}} is waiting for a step to complete, the response includes status information for the step that's being performed on the index.

```console-result
{
  "indices": {
    "test-000020": {
      "index": "test-000020",
      "index_creation_date_millis": 1538475653281,
      "time_since_index_creation": "4.12m",
      "managed": true,
      "policy": "my_lifecycle3",
      "lifecycle_date_millis": 1538475653281,
      "lifecycle_date": "2018-10-15T13:45:21.981Z",
      "age": "4.12m",
      "phase": "warm",
      "phase_time_millis": 1538475653317,
      "phase_time": "2018-10-15T13:45:22.577Z",
      "action": "allocate",
      "action_time_millis": 1538475653317,
      "action_time": "2018-10-15T13:45:22.577Z",
      "step": "check-allocation",
      "step_time_millis": 1538475653317,
      "step_time": "2018-10-15T13:45:22.577Z",
      "step_info": { <1>
        "message": "Waiting for all shard copies to be active",
        "shards_left_to_allocate": -1,
        "all_shards_active": false,
        "number_of_replicas": 2
      },
      "phase_execution": {
        "policy": "my_lifecycle3",
        "phase_definition": {
          "min_age": "0ms",
          "actions": {
            "allocate": {
              "number_of_replicas": 2,
              "include": {
                "box_type": "warm"
              },
              "exclude": {},
              "require": {}
            },
            "forcemerge": {
              "max_num_segments": 1
            }
          }
        },
        "version": 2,
        "modified_date": "2018-10-15T13:20:02.489Z",
        "modified_date_in_millis": 1539609602489
      }
    }
  }
}
```
% TESTRESPONSE[skip:not possible to get the cluster into this state in a docs test]

1. The status of the step that's in progress.

## Diagnose lifecycle errors

If the index is in the ERROR step, something went wrong while executing a step in the policy and you will need to take action for the index to proceed
to the next step. Some steps are safe to automatically be retried in certain circumstances. To help you diagnose the problem, the explain response shows the step that failed, the step info which provides information about the error, and information about the retry attempts executed for the failed step if it's the case.

```console-result
{
  "indices": {
    "test-000056": {
      "index": "test-000056",
      "index_creation_date_millis": 1538475653281,
      "time_since_index_creation": "50.1d",
      "managed": true,
      "policy": "my_lifecycle3",
      "lifecycle_date_millis": 1538475653281,
      "lifecycle_date": "2018-10-15T13:45:21.981Z",
      "age": "50.1d",
      "phase": "hot",
      "phase_time_millis": 1538475653317,
      "phase_time": "2018-10-15T13:45:22.577Z",
      "action": "rollover",
      "action_time_millis": 1538475653317,
      "action_time": "2018-10-15T13:45:22.577Z",
      "step": "ERROR",
      "step_time_millis": 1538475653317,
      "step_time": "2018-10-15T13:45:22.577Z",
      "failed_step": "check-rollover-ready", <1>
      "is_auto_retryable_error": true, <2>
      "failed_step_retry_count": 1, <3>
      "step_info": { <4>
        "type": "cluster_block_exception",
        "reason": "index [test-000057/H7lF9n36Rzqa-KfKcnGQMg] blocked by: [FORBIDDEN/5/index read-only (api)",
        "index_uuid": "H7lF9n36Rzqa-KfKcnGQMg",
        "index": "test-000057"
      },
      "previous_step_info": { <5>
        "type": "cluster_block_exception",
        "reason": "index [test-000057/H7lF9n36Rzqa-KfKcnGQMg] blocked by: [FORBIDDEN/5/index read-only (api)",
        "index_uuid": "H7lF9n36Rzqa-KfKcnGQMg",
        "index": "test-000057"
      },
      "phase_execution": {
        "policy": "my_lifecycle3",
        "phase_definition": {
          "min_age": "0ms",
          "actions": {
            "rollover": {
              "max_age": "30s"
            }
          }
        },
        "version": 3,
        "modified_date": "2018-10-15T13:21:41.576Z",
        "modified_date_in_millis": 1539609701576
      }
    }
  }
}
```
% TESTRESPONSE[skip:not possible to get the cluster into this state in a docs test]

1. The step that caused the error.
2. Indicates if retrying the failed step can overcome the error. If this is true, {{ilm-init}} will retry the failed step automatically.
3. Shows the number of attempted automatic retries to execute the failed step.
4. What went wrong.
5. Contains a copy of the `step_info` field (when it exists) of the last attempted or executed step for diagnostic purposes, since the `step_info` is overwritten during each new attempt.
