---
applies_to:
  stack: all
navigation_title: Update cross-cluster API examples 
---
# Update cross-cluster API key API examples

The [update cross-cluster API key API](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-security-update-cross-cluster-api-key) updates the attributes of an existing cross-cluster API key, which is used for API key based remote cluster access. This page shows you examples of using this API.

If you create a cross-cluster API key as follows:

```console
POST /_security/cross_cluster/api_key
{
  "name": "my-cross-cluster-api-key",
  "access": {
    "search": [
      {
        "names": ["logs*"]
      }
    ]
  },
  "metadata": {
    "application": "search"
  }
}
```

A successful call returns a JSON structure that provides API key information. For example:

```console-result
{
  "id": "VuaCfGcBCdbkQm-e5aOx",
  "name": "my-cross-cluster-api-key",
  "api_key": "ui2lp2axTNmsyakw9tvNnw",
  "encoded": "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw=="
}
```

% TESTRESPONSE[s/VuaCfGcBCdbkQm-e5aOx/$body.id/]
% TESTRESPONSE[s/ui2lp2axTNmsyakw9tvNnw/$body.api_key/]
% TESTRESPONSE[s/VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw==/$body.encoded/]

Information of the API key, including its exact role descriptor can be inspected with the [Get API key API](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-security-get-api-key).

```console
GET /_security/api_key?id=VuaCfGcBCdbkQm-e5aOx
```

% TEST[s/VuaCfGcBCdbkQm-e5aOx/$body.id/]
% TEST[continued]

A successful call returns a JSON structure that contains the information of the API key:

```js
{
  "api_keys": [
    {
      "id": "VuaCfGcBCdbkQm-e5aOx",
      "name": "my-cross-cluster-api-key",
      "type": "cross_cluster",
      "creation": 1548550550158,
      "expiration": null,
      "invalidated": false,
      "username": "myuser",
      "realm": "native1",
      "metadata": {
        "application": "search"
      },
      "role_descriptors": {
        "cross_cluster": {  <1>
          "cluster": [
              "cross_cluster_search"
          ],
          "indices": [
            {
              "names": [
                "logs*"
              ],
              "privileges": [
                "read", "read_cross_cluster", "view_index_metadata"
              ],
              "allow_restricted_indices": false
            }
          ],
          "applications": [ ],
          "run_as": [ ],
          "metadata": { },
          "transient_metadata": {
            "enabled": true
          }
        }
      },
      "access": {  <2>
        "search": [
          {
            "names": [
              "logs*"
            ],
            "allow_restricted_indices": false
          }
        ]
      }
    }
  ]
}
```

% NOTCONSOLE

1. Role descriptor corresponding to the specified `access` scope at creation time.
In this example, it grants cross cluster search permission for the `logs*` index pattern.
2. The `access` corresponds to the value specified at API key creation time.

The following example updates the API key created above, assigning it new access scope and metadata:

```console
PUT /_security/cross_cluster/api_key/VuaCfGcBCdbkQm-e5aOx
{
  "access": {
    "replication": [
      {
        "names": ["archive"]
      }
    ]
  },
  "metadata": {
    "application": "replication"
  }
}
```

% TEST[s/VuaCfGcBCdbkQm-e5aOx/\${body.api_keys.0.id}/]
% TEST[continued]

A successful call returns a JSON structure indicating that the API key was updated:

```console-result
{
  "updated": true
}
```

The API key's permissions after the update can be inspected again with the [Get API key API](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-security-get-api-key) and it will be:

```js
{
  "api_keys": [
    {
      "id": "VuaCfGcBCdbkQm-e5aOx",
      "name": "my-cross-cluster-api-key",
      "type": "cross_cluster",
      "creation": 1548550550158,
      "expiration": null,
      "invalidated": false,
      "username": "myuser",
      "realm": "native1",
      "metadata": {
        "application": "replication"
      },
      "role_descriptors": {
        "cross_cluster": {  <1>
          "cluster": [
              "cross_cluster_replication"
          ],
          "indices": [
            {
              "names": [
                "archive*"
              ],
              "privileges": [
                "cross_cluster_replication", "cross_cluster_replication_internal"
              ],
              "allow_restricted_indices": false
            }
          ],
          "applications": [ ],
          "run_as": [ ],
          "metadata": { },
          "transient_metadata": {
            "enabled": true
          }
        }
      },
      "access": {  <2>
        "replication": [
          {
            "names": [
              "archive*"
            ],
            "allow_restricted_indices": false
          }
        ]
      }
    }
  ]
}
```

% NOTCONSOLE

1. Role descriptor is updated to be the `access` scope specified at update time.
In this example, it is updated to grant the cross cluster replication permission
for the `archive*` index pattern.
2. The `access` corresponds to the value specified at API key update time.
