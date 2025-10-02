---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/8.18/security-api-query-api-key.html#security-api-query-api-key-example
applies_to:
  stack: all
navigation_title: Query API key information
---

# Query API key information examples

This page provides usage examples for the [Query API key information API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-query-api-keys), which retrieves API key metadata in a paginated fashion. These examples demonstrate how to retrieve, filter, sort, and aggregate API key data using query DSL and aggregation features.

You can learn how to:

- [Retrieve all API keys](#retrieve-all-api-keys)
- [Group API keys by owner and expiration](#group-api-keys-by-owner-and-expiration)
- [Group invalidated API keys by owner and name](#group-invalidated-api-keys-by-owner-and-name)

<!--

```console
POST /_security/user/king
{
  "password" : "security-test-password",
  "roles": []
}
POST /_security/user/june
{
  "password" : "security-test-password",
  "roles": []
}
POST /_security/api_key/grant
{
  "grant_type": "password",
  "username" : "king",
  "password" : "security-test-password",
  "api_key" : {
    "name": "king-key-no-expire"
  }
}
DELETE /_security/api_key
{
  "name" : "king-key-no-expire"
}
POST /_security/api_key/grant
{
  "grant_type": "password",
  "username" : "king",
  "password" : "security-test-password",
  "api_key" : {
    "name": "king-key-10",
    "expiration": "10d"
  }
}
POST /_security/api_key/grant
{
  "grant_type": "password",
  "username" : "king",
  "password" : "security-test-password",
  "api_key" : {
    "name": "king-key-100",
    "expiration": "100d"
  }
}
POST /_security/api_key/grant
{
  "grant_type": "password",
  "username" : "june",
  "password" : "security-test-password",
  "api_key" : {
    "name": "june-key-no-expire"
  }
}
POST /_security/api_key/grant
{
  "grant_type": "password",
  "username" : "june",
  "password" : "security-test-password",
  "api_key" : {
    "name": "june-key-10",
    "expiration": "10d"
  }
}
POST /_security/api_key/grant
{
  "grant_type": "password",
  "username" : "june",
  "password" : "security-test-password",
  "api_key" : {
    "name": "june-key-100",
    "expiration": "100d"
  }
}
DELETE /_security/api_key
{
  "name" : "june-key-100"
}
```
% TESTSETUP

```console
DELETE /_security/user/king
DELETE /_security/user/june
DELETE /_security/api_key
{
  "name" : "king-key-no-expire"
}
DELETE /_security/api_key
{
  "name" : "king-key-10"
}
DELETE /_security/api_key
{
  "name" : "king-key-100"
}
DELETE /_security/api_key
{
  "name" : "june-key-no-expire"
}
DELETE /_security/api_key
{
  "name" : "june-key-10"
}
```
% TEARDOWN
-->

## Retrieve all API keys

The following request lists all API keys, assuming you have the `manage_api_key` privilege:

```console
GET /_security/_query/api_key
```

A successful call returns a JSON structure that contains the information retrieved from one or more API keys:

```js
{
  "total": 3,
  "count": 3,
  "api_keys": [ <1>
    {
      "id": "nkvrGXsB8w290t56q3Rg",
      "name": "my-api-key-1",
      "creation": 1628227480421,
      "expiration": 1629091480421,
      "invalidated": false,
      "username": "elastic",
      "realm": "reserved",
      "realm_type": "reserved",
      "metadata": {
        "letter": "a"
      },
      "role_descriptors": { <2>
        "role-a": {
          "cluster": [
            "monitor"
          ],
          "indices": [
            {
              "names": [
                "index-a"
              ],
              "privileges": [
                "read"
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
      }
    },
    {
      "id": "oEvrGXsB8w290t5683TI",
      "name": "my-api-key-2",
      "creation": 1628227498953,
      "expiration": 1628313898953,
      "invalidated": false,
      "username": "elastic",
      "realm": "reserved",
      "metadata": {
        "letter": "b"
      },
      "role_descriptors": { } <3>
    }
  ]
}
```
% NOTCONSOLE

1. The list of API keys that were retrieved for this request
2. The role descriptors that are assigned to this API key when it was [created](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key#operation-security-create-api-key-body-application-json-role_descriptors) or last [updated](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-api-key#operation-security-update-api-key-body-application-json-role_descriptors) The API key’s effective permissions are the intersection of its assigned privileges and a point-in-time snapshot of the owner user’s permissions.
3. An empty role descriptors means the API key inherits the owner user's permissions.

If you create an API key with the following details:

```console
POST /_security/api_key
{
  "name": "application-key-1",
  "metadata": { "application": "my-application"}
}
```

A successful call returns a JSON structure that provides API key information. For example:

```console-result
  "id": "VuaCfGcBCdbkQm-e5aOx",
  "name": "application-key-1",
  "api_key": "ui2lp2axTNmsyakw9tvNnw",
  "encoded": "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw=="
```
% TESTRESPONSE[s/VuaCfGcBCdbkQm-e5aOx/$body.id/]
% TESTRESPONSE[s/ui2lp2axTNmsyakw9tvNnw/$body.api_key/]
% TESTRESPONSE[s/VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw==/$body.encoded/]

Use the information from the response to retrieve the API key by ID:

```console
GET /_security/_query/api_key?with_limited_by=true
{
  "query": {
    "ids": {
      "values": [
        "VuaCfGcBCdbkQm-e5aOx"
      ]
    }
  }
}
```
% TEST[s/VuaCfGcBCdbkQm-e5aOx/$body.id/]
% TEST[continued]

A successful call returns a JSON structure for API key information including its limited-by role descriptors:

```js
{
  "api_keys": [
    {
      "id": "VuaCfGcBCdbkQm-e5aOx",
      "name": "application-key-1",
      "creation": 1548550550158,
      "expiration": 1548551550158,
      "invalidated": false,
      "username": "myuser",
      "realm": "native1",
      "realm_type": "native",
      "metadata": {
        "application": "my-application"
      },
      "role_descriptors": { },
      "limited_by": [ <1>
        {
          "role-power-user": {
            "cluster": [
              "monitor"
            ],
            "indices": [
              {
                "names": [
                  "*"
                ],
                "privileges": [
                  "read"
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
        }
      ]
    }
  ]
}
```
% NOTCONSOLE

1. The owner user's permissions associated with the API key.
It is a point-in-time snapshot captured at [creation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key#operation-security-create-api-key-body-application-json-role_descriptors) and
subsequent [updates](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-api-key#operation-security-update-api-key-body-application-json-role_descriptors). An API key's
effective permissions are an intersection of its assigned privileges and
the owner user's permissions.

You can also retrieve the API key by name:

```console
GET /_security/_query/api_key
{
  "query": {
    "term": {
      "name": {
        "value": "application-key-1"
      }
    }
  }
}
```
% TEST[continued]

Use a `bool` query to issue complex logical conditions and use `from`, `size`, `sort` to help paginate the result:

```js
GET /_security/_query/api_key
{
  "query": {
    "bool": {
      "must": [
        {
          "prefix": {
            "name": "app1-key-" <1>
          }
        },
        {
          "term": {
            "invalidated": "false" <2>
          }
        }
      ],
      "must_not": [
        {
          "term": {
            "name": "app1-key-01" <3>
          }
        }
      ],
      "filter": [
        {
          "wildcard": {
            "username": "org-*-user" <4>
          }
        },
        {
          "term": {
            "metadata.environment": "production" <5>
          }
        }
      ]
    }
  },
  "from": 20, <6>
  "size": 10, <7>
  "sort": [ <8>
    { "creation": { "order": "desc", "format": "date_time" } },
    "name"
  ]
}
```
% NOTCONSOLE

1. The API key name must begin with `app1-key-`
2. The API key must still be valid
3. The API key name must not be `app1-key-01`
4. The API key must be owned by a username of the [wildcard](../../query-languages/query-dsl/query-dsl-wildcard-query.md) pattern `org-*-user`
5. The API key must have the metadata field `environment` that has the value of `production`
6. The offset to begin the search result is the 20th (zero-based index) API key
7. The page size of the response is 10 API keys
8. The result is first sorted by `creation` date in descending order, then by name in ascending order

The response contains a list of matched API keys along with their sort values:

```js
{
  "total": 100,
  "count": 10,
  "api_keys": [
    {
      "id": "CLXgVnsBOGkf8IyjcXU7",
      "name": "app1-key-79",
      "creation": 1629250154811,
      "invalidated": false,
      "username": "org-admin-user",
      "realm": "native1",
      "metadata": {
        "environment": "production"
      },
      "role_descriptors": { },
      "_sort": [
        "2021-08-18T01:29:14.811Z",  <1>
        "app1-key-79"  <2>
      ]
    },
    {
      "id": "BrXgVnsBOGkf8IyjbXVB",
      "name": "app1-key-78",
      "creation": 1629250153794,
      "invalidated": false,
      "username": "org-admin-user",
      "realm": "native1",
      "metadata": {
        "environment": "production"
      },
      "role_descriptors": { },
      "_sort": [
        "2021-08-18T01:29:13.794Z",
        "app1-key-78"
      ]
    },
    ...
  ]
}
```
% NOTCONSOLE

1. The first sort value is creation time, which is displayed in `date_time` <<mapping-date-format,format>> as defined in the request
2. The second sort value is the API key name

## Group API keys by owner and expiration

For example, given 2 users "june" and "king", each owning 3 API keys:

- one that never expires (invalidated for user "king")
- one that expires in 10 days
- and one that expires in 100 day (invalidated for user "june")

The following request returns the names of valid (not expired and not invalidated) API keys that expire soon (in 30 days time), grouped by owner username.

### Request

```console
POST /_security/_query/api_key
{
  "size": 0,
  "query": {
    "bool": {
      "must": {
        "term": {
          "invalidated": false  <1>
        }
      },
      "should": [  <2>
        {
          "range": { "expiration": { "gte": "now" } }
        },
        {
          "bool": { "must_not": { "exists": { "field": "expiration" } } }
        }
      ],
      "minimum_should_match": 1
    }
  },
  "aggs": {
    "keys_by_username": {
      "composite": {
        "sources": [ { "usernames": { "terms": { "field": "username" } } } ]  <3>
      },
      "aggs": {
        "expires_soon": {
          "filter": {
            "range": { "expiration": { "lte": "now+30d/d" } }  <4>
          },
          "aggs": {
            "key_names": { "terms": { "field": "name" } }
          }
        }
      }
    }
  }
}
```

1. Matching API keys must not be invalidated
2. Matching API keys must be either not expired or not have an expiration date
3. Aggregate all matching keys (i.e. all valid keys) by their owner username
4. Further aggregate the per-username valid keys into a soon-to-expire bucket

### Response

```console-result
{
  "total" : 4,  <1>
  "count" : 0,
  "api_keys" : [ ],
  "aggregations" : {
    "keys_by_username" : {
      "after_key" : {
        "usernames" : "king"
      },
      "buckets" : [
        {
          "key" : {
            "usernames" : "june"
          },
          "doc_count" : 2,  <2>
          "expires_soon" : {
            "doc_count" : 1,
            "key_names" : {
              "doc_count_error_upper_bound" : 0,
              "sum_other_doc_count" : 0,
              "buckets" : [
                {
                  "key" : "june-key-10",
                  "doc_count" : 1
                }
              ]
            }
          }
        },
        {
          "key" : {
            "usernames" : "king"
          },
          "doc_count" : 2,
          "expires_soon" : {
            "doc_count" : 1,  <3>
            "key_names" : {
              "doc_count_error_upper_bound" : 0,
              "sum_other_doc_count" : 0,
              "buckets" : [  <4>
                {
                  "key" : "king-key-10",
                  "doc_count" : 1
                }
              ]
            }
          }
        }
      ]
    }
  }
}

```

1. Total number of valid API keys (2 for each user)
2. Number of valid API keys for user "june"
3. Number of valid API keys expiring soon for user "king"
4. The names of soon-to-expire API keys for user "king"

## Group invalidated API keys by owner and name

To retrieve the invalidated (but not yet deleted) API keys, grouped by owner username and API key name, issue the following request:

### Request

```console
POST /_security/_query/api_key
{
  "size": 0,
  "query": {
    "bool": {
      "filter": {
        "term": {
          "invalidated": true
        }
      }
    }
  },
  "aggs": {
    "invalidated_keys": {
      "composite": {
        "sources": [
          { "username": { "terms": { "field": "username" } } },
          { "key_name": { "terms": { "field": "name" } } }
        ]
      }
    }
  }
}
```

### Response

```console-result
{
  "total" : 2,
  "count" : 0,
  "api_keys" : [ ],
  "aggregations" : {
    "invalidated_keys" : {
      "after_key" : {
        "username" : "king",
        "key_name" : "king-key-no-expire"
      },
      "buckets" : [
        {
          "key" : {
            "username" : "june",
            "key_name" : "june-key-100"
          },
          "doc_count" : 1
        },
        {
          "key" : {
            "username" : "king",
            "key_name" : "king-key-no-expire"
          },
          "doc_count" : 1
        }
      ]
    }
  }
}
```