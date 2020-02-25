# Elasticsearch REST API JSON specification

This repository contains a collection of JSON files which describe the [Elasticsearch](http://elastic.co) HTTP API.

Their purpose is to formalize and standardize the API, to facilitate development of libraries and integrations.

Example for the ["Create Index"](https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html) API:

```json
{
  "indices.create": {
    "documentation":{
      "url":"https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html"
    },
    "stability": "stable",
    "url":{
      "paths":[
        {
          "path":"/{index}",
          "method":"PUT",
          "parts":{
            "index":{
              "type":"string",
              "description":"The name of the index"
            }
          }
        }
      ]
    },
    "params": {
      "timeout": {
        "type" : "time",
        "description" : "Explicit operation timeout"
      }
    },
    "body": {
      "description" : "The configuration for the index (`settings` and `mappings`)"
    }
  }
}
```

The specification contains:

* The _name_ of the API (`indices.create`), which usually corresponds to the client calls
* Link to the documentation at the <http://elastic.co> website
* `stability` indicating the state of the API, has to be declared explicitly or YAML tests will fail
    * `experimental` highly likely to break in the near future (minor/path), no bwc guarantees.
    Possibly removed in the future.
    * `beta` less likely to break or be removed but still reserve the right to do so
    * `stable` No backwards breaking changes in a minor
* Request URL: HTTP method, path and parts
* Request parameters
* Request body specification

**NOTE**
If an API is stable but it response should be treated as an arbitrary map of key values please notate this as followed

```json
{
  "api.name": {
    "stability" : "stable",
    "response": {
      "treat_json_as_key_value" : true
    }
  }
}
```

## Type definition
In the documentation, you will find the `type` field, which documents which type every parameter will accept.

#### Querystring parameters
| Type  | Description  |
|---|---|
| `list`  | An array of strings *(represented as a comma separated list in the querystring)* |
| `date` | A string representing a date formatted in ISO8601 or a number representing milliseconds since the epoch *(used only in ML)*   |
| `time` | A numeric or string value representing duration |
| `string` | A string value  |
| `enum` | A set of named constants *(a single value should be sent in the querystring)*  |
| `int` | A signed 32-bit integer with a minimum value of -2<sup>31</sup> and a maximum value of 2<sup>31</sup>-1.  |
| `double` | A [double-precision 64-bit IEEE 754](https://en.wikipedia.org/wiki/Floating-point_arithmetic) floating point number, restricted to finite values.  |
| `long` | A signed 64-bit integer with a minimum value of -2<sup>63</sup> and a maximum value of 2<sup>63</sup>-1. *(Note: the max safe integer for JSON is 2<sup>53</sup>-1)* |
| `number` | Alias for `double`. *(deprecated, a more specific type should be used)*  |
| `boolean` | Boolean fields accept JSON true and false values  |

## Backwards compatibility

The specification follows the same backward compatibility guarantees as Elasticsearch.

- Within a Major, additions only.
- If an item has been documented wrong it should be deprecated instead as removing these might break downstream clients.
- Major version change, may deprecate pieces or simply remove them given enough deprecation time.

## Deprecations

The specification schema allows to codify API deprecations, either for an entire API, or for specific parts of the API, such as paths or parameters.

#### Entire API:

```json
{
  "api" : {
    "deprecated" : {
      "version" : "7.0.0",
      "description" : "Reason API is being deprecated"
    },
  }
}
```

#### Specific paths and their parts:

```json
{
  "api": {
    "url": {
      "paths": [
        {
          "path":"/{index}/{type}/{id}/_create",
          "method":"PUT",
          "parts":{
            "id":{
              "type":"string",
              "description":"Document ID"
            },
            "index":{
              "type":"string",
              "description":"The name of the index"
            },
            "type":{
              "type":"string",
              "description":"The type of the document",
              "deprecated":true
            }
          },
          "deprecated":{
            "version":"7.0.0",
            "description":"Specifying types in urls has been deprecated"
          }
        }
      ]
    }
  }
}
```

#### Parameters

```json
{
  "api": {
    "url": {
      "params": {
        "stored_fields": {
          "type": "list",
          "description" : "",
          "deprecated" : {
            "version" : "7.0.0",
            "description" : "Reason parameter is being deprecated"
          }
        }
      }
    }
  }
}
```

## License

This software is licensed under the Apache License, version 2 ("ALv2").
