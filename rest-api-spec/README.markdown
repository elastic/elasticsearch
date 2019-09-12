# Elasticsearch REST API JSON specification

This repository contains a collection of JSON files which describe the [Elasticsearch](http://elastic.co) HTTP API.

Their purpose is to formalize and standardize the API, to facilitate development of libraries and integrations.

Example for the ["Create Index"](http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html) API:

```json
{
  "indices.create": {
    "documentation":{
      "url":"http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html"
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
