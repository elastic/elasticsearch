# Elasticsearch REST API JSON specification

This repository contains a collection of JSON files which describe the [Elasticsearch](http://elastic.co) HTTP API.

Their purpose is to formalize and standardize the API, to facilitate development of libraries and integrations.

Example for the ["Create Index"](http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html) API:

```json
{
  "indices.create": {
    "documentation": "http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html",
    "stability": "stable",
    "methods": ["PUT", "POST"],
    "url": {
      "paths": ["/{index}"],
      "parts": {
        "index": {
          "type" : "string",
          "required" : true,
          "description" : "The name of the index"
        }
      },
      "params": {
        "timeout": {
          "type" : "time",
          "description" : "Explicit operation timeout"
        }
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
* Link to the documentation at <http://elastic.co>
* `stability` indicating the state of the API, has to be declared explicitly or YAML tests will fail
    * `experimental` highly likely to break in the near future (minor/path), no bwc guarantees. 
    Possibly removed in the future.
    * `beta` less likely to break or be removed but still reserve the right to do so
    * `stable` No backwards breaking changes in a minor 
* List of HTTP methods for the endpoint
* URL specification: path, parts, parameters
* Whether body is allowed for the endpoint or not and its description

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


The `methods` and `url.paths` elements list all possible HTTP methods and URLs for the endpoint;
it is the responsibility of the developer to use this information for a sensible API on the target platform.

## Backwards compatibility 

The specification follows the same backward compatibility guarantees as Elasticsearch. 

- Within a Major, additions only. 
  - If an item has been documented wrong it should be deprecated instead as removing these might break downstream clients.
- Major version change, may deprecate pieces or simply remove them given enough deprecation time.

## Deprecations

The spec allows for deprecations of:

#### Entire API:

```json
{
  "api" : {
    "documentation": "...",
    "deprecated" : {
      "version" : "7.0.0",
      "description" : "Reason API is being deprecated"
    },
  }
}
```

#### Specific paths:

```json
{
  "api": {
    "documentation": "",
    "url": {
      "paths": ["/_monitoring/bulk"],
      "deprecated_paths" : [
        {
          "version" : "7.0.0",
          "path" : "/_monitoring/{type}/bulk",
          "description" : "Specifying types in urls has been deprecated"
        }
      ]
    }
  }
}
```

Here `paths` describes the preferred paths and `deprecated_paths` indicates `paths` that will still work but are now 
deprecated.

#### Parameters 

```json
{
  "api": {
    "documentation": "",
    "methods": ["GET"],
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
