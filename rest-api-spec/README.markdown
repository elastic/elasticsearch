# Elasticsearch REST API JSON specification

This repository contains a collection of JSON files which describe the [Elasticsearch](http://elastic.co) HTTP API.

Their purpose is to formalize and standardize the API, to facilitate development of libraries and integrations.

Example for the ["Create Index"](http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html) API:

```json
{
  "indices.create": {
    "documentation": "http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html",
    "methods": ["PUT", "POST"],
    "url": {
      "path": "/{index}",
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
* List of HTTP methods for the endpoint
* URL specification: path, parts, parameters
* Whether body is allowed for the endpoint or not and its description

The `methods` and `url.paths` elements list all possible HTTP methods and URLs for the endpoint;
it is the responsibility of the developer to use this information for a sensible API on the target platform.

## License

This software is licensed under the Apache License, version 2 ("ALv2").
