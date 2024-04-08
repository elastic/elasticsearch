/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ClientYamlSuiteRestApiTests extends ESTestCase {

    public void testParseCommonSpec() throws IOException {
        XContentParser parser = createParser(YamlXContent.yamlXContent, COMMON_SPEC);
        ClientYamlSuiteRestSpec restSpec = new ClientYamlSuiteRestSpec();
        ClientYamlSuiteRestSpec.parseCommonSpec(parser, restSpec);
        assertTrue(restSpec.isGlobalParameter("pretty"));
        assertTrue(restSpec.isGlobalParameter("human"));
        assertTrue(restSpec.isGlobalParameter("error_trace"));
        assertTrue(restSpec.isGlobalParameter("source"));
        assertTrue(restSpec.isGlobalParameter("filter_path"));
        assertFalse(restSpec.isGlobalParameter("unknown"));
    }

    public void testPathMatching() throws IOException {
        XContentParser parser = createParser(YamlXContent.yamlXContent, REST_SPEC_API);
        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApiParser().parse("index.json", parser);
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Collections.emptySet());
            assertEquals(1, paths.size());
            assertEquals("/_doc", paths.get(0).path());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Collections.singleton("wait_for_active_shards"));
            assertEquals(1, paths.size());
            assertEquals("/_doc", paths.get(0).path());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Collections.singleton("index"));
            assertEquals(1, paths.size());
            assertEquals("/{index}/_doc", paths.get(0).path());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Set.of("index", "id"));
            assertEquals(1, paths.size());
            assertEquals("/{index}/_doc/{id}", paths.get(0).path());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Set.of("index", "type"));
            assertEquals(3, paths.size());
            assertEquals("/{index}/_mapping/{type}", paths.get(0).path());
            assertEquals("/{index}/{type}", paths.get(1).path());
            assertEquals("/{index}/_mappings/{type}", paths.get(2).path());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Set.of("index", "type", "id"));
            assertEquals(1, paths.size());
            assertEquals("/{index}/{type}/{id}", paths.get(0).path());
        }
    }

    private static final String COMMON_SPEC = """
        {
          "documentation" : {
            "url": "Parameters that are accepted by all API endpoints.",
            "documentation": "https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html"
          },
          "params": {
            "pretty": {
              "type": "boolean",
              "description": "Pretty format the returned JSON response.",
              "default": false
            },
            "human": {
              "type": "boolean",
              "description": "Return human readable values for statistics.",
              "default": true
            },
            "error_trace": {
              "type": "boolean",
              "description": "Include the stack trace of returned errors.",
              "default": false
            },
            "source": {
              "type": "string",
              "description": "The URL-encoded request definition. Useful for libraries that do not accept a request body\
        for non-POST requests."
            },
            "filter_path": {
              "type": "list",
              "description": "A comma-separated list of filters used to reduce the response."
            }
          }
        }
        """;

    private static final String REST_SPEC_API = """
        {
          "index":{
            "documentation":{
              "url":"https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-index_.html",
              "description":"Creates or updates a document in an index."
            },
            "stability":"stable",
            "visibility": "public",
            "headers": { "accept": ["application/json"] },
            "url":{
              "paths":[
                {
                  "path":"/_doc",
                  "methods":[
                    "PUT"
                  ],
                  "parts":{
                  }
                },
                {
                  "path":"/{index}/_mapping/{type}",
                  "methods":[
                    "PUT"
                  ],
                  "parts":{
                    "index":{
                      "type":"string",
                      "required":true,
                      "description":"The name of the index"
                    },
                    "type":{
                      "type":"string",
                      "description":"The type of the document"
                    }
                  }
                },
                {
                  "path":"/{index}/_mappings/{type}",
                  "methods":[
                    "PUT"
                  ],
                  "parts":{
                    "index":{
                      "type":"string",
                      "required":true,
                      "description":"The name of the index"
                    },
                    "type":{
                      "type":"string",
                      "description":"The type of the document"
                    }
                  }
                },
                {
                  "path":"/{index}/_doc/{id}",
                  "methods":[
                    "PUT"
                  ],
                  "parts":{
                    "id":{
                      "type":"string",
                      "description":"Document ID"
                    },
                    "index":{
                      "type":"string",
                      "required":true,
                      "description":"The name of the index"
                    }
                  }
                },
                {
                  "path":"/{index}/_doc",
                  "methods":[
                    "POST"
                  ],
                  "parts":{
                    "index":{
                      "type":"string",
                      "required":true,
                      "description":"The name of the index"
                    }
                  }
                },
                {
                  "path":"/{index}/{type}",
                  "methods":[
                    "POST"
                  ],
                  "parts":{
                    "index":{
                      "type":"string",
                      "required":true,
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
                },
                {
                  "path":"/{index}/{type}/{id}",
                  "methods":[
                    "PUT"
                  ],
                  "parts":{
                    "id":{
                      "type":"string",
                      "description":"Document ID"
                    },
                    "index":{
                      "type":"string",
                      "required":true,
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
            },
            "params":{
              "wait_for_active_shards":{
                "type":"string",
                "description":"Sets the number of shard copies that must be active before proceeding with the index operation.\
        Defaults to 1, meaning the primary shard only. Set to `all` for all shard copies, otherwise set to any non-negative value\
        less than or equal to the total number of copies for the shard (number of replicas + 1)"
              },
              "op_type":{
                "type":"enum",
                "options":[
                  "index",
                  "create"
                ],
                "default":"index",
                "description":"Explicit operation type"
              },
              "refresh":{
                "type":"enum",
                "options":[
                  "true",
                  "false",
                  "wait_for"
                ],
                "description":"If `true` then refresh the affected shards to make this operation visible to search, if `wait_for`\
        then wait for a refresh to make this operation visible to search, if `false` (the default) then do nothing with refreshes."
              },
              "routing":{
                "type":"string",
                "description":"Specific routing value"
              },
              "timeout":{
                "type":"time",
                "description":"Explicit operation timeout"
              },
              "version":{
                "type":"number",
                "description":"Explicit version number for concurrency control"
              },
              "version_type":{
                "type":"enum",
                "options":[
                  "internal",
                  "external",
                  "external_gte",
                  "force"
                ],
                "description":"Specific version type"
              },
              "if_seq_no":{
                "type":"number",
                "description":"only perform the index operation if the last operation that has changed the document has the specified\
        sequence number"
              },
              "if_primary_term":{
                "type":"number",
                "description":"only perform the index operation if the last operation that has changed the document has the specified\
        primary term"
              },
              "pipeline":{
                "type":"string",
                "description":"The pipeline id to preprocess incoming documents with"
              }
            },
            "body":{
              "description":"The document",
              "required":true
            }
          }
        }
        """;
}
