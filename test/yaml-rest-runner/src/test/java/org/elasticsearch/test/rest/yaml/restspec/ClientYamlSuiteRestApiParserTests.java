/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.test.rest.yaml.section.AbstractClientYamlTestFragmentParserTestCase;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.util.Iterator;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClientYamlSuiteRestApiParserTests extends AbstractClientYamlTestFragmentParserTestCase {
    public void testParseRestSpecIndexApi() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, REST_SPEC_INDEX_API);
        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApiParser().parse("index.json", parser);

        assertThat(restApi, notNullValue());
        assertThat(restApi.getName(), equalTo("index"));
        assertThat(restApi.getPaths().size(), equalTo(2));
        Iterator<ClientYamlSuiteRestApi.Path> iterator = restApi.getPaths().iterator();
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.path(), equalTo("/{index}/{type}"));
            assertThat(next.methods().length, equalTo(1));
            assertThat(next.methods()[0], equalTo("POST"));
            assertThat(next.parts().size(), equalTo(2));
            assertThat(next.parts(), containsInAnyOrder("index", "type"));
        }
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.path(), equalTo("/{index}/{type}/{id}"));
            assertThat(next.methods().length, equalTo(1));
            assertThat(next.methods()[0], equalTo("PUT"));
            assertThat(next.parts().size(), equalTo(3));
            assertThat(next.parts(), containsInAnyOrder("id", "index", "type"));
        }
        assertThat(restApi.getParams().size(), equalTo(4));
        assertThat(restApi.getParams().keySet(), containsInAnyOrder("wait_for_active_shards", "op_type", "routing", "refresh"));
        restApi.getParams().forEach((key, value) -> assertThat(value, equalTo(false)));
        assertThat(restApi.isBodySupported(), equalTo(true));
        assertThat(restApi.isBodyRequired(), equalTo(true));
        assertThat(restApi.getRequestMimeTypes(), containsInAnyOrder("application/json", "a/mime-type"));
        assertThat(restApi.getResponseMimeTypes(), containsInAnyOrder("application/json"));
    }

    public void testParseRestSpecGetTemplateApi() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, REST_SPEC_GET_TEMPLATE_API);
        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApiParser().parse("indices.get_template.json", parser);
        assertThat(restApi, notNullValue());
        assertThat(restApi.getName(), equalTo("indices.get_template"));
        assertThat(restApi.getPaths().size(), equalTo(2));
        Iterator<ClientYamlSuiteRestApi.Path> iterator = restApi.getPaths().iterator();
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.path(), equalTo("/_template"));
            assertThat(next.methods().length, equalTo(1));
            assertThat(next.methods()[0], equalTo("GET"));
            assertEquals(0, next.parts().size());
        }
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.path(), equalTo("/_template/{name}"));
            assertThat(next.methods().length, equalTo(1));
            assertThat(next.methods()[0], equalTo("GET"));
            assertThat(next.parts().size(), equalTo(1));
            assertThat(next.parts(), contains("name"));
        }
        assertThat(restApi.getParams().size(), equalTo(0));
        assertThat(restApi.isBodySupported(), equalTo(false));
        assertThat(restApi.isBodyRequired(), equalTo(false));
        assertThat(restApi.getRequestMimeTypes(), nullValue());
        assertThat(restApi.getResponseMimeTypes(), containsInAnyOrder("application/json"));
    }

    public void testParseRestSpecCountApi() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, REST_SPEC_COUNT_API);
        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApiParser().parse("count.json", parser);
        assertThat(restApi, notNullValue());
        assertThat(restApi.getName(), equalTo("count"));
        assertThat(restApi.getPaths().size(), equalTo(3));
        Iterator<ClientYamlSuiteRestApi.Path> iterator = restApi.getPaths().iterator();
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.path(), equalTo("/_count"));
            assertThat(next.methods().length, equalTo(2));
            assertThat(next.methods()[0], equalTo("POST"));
            assertThat(next.methods()[1], equalTo("GET"));
            assertEquals(0, next.parts().size());
        }
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.path(), equalTo("/{index}/_count"));
            assertThat(next.methods().length, equalTo(2));
            assertThat(next.methods()[0], equalTo("POST"));
            assertThat(next.methods()[1], equalTo("GET"));
            assertEquals(1, next.parts().size());
            assertThat(next.parts(), contains("index"));
        }
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.path(), equalTo("/{index}/{type}/_count"));
            assertThat(next.methods().length, equalTo(2));
            assertThat(next.methods()[0], equalTo("POST"));
            assertThat(next.methods()[1], equalTo("GET"));
            assertThat(next.parts(), containsInAnyOrder("index", "type"));
        }
        assertThat(restApi.getParams().size(), equalTo(1));
        assertThat(restApi.getParams().keySet(), contains("ignore_unavailable"));
        assertThat(restApi.getParams(), hasEntry("ignore_unavailable", false));
        assertThat(restApi.isBodySupported(), equalTo(true));
        assertThat(restApi.isBodyRequired(), equalTo(false));
    }

    public void testRequiredBodyWithoutUrlParts() throws Exception {
        String spec = """
            {
              "count": {
                "documentation": "whatever",
                "stability": "stable",
                "visibility": "public",
                "url": {
                  "paths": [\s
                    {
                      "path":"/whatever",
                      "methods":[
                        "POST",
                        "GET"
                      ]
                    }
                  ]
                },
                "body": {
                  "description" : "whatever",
                  "required" : true
                }
              }
            }""";

        parser = createParser(YamlXContent.yamlXContent, spec);
        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApiParser().parse("count.json", parser);

        assertThat(restApi, notNullValue());
        assertThat(restApi.getPaths().size(), equalTo(1));
        assertThat(restApi.getPaths().iterator().next().parts().isEmpty(), equalTo(true));
        assertThat(restApi.getParams().isEmpty(), equalTo(true));
        assertThat(restApi.isBodyRequired(), equalTo(true));
    }

    private static final String REST_SPEC_COUNT_API = """
        {
          "count":{
            "documentation":{
              "url":"https://www.elastic.co/guide/en/elasticsearch/reference/master/search-count.html",
              "description":"Returns number of documents matching a query."
            },
            "stability": "stable",
            "visibility": "public",
            "headers": { "accept": ["application/json"] },
            "url":{
              "paths":[
                {
                  "path":"/_count",
                  "methods":[
                    "POST",
                    "GET"
                  ]
                },
                {
                  "path":"/{index}/_count",
                  "methods":[
                    "POST",
                    "GET"
                  ],
                  "parts":{
                    "index":{
                      "type":"list",
                      "description":"A comma-separated list of indices to restrict the results"
                    }
                  }
                },
                {
                  "path":"/{index}/{type}/_count",
                  "methods":[
                    "POST",
                    "GET"
                  ],
                  "parts":{
                    "index":{
                      "type":"list",
                      "description":"A comma-separated list of indices to restrict the results"
                    },
                    "type":{
                      "type":"list",
                      "description":"A comma-separated list of types to restrict the results",
                      "deprecated":true
                    }
                  }
                }
              ]
            },
            "params":{
              "ignore_unavailable":{
                "type":"boolean",
                "description":"Whether specified concrete indices should be ignored when unavailable (missing or closed)"
              }
            },
            "body":{
              "description":"A query to restrict the results specified with the Query DSL (optional)",
              "content_type": ["application/json"]
            }
          }
        }

        """;

    private static final String REST_SPEC_GET_TEMPLATE_API = """
        {
          "indices.get_template":{
            "documentation":{
              "url":"https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-templates.html",
              "description":"Returns an index template."
            },
            "headers": { "accept": ["application/json"] },
            "stability": "stable",
            "visibility": "public",
            "url":{
              "paths":[
                {
                  "path":"/_template",
                  "methods":[
                    "GET"
                  ]
                },
                {
                  "path":"/_template/{name}",
                  "methods":[
                    "GET"
                  ],
                  "parts":{
                    "name":{
                      "type":"list",
                      "description":"The comma separated names of the index templates"
                    }
                  }
                }
              ]
            }
          }
        }
        """;

    private static final String REST_SPEC_INDEX_API = """
        {
          "index":{
            "documentation":{
              "url":"https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-index_.html",
              "description":"Creates or updates a document in an index."
            },
            "stability": "stable",
            "visibility": "public",
            "headers": {        "accept": ["application/json"],
                "content_type": ["application/json", "a/mime-type"]
            },
            "url":{
              "paths":[
                {
                  "path":"/{index}/{type}",
                  "methods":[
                    "POST"
                  ],
                  "parts":{
                    "index":{
                      "type":"string",
                      "description":"The name of the index"
                    },
                    "type":{
                      "type":"string",
                      "description":"The type of the document",
                      "deprecated":true
                    }
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
                "description":"Sets the number of shard copies that must be active before proceeding with the index operation. "
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
                "description":"If `true` then refresh the affected shards to make this operation visible to search"
              },
              "routing":{
                "type":"string",
                "description":"Specific routing value"
              }
            },
            "body":{
              "description":"The document",
              "content_type": ["application/json"],
              "required":true
            }
          }
        }
        """;
}
