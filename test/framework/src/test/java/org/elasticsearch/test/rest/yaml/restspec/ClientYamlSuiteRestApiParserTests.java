/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.yaml.section.AbstractClientYamlTestFragmentParserTestCase;

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
            assertThat(next.getPath(), equalTo("/{index}/{type}"));
            assertThat(next.getMethods().length, equalTo(1));
            assertThat(next.getMethods()[0], equalTo("POST"));
            assertThat(next.getParts().size(), equalTo(2));
            assertThat(next.getParts(), containsInAnyOrder("index", "type"));
        }
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.getPath(), equalTo("/{index}/{type}/{id}"));
            assertThat(next.getMethods().length, equalTo(1));
            assertThat(next.getMethods()[0], equalTo("PUT"));
            assertThat(next.getParts().size(), equalTo(3));
            assertThat(next.getParts(), containsInAnyOrder("id", "index", "type"));
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
            assertThat(next.getPath(), equalTo("/_template"));
            assertThat(next.getMethods().length, equalTo(1));
            assertThat(next.getMethods()[0], equalTo("GET"));
            assertEquals(0, next.getParts().size());
        }
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.getPath(), equalTo("/_template/{name}"));
            assertThat(next.getMethods().length, equalTo(1));
            assertThat(next.getMethods()[0], equalTo("GET"));
            assertThat(next.getParts().size(), equalTo(1));
            assertThat(next.getParts(), contains("name"));
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
            assertThat(next.getPath(), equalTo("/_count"));
            assertThat(next.getMethods().length, equalTo(2));
            assertThat(next.getMethods()[0], equalTo("POST"));
            assertThat(next.getMethods()[1], equalTo("GET"));
            assertEquals(0, next.getParts().size());
        }
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.getPath(), equalTo("/{index}/_count"));
            assertThat(next.getMethods().length, equalTo(2));
            assertThat(next.getMethods()[0], equalTo("POST"));
            assertThat(next.getMethods()[1], equalTo("GET"));
            assertEquals(1, next.getParts().size());
            assertThat(next.getParts(), contains("index"));
        }
        {
            ClientYamlSuiteRestApi.Path next = iterator.next();
            assertThat(next.getPath(), equalTo("/{index}/{type}/_count"));
            assertThat(next.getMethods().length, equalTo(2));
            assertThat(next.getMethods()[0], equalTo("POST"));
            assertThat(next.getMethods()[1], equalTo("GET"));
            assertThat(next.getParts(), containsInAnyOrder("index", "type"));
        }
        assertThat(restApi.getParams().size(), equalTo(1));
        assertThat(restApi.getParams().keySet(), contains("ignore_unavailable"));
        assertThat(restApi.getParams(), hasEntry("ignore_unavailable", false));
        assertThat(restApi.isBodySupported(), equalTo(true));
        assertThat(restApi.isBodyRequired(), equalTo(false));
    }

    public void testRequiredBodyWithoutUrlParts() throws Exception {
        String spec = "{\n" +
            "  \"count\": {\n" +
            "    \"documentation\": \"whatever\",\n" +
            "    \"stability\": \"stable\",\n" +
            "    \"visibility\": \"public\",\n" +
            "    \"url\": {\n" +
            "      \"paths\": [ \n" +
            "        {\n" +
            "          \"path\":\"/whatever\",\n" +
            "          \"methods\":[\n" +
            "            \"POST\",\n" +
            "            \"GET\"\n" +
            "          ]\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    \"body\": {\n" +
            "      \"description\" : \"whatever\",\n" +
            "      \"required\" : true\n" +
            "    }\n" +
            "  }\n" +
            "}";

        parser = createParser(YamlXContent.yamlXContent, spec);
        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApiParser().parse("count.json", parser);

        assertThat(restApi, notNullValue());
        assertThat(restApi.getPaths().size(), equalTo(1));
        assertThat(restApi.getPaths().iterator().next().getParts().isEmpty(), equalTo(true));
        assertThat(restApi.getParams().isEmpty(), equalTo(true));
        assertThat(restApi.isBodyRequired(), equalTo(true));
    }

    private static final String REST_SPEC_COUNT_API = "{\n" +
        "  \"count\":{\n" +
        "    \"documentation\":{\n" +
        "      \"url\":\"https://www.elastic.co/guide/en/elasticsearch/reference/master/search-count.html\",\n" +
        "      \"description\":\"Returns number of documents matching a query.\"\n" +
        "    },\n" +
        "    \"stability\": \"stable\",\n" +
        "    \"visibility\": \"public\",\n" +
        "    \"headers\": { \"accept\": [\"application/json\"] },\n" +
        "    \"url\":{\n" +
        "      \"paths\":[\n" +
        "        {\n" +
        "          \"path\":\"/_count\",\n" +
        "          \"methods\":[\n" +
        "            \"POST\",\n" +
        "            \"GET\"\n" +
        "          ]\n" +
        "        },\n" +
        "        {\n" +
        "          \"path\":\"/{index}/_count\",\n" +
        "          \"methods\":[\n" +
        "            \"POST\",\n" +
        "            \"GET\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"index\":{\n" +
        "              \"type\":\"list\",\n" +
        "              \"description\":\"A comma-separated list of indices to restrict the results\"\n" +
        "            }\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"path\":\"/{index}/{type}/_count\",\n" +
        "          \"methods\":[\n" +
        "            \"POST\",\n" +
        "            \"GET\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"index\":{\n" +
        "              \"type\":\"list\",\n" +
        "              \"description\":\"A comma-separated list of indices to restrict the results\"\n" +
        "            },\n" +
        "            \"type\":{\n" +
        "              \"type\":\"list\",\n" +
        "              \"description\":\"A comma-separated list of types to restrict the results\",\n" +
        "              \"deprecated\":true\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"params\":{\n" +
        "      \"ignore_unavailable\":{\n" +
        "        \"type\":\"boolean\",\n" +
        "        \"description\":\"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"body\":{\n" +
        "      \"description\":\"A query to restrict the results specified with the Query DSL (optional)\",\n" +
        "      \"content_type\": [\"application/json\"]\n" +
        "    }\n" +
        "  }\n" +
        "}\n\n";

    private static final String REST_SPEC_GET_TEMPLATE_API = "{\n" +
        "  \"indices.get_template\":{\n" +
        "    \"documentation\":{\n" +
        "      \"url\":\"https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-templates.html\",\n" +
        "      \"description\":\"Returns an index template.\"\n" +
        "    },\n" +
        "    \"headers\": { \"accept\": [\"application/json\"] },\n" +
        "    \"stability\": \"stable\",\n" +
        "    \"visibility\": \"public\",\n" +
        "    \"url\":{\n" +
        "      \"paths\":[\n" +
        "        {\n" +
        "          \"path\":\"/_template\",\n" +
        "          \"methods\":[\n" +
        "            \"GET\"\n" +
        "          ]\n" +
        "        },\n" +
        "        {\n" +
        "          \"path\":\"/_template/{name}\",\n" +
        "          \"methods\":[\n" +
        "            \"GET\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"name\":{\n" +
        "              \"type\":\"list\",\n" +
        "              \"description\":\"The comma separated names of the index templates\"\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  }\n" +
        "}\n";

    private static final String REST_SPEC_INDEX_API = "{\n" +
        "  \"index\":{\n" +
        "    \"documentation\":{\n" +
        "      \"url\":\"https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-index_.html\",\n" +
        "      \"description\":\"Creates or updates a document in an index.\"\n" +
        "    },\n" +
        "    \"stability\": \"stable\",\n" +
        "    \"visibility\": \"public\",\n" +
        "    \"headers\": { " +
        "       \"accept\": [\"application/json\"],\n " +
        "       \"content_type\": [\"application/json\", \"a/mime-type\"]\n " +
        "   },\n" +
        "    \"url\":{\n" +
        "      \"paths\":[\n" +
        "        {\n" +
        "          \"path\":\"/{index}/{type}\",\n" +
        "          \"methods\":[\n" +
        "            \"POST\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"index\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"description\":\"The name of the index\"\n" +
        "            },\n" +
        "            \"type\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"description\":\"The type of the document\",\n" +
        "              \"deprecated\":true\n" +
        "            }\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"path\":\"/{index}/{type}/{id}\",\n" +
        "          \"methods\":[\n" +
        "            \"PUT\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"id\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"description\":\"Document ID\"\n" +
        "            },\n" +
        "            \"index\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"description\":\"The name of the index\"\n" +
        "            },\n" +
        "            \"type\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"description\":\"The type of the document\",\n" +
        "              \"deprecated\":true\n" +
        "            }\n" +
        "          },\n" +
        "          \"deprecated\":{\n" +
        "            \"version\":\"7.0.0\",\n" +
        "            \"description\":\"Specifying types in urls has been deprecated\"\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"params\":{\n" +
        "      \"wait_for_active_shards\":{\n" +
        "        \"type\":\"string\",\n" +
        "        \"description\":\"Sets the number of shard copies that must be active before proceeding with the index operation. \"\n" +
        "      },\n" +
        "      \"op_type\":{\n" +
        "        \"type\":\"enum\",\n" +
        "        \"options\":[\n" +
        "          \"index\",\n" +
        "          \"create\"\n" +
        "        ],\n" +
        "        \"default\":\"index\",\n" +
        "        \"description\":\"Explicit operation type\"\n" +
        "      },\n" +
        "      \"refresh\":{\n" +
        "        \"type\":\"enum\",\n" +
        "        \"options\":[\n" +
        "          \"true\",\n" +
        "          \"false\",\n" +
        "          \"wait_for\"\n" +
        "        ],\n" +
        "        \"description\":\"If `true` then refresh the affected shards to make this operation visible to search\"\n" +
        "      },\n" +
        "      \"routing\":{\n" +
        "        \"type\":\"string\",\n" +
        "        \"description\":\"Specific routing value\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"body\":{\n" +
        "      \"description\":\"The document\",\n" +
        "      \"content_type\": [\"application/json\"],\n" +
        "      \"required\":true\n" +
        "    }\n" +
        "  }\n" +
        "}\n";
}
