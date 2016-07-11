/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.rest.test;

import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.spec.RestApi;
import org.elasticsearch.test.rest.spec.RestApiParser;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class RestApiParserTests extends AbstractParserTestCase {
    public void testParseRestSpecIndexApi() throws Exception {
        parser = JsonXContent.jsonXContent.createParser(REST_SPEC_INDEX_API);
        RestApi restApi = new RestApiParser().parse("location", parser);

        assertThat(restApi, notNullValue());
        assertThat(restApi.getName(), equalTo("index"));
        assertThat(restApi.getMethods().size(), equalTo(2));
        assertThat(restApi.getMethods().get(0), equalTo("POST"));
        assertThat(restApi.getMethods().get(1), equalTo("PUT"));
        assertThat(restApi.getPaths().size(), equalTo(2));
        assertThat(restApi.getPaths().get(0), equalTo("/{index}/{type}"));
        assertThat(restApi.getPaths().get(1), equalTo("/{index}/{type}/{id}"));
        assertThat(restApi.getPathParts().size(), equalTo(3));
        assertThat(restApi.getPathParts().get(0), equalTo("id"));
        assertThat(restApi.getPathParts().get(1), equalTo("index"));
        assertThat(restApi.getPathParts().get(2), equalTo("type"));
        assertThat(restApi.getParams().size(), equalTo(4));
        assertThat(restApi.getParams(), contains("consistency", "op_type", "parent", "refresh"));
        assertThat(restApi.isBodySupported(), equalTo(true));
        assertThat(restApi.isBodyRequired(), equalTo(true));
    }

    public void testParseRestSpecGetTemplateApi() throws Exception {
        parser = JsonXContent.jsonXContent.createParser(REST_SPEC_GET_TEMPLATE_API);
        RestApi restApi = new RestApiParser().parse("location", parser);
        assertThat(restApi, notNullValue());
        assertThat(restApi.getName(), equalTo("indices.get_template"));
        assertThat(restApi.getMethods().size(), equalTo(1));
        assertThat(restApi.getMethods().get(0), equalTo("GET"));
        assertThat(restApi.getPaths().size(), equalTo(2));
        assertThat(restApi.getPaths().get(0), equalTo("/_template"));
        assertThat(restApi.getPaths().get(1), equalTo("/_template/{name}"));
        assertThat(restApi.getPathParts().size(), equalTo(1));
        assertThat(restApi.getPathParts().get(0), equalTo("name"));
        assertThat(restApi.getParams().size(), equalTo(0));
        assertThat(restApi.isBodySupported(), equalTo(false));
        assertThat(restApi.isBodyRequired(), equalTo(false));
    }

    public void testParseRestSpecCountApi() throws Exception {
        parser = JsonXContent.jsonXContent.createParser(REST_SPEC_COUNT_API);
        RestApi restApi = new RestApiParser().parse("location", parser);
        assertThat(restApi, notNullValue());
        assertThat(restApi.getName(), equalTo("count"));
        assertThat(restApi.getMethods().size(), equalTo(2));
        assertThat(restApi.getMethods().get(0), equalTo("POST"));
        assertThat(restApi.getMethods().get(1), equalTo("GET"));
        assertThat(restApi.getPaths().size(), equalTo(3));
        assertThat(restApi.getPaths().get(0), equalTo("/_count"));
        assertThat(restApi.getPaths().get(1), equalTo("/{index}/_count"));
        assertThat(restApi.getPaths().get(2), equalTo("/{index}/{type}/_count"));
        assertThat(restApi.getPathParts().size(), equalTo(2));
        assertThat(restApi.getPathParts().get(0), equalTo("index"));
        assertThat(restApi.getPathParts().get(1), equalTo("type"));
        assertThat(restApi.getParams().size(), equalTo(1));
        assertThat(restApi.getParams().get(0), equalTo("ignore_unavailable"));
        assertThat(restApi.isBodySupported(), equalTo(true));
        assertThat(restApi.isBodyRequired(), equalTo(false));
    }

    private static final String REST_SPEC_COUNT_API = "{\n" +
            "  \"count\": {\n" +
            "    \"documentation\": \"http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-count.html\",\n" +
            "    \"methods\": [\"POST\", \"GET\"],\n" +
            "    \"url\": {\n" +
            "      \"path\": \"/_count\",\n" +
            "      \"paths\": [\"/_count\", \"/{index}/_count\", \"/{index}/{type}/_count\"],\n" +
            "      \"parts\": {\n" +
            "        \"index\": {\n" +
            "          \"type\" : \"list\",\n" +
            "          \"description\" : \"A comma-separated list of indices to restrict the results\"\n" +
            "        },\n" +
            "        \"type\": {\n" +
            "          \"type\" : \"list\",\n" +
            "          \"description\" : \"A comma-separated list of types to restrict the results\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"params\": {\n" +
            "        \"ignore_unavailable\": {\n" +
            "          \"type\" : \"boolean\",\n" +
            "          \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"\n" +
            "        } \n" +
            "      }\n" +
            "    },\n" +
            "    \"body\": {\n" +
            "      \"description\" : \"A query to restrict the results specified with the Query DSL (optional)\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";

    private static final String REST_SPEC_GET_TEMPLATE_API = "{\n" +
            "  \"indices.get_template\": {\n" +
            "    \"documentation\": \"http://www.elasticsearch.org/guide/reference/api/admin-indices-templates/\",\n" +
            "    \"methods\": [\"GET\"],\n" +
            "    \"url\": {\n" +
            "      \"path\": \"/_template/{name}\",\n" +
            "      \"paths\": [\"/_template\", \"/_template/{name}\"],\n" +
            "      \"parts\": {\n" +
            "        \"name\": {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"required\" : false,\n" +
            "          \"description\" : \"The name of the template\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"params\": {\n" +
            "      }\n" +
            "    },\n" +
            "    \"body\": null\n" +
            "  }\n" +
            "}";

    private static final String REST_SPEC_INDEX_API = "{\n" +
            "  \"index\": {\n" +
            "    \"documentation\": \"http://elasticsearch.org/guide/reference/api/index_/\",\n" +
            "    \"methods\": [\"POST\", \"PUT\"],\n" +
            "    \"url\": {\n" +
            "      \"path\": \"/{index}/{type}\",\n" +
            "      \"paths\": [\"/{index}/{type}\", \"/{index}/{type}/{id}\"],\n" +
            "      \"parts\": {\n" +
            "        \"id\": {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"description\" : \"Document ID\"\n" +
            "        },\n" +
            "        \"index\": {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"required\" : true,\n" +
            "          \"description\" : \"The name of the index\"\n" +
            "        },\n" +
            "        \"type\": {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"required\" : true,\n" +
            "          \"description\" : \"The type of the document\"\n" +
            "        }\n" +
            "      }   ,\n" +
            "      \"params\": {\n" +
            "        \"consistency\": {\n" +
            "          \"type\" : \"enum\",\n" +
            "          \"options\" : [\"one\", \"quorum\", \"all\"],\n" +
            "          \"description\" : \"Explicit write consistency setting for the operation\"\n" +
            "        },\n" +
            "        \"op_type\": {\n" +
            "          \"type\" : \"enum\",\n" +
            "          \"options\" : [\"index\", \"create\"],\n" +
            "          \"default\" : \"index\",\n" +
            "          \"description\" : \"Explicit operation type\"\n" +
            "        },\n" +
            "        \"parent\": {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"description\" : \"ID of the parent document\"\n" +
            "        },\n" +
            "        \"refresh\": {\n" +
            "          \"type\" : \"boolean\",\n" +
            "          \"description\" : \"Refresh the index after performing the operation\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"body\": {\n" +
            "      \"description\" : \"The document\",\n" +
            "      \"required\" : true\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
}
