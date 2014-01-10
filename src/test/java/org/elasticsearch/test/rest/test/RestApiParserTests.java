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
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class RestApiParserTests extends AbstractParserTests {

    @Test
    public void testParseRestSpecIndexApi() throws Exception {
        parser = JsonXContent.jsonXContent.createParser(REST_SPEC_INDEX_API);
        RestApi restApi = new RestApiParser().parse(parser);

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
    }

    @Test
    public void testParseRestSpecGetTemplateApi() throws Exception {
        parser = JsonXContent.jsonXContent.createParser(REST_SPEC_GET_TEMPLATE_API);
        RestApi restApi = new RestApiParser().parse(parser);
        assertThat(restApi, notNullValue());
        assertThat(restApi.getName(), equalTo("indices.get_template"));
        assertThat(restApi.getMethods().size(), equalTo(1));
        assertThat(restApi.getMethods().get(0), equalTo("GET"));
        assertThat(restApi.getPaths().size(), equalTo(2));
        assertThat(restApi.getPaths().get(0), equalTo("/_template"));
        assertThat(restApi.getPaths().get(1), equalTo("/_template/{name}"));
        assertThat(restApi.getPathParts().size(), equalTo(1));
        assertThat(restApi.getPathParts().get(0), equalTo("name"));
    }

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
            "        \"percolate\": {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"description\" : \"Percolator queries to execute while indexing the document\"\n" +
            "        },\n" +
            "        \"refresh\": {\n" +
            "          \"type\" : \"boolean\",\n" +
            "          \"description\" : \"Refresh the index after performing the operation\"\n" +
            "        },\n" +
            "        \"replication\": {\n" +
            "          \"type\" : \"enum\",\n" +
            "          \"options\" : [\"sync\",\"async\"],\n" +
            "          \"default\" : \"sync\",\n" +
            "          \"description\" : \"Specific replication type\"\n" +
            "        },\n" +
            "        \"routing\": {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"description\" : \"Specific routing value\"\n" +
            "        },\n" +
            "        \"timeout\": {\n" +
            "          \"type\" : \"time\",\n" +
            "          \"description\" : \"Explicit operation timeout\"\n" +
            "        },\n" +
            "        \"timestamp\": {\n" +
            "          \"type\" : \"time\",\n" +
            "          \"description\" : \"Explicit timestamp for the document\"\n" +
            "        },\n" +
            "        \"ttl\": {\n" +
            "          \"type\" : \"duration\",\n" +
            "          \"description\" : \"Expiration time for the document\"\n" +
            "        },\n" +
            "        \"version\" : {\n" +
            "          \"type\" : \"number\",\n" +
            "          \"description\" : \"Explicit version number for concurrency control\"\n" +
            "        },\n" +
            "        \"version_type\": {\n" +
            "          \"type\" : \"enum\",\n" +
            "          \"options\" : [\"internal\",\"external\"],\n" +
            "          \"description\" : \"Specific version type\"\n" +
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
