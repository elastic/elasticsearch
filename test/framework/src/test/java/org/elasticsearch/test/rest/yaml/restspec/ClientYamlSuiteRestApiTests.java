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
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ClientYamlSuiteRestApiTests extends ESTestCase {

    public void testPathMatching() throws IOException {
        XContentParser parser = createParser(YamlXContent.yamlXContent, REST_SPEC_API);
        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApiParser().parse("index.json", parser);
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Collections.emptySet());
            assertEquals(1, paths.size());
            assertEquals("/_doc", paths.get(0).getPath());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Collections.singleton("wait_for_active_shards"));
            assertEquals(1, paths.size());
            assertEquals("/_doc", paths.get(0).getPath());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Collections.singleton("index"));
            assertEquals(1, paths.size());
            assertEquals("/{index}/_doc", paths.get(0).getPath());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Set.of("index", "id"));
            assertEquals(1, paths.size());
            assertEquals("/{index}/_doc/{id}", paths.get(0).getPath());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Set.of("index", "type"));
            assertEquals(3, paths.size());
            assertEquals("/{index}/_mapping/{type}", paths.get(0).getPath());
            assertEquals("/{index}/{type}", paths.get(1).getPath());
            assertEquals("/{index}/_mappings/{type}", paths.get(2).getPath());
        }
        {
            List<ClientYamlSuiteRestApi.Path> paths = restApi.getBestMatchingPaths(Set.of("index", "type", "id"));
            assertEquals(1, paths.size());
            assertEquals("/{index}/{type}/{id}", paths.get(0).getPath());
        }
    }

    private static final String REST_SPEC_API = "{\n" +
        "  \"index\":{\n" +
        "    \"documentation\":{\n" +
        "      \"url\":\"http://www.elastic.co/guide/en/elasticsearch/reference/master/docs-index_.html\",\n" +
        "      \"description\":\"Creates or updates a document in an index.\"\n" +
        "    },\n" +
        "    \"stability\":\"stable\",\n" +
        "    \"url\":{\n" +
        "      \"paths\":[\n" +
        "        {\n" +
        "          \"path\":\"/_doc\",\n" +
        "          \"methods\":[\n" +
        "            \"PUT\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"path\":\"/{index}/_mapping/{type}\",\n" +
        "          \"methods\":[\n" +
        "            \"PUT\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"index\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"required\":true,\n" +
        "              \"description\":\"The name of the index\"\n" +
        "            },\n" +
        "            \"type\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"description\":\"The type of the document\"\n" +
        "            }\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"path\":\"/{index}/_mappings/{type}\",\n" +
        "          \"methods\":[\n" +
        "            \"PUT\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"index\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"required\":true,\n" +
        "              \"description\":\"The name of the index\"\n" +
        "            },\n" +
        "            \"type\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"description\":\"The type of the document\"\n" +
        "            }\n" +
        "          }\n" +
        "        },\n" +

        "        {\n" +
        "          \"path\":\"/{index}/_doc/{id}\",\n" +
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
        "              \"required\":true,\n" +
        "              \"description\":\"The name of the index\"\n" +
        "            }\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"path\":\"/{index}/_doc\",\n" +
        "          \"methods\":[\n" +
        "            \"POST\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"index\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"required\":true,\n" +
        "              \"description\":\"The name of the index\"\n" +
        "            }\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"path\":\"/{index}/{type}\",\n" +
        "          \"methods\":[\n" +
        "            \"POST\"\n" +
        "          ],\n" +
        "          \"parts\":{\n" +
        "            \"index\":{\n" +
        "              \"type\":\"string\",\n" +
        "              \"required\":true,\n" +
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
        "              \"required\":true,\n" +
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
        "        \"description\":\"Sets the number of shard copies that must be active before proceeding with the index operation. " +
        "Defaults to 1, meaning the primary shard only. Set to `all` for all shard copies, otherwise set to any non-negative value less " +
        "than or equal to the total number of copies for the shard (number of replicas + 1)\"\n" +
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
        "        \"description\":\"If `true` then refresh the affected shards to make this operation visible to search, if `wait_for` " +
        "then wait for a refresh to make this operation visible to search, if `false` (the default) then do nothing with refreshes.\"\n" +
        "      },\n" +
        "      \"routing\":{\n" +
        "        \"type\":\"string\",\n" +
        "        \"description\":\"Specific routing value\"\n" +
        "      },\n" +
        "      \"timeout\":{\n" +
        "        \"type\":\"time\",\n" +
        "        \"description\":\"Explicit operation timeout\"\n" +
        "      },\n" +
        "      \"version\":{\n" +
        "        \"type\":\"number\",\n" +
        "        \"description\":\"Explicit version number for concurrency control\"\n" +
        "      },\n" +
        "      \"version_type\":{\n" +
        "        \"type\":\"enum\",\n" +
        "        \"options\":[\n" +
        "          \"internal\",\n" +
        "          \"external\",\n" +
        "          \"external_gte\",\n" +
        "          \"force\"\n" +
        "        ],\n" +
        "        \"description\":\"Specific version type\"\n" +
        "      },\n" +
        "      \"if_seq_no\":{\n" +
        "        \"type\":\"number\",\n" +
        "        \"description\":\"only perform the index operation if the last operation that has changed the document has the " +
        "specified sequence number\"\n" +
        "      },\n" +
        "      \"if_primary_term\":{\n" +
        "        \"type\":\"number\",\n" +
        "        \"description\":\"only perform the index operation if the last operation that has changed the document has the " +
        "specified primary term\"\n" +
        "      },\n" +
        "      \"pipeline\":{\n" +
        "        \"type\":\"string\",\n" +
        "        \"description\":\"The pipeline id to preprocess incoming documents with\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"body\":{\n" +
        "      \"description\":\"The document\",\n" +
        "      \"required\":true\n" +
        "    }\n" +
        "  }\n" +
        "}\n";
}
