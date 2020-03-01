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
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.util.Map;

public class JodaIT extends AbstractRollingTestCase {
    /**
     * Create a mapping that explicitly disables the _all field (possible in 6x, see #37429)
     * and check that it can be upgraded to 7x.
     */
    public void testAllFieldDisable6x() throws Exception {
//        assumeTrue("_all", UPGRADE_FROM_VERSION.before(Version.V_7_0_0));
        switch (CLUSTER_TYPE) {
            case OLD:
                Request createTestIndex = new Request("PUT", "joda_it");
                createTestIndex.addParameter("include_type_name", "true");
                createTestIndex.setJsonEntity("{\n" +
                    "  \"settings\": {\n" +
                    "    \"index.number_of_shards\": 1\n" +
                    "  },\n" +
                    "  \"mappings\": {\n" +
                    "    \"_doc\": {\n" +
                    "      \"properties\": {\n" +
                    "        \"datetime\": {\n" +
                    "          \"type\": \"date\",\n" +
                    "          \"format\": \"YYYY-MM-dd'T'HH:mm:SSZZ\"\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}"
                );
                Version minNodeVersion = getMinVersion();
                System.out.println(UPGRADE_FROM_VERSION);
                System.out.println(minNodeVersion);

                if (minNodeVersion.equals(Version.V_6_8_0)) {
                    createTestIndex.setOptions(
                        expectWarnings("Use of 'Y' (year-of-era) will change to 'y' in the next major version of Elasticsearch." +
                            " Prefix your date format with '8' to use the new specifier."));

                } else {
                    createTestIndex.setOptions(
                        expectWarnings("'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.; " +
                            "'Z' time zone offset/id fails when parsing 'Z' for Zulu timezone. Consider using 'X'." +
                            " Prefix your date format with '8' to use the new specifier."));
                }


                Response resp = client().performRequest(createTestIndex);
                assertEquals(200, resp.getStatusLine().getStatusCode());

                postNewDoc();

                break;
            case MIXED:
                postNewDoc();

                Request search = new Request("GET", "joda_it/_search");
                search.setJsonEntity("{\n" +
                    "  \"sort\": \"datetime\",\n" +
                    "  \"query\": {\n" +
                    "    \"range\": {\n" +
                    "      \"datetime\": {\n" +
                    "        \"gte\": \"2020-01-01T00:00:00+01:00\",\n" +
                    "        \"lte\": \"2020-01-02T00:00:00+01:00\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}\n"
                );


                search.setOptions(
                    expectWarnings("Use of 'Y' (year-of-era) will change to 'y' in the next major version of Elasticsearch. Prefix your date format with '8' to use the new specifier."));

                Response searchResp = client().performRequest(search);
                assertEquals(200, searchResp.getStatusLine().getStatusCode());
                break;
            case UPGRADED:
                postNewDoc();

                search = new Request("GET", "joda_it/_search");
                search.setJsonEntity("{\n" +
                    "  \"sort\": \"datetime\",\n" +
                    "  \"query\": {\n" +
                    "    \"range\": {\n" +
                    "      \"datetime\": {\n" +
                    "        \"gte\": \"2020-01-01T00:00:00+01:00\",\n" +
                    "        \"lte\": \"2020-01-02T00:00:00+01:00\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}\n"
                );


                search.setOptions(
                    expectWarnings("'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.; " +
                        "'Z' time zone offset/id fails when parsing 'Z' for Zulu timezone. Consider using 'X'. " +
                        "Use new java.time date format specifiers."));

                 searchResp = client().performRequest(search);
                assertEquals(200, searchResp.getStatusLine().getStatusCode());
                break;
            default:
//                final Request request = new Request("GET", "all-index");
//                Response response = client().performRequest(request);
//                assertEquals(200, response.getStatusLine().getStatusCode());
//                Object enabled = XContentMapValues.extractValue("all-index.mappings._all.enabled", entityAsMap(response));
//                assertNotNull(enabled);
//                assertEquals(false, enabled);
                break;
        }
    }

    private Version getMinVersion() throws IOException {
        Version minNodeVersion = null;
        Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "_nodes")));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
        for (Map.Entry<?, ?> node : nodes.entrySet()) {
            Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
            Version nodeVersion = Version.fromString(nodeInfo.get("version").toString());
            if (minNodeVersion == null) {
                minNodeVersion = nodeVersion;
            } else if (nodeVersion.before(minNodeVersion)) {
                minNodeVersion = nodeVersion;
            }
        }
        return minNodeVersion;
    }

    private void postNewDoc() throws IOException {
        Response resp;
        Request putDoc = new Request("POST", "joda_it/_doc");
        putDoc.setJsonEntity("{\n" +
            "  \"datetime\": \"2020-01-01T01:01:01+01:00\"\n" +
            "}"
        );
        resp = client().performRequest(putDoc);
        assertEquals(201, resp.getStatusLine().getStatusCode());
    }

}
