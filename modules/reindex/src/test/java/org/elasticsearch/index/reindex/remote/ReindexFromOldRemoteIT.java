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

package org.elasticsearch.index.reindex.remote;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class ReindexFromOldRemoteIT extends ESRestTestCase {
    /**
     * Number of documents to test when reindexing from an old version.
     */
    private static final int DOCS = 5;

    private void oldEsTestCase(String portPropertyName, String requestsPerSecond) throws IOException {
        boolean enabled = Booleans.parseBoolean(System.getProperty("tests.fromOld"));
        assumeTrue("test is disabled, probably because this is windows", enabled);

        int oldEsPort = Integer.parseInt(System.getProperty(portPropertyName));
        try (RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {
            try {
                Request createIndex = new Request("PUT", "/test");
                createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\": 1}}");
                oldEs.performRequest(createIndex);

                for (int i = 0; i < DOCS; i++) {
                    Request doc = new Request("PUT", "/test/doc/testdoc" + i);
                    doc.addParameter("refresh", "true");
                    doc.setJsonEntity("{\"test\":\"test\"}");
                    oldEs.performRequest(doc);
                }

                Request reindex = new Request("POST", "/_reindex");
                if (randomBoolean()) {
                    // Reindex using the external version_type
                    reindex.setJsonEntity(
                        "{\n"
                            + "  \"source\":{\n"
                            + "    \"index\": \"test\",\n"
                            + "    \"size\": 1,\n"
                            + "    \"remote\": {\n"
                            + "      \"host\": \"http://127.0.0.1:" + oldEsPort + "\"\n"
                            + "    }\n"
                            + "  },\n"
                            + "  \"dest\": {\n"
                            + "    \"index\": \"test\",\n"
                            + "    \"version_type\": \"external\"\n"
                            + "  }\n"
                            + "}");
                } else {
                    // Reindex using the default internal version_type
                    reindex.setJsonEntity(
                        "{\n"
                            + "  \"source\":{\n"
                            + "    \"index\": \"test\",\n"
                            + "    \"size\": 1,\n"
                            + "    \"remote\": {\n"
                            + "      \"host\": \"http://127.0.0.1:" + oldEsPort + "\"\n"
                            + "    }\n"
                            + "  },\n"
                            + "  \"dest\": {\n"
                            + "    \"index\": \"test\"\n"
                            + "  }\n"
                            + "}");
                }
                reindex.addParameter("refresh", "true");
                reindex.addParameter("pretty", "true");
                if (requestsPerSecond != null) {
                    reindex.addParameter("requests_per_second", requestsPerSecond);
                }
                client().performRequest(reindex);

                Request search = new Request("POST", "/test/_search");
                search.addParameter("pretty", "true");
                Response response = client().performRequest(search);
                String result = EntityUtils.toString(response.getEntity());
                for (int i = 0; i < DOCS; i++) {
                    assertThat(result, containsString("\"_id\" : \"testdoc" + i + "\""));
                }
            } finally {
                oldEs.performRequest(new Request("DELETE", "/test"));
            }
        }
    }

    public void testEs2() throws IOException {
        oldEsTestCase("es2.port", null);
    }

    public void testEs1() throws IOException {
        oldEsTestCase("es1.port", null);
    }

    public void testEs090() throws IOException {
        oldEsTestCase("es090.port", null);
    }

    public void testEs2WithFunnyThrottle() throws IOException {
        oldEsTestCase("es2.port", "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

    public void testEs1WithFunnyThrottle() throws IOException {
        oldEsTestCase("es1.port", "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

    public void testEs090WithFunnyThrottle() throws IOException {
        oldEsTestCase("es090.port", "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

}
