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

package org.elasticsearch.smoketest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

public class ReindexFromOldRemoteIT extends ESRestTestCase {
    private void oldEsTestCase(String portPropertyName, String requestsPerSecond) throws IOException {
        int oldEsPort = Integer.parseInt(System.getProperty(portPropertyName));
        try (RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {
            try {
                HttpEntity entity = new StringEntity("{\"settings\":{\"number_of_shards\": 1}}", ContentType.APPLICATION_JSON);
                oldEs.performRequest("PUT", "/test", singletonMap("refresh", "true"), entity);

                entity = new StringEntity("{\"test\":\"test\"}", ContentType.APPLICATION_JSON);
                oldEs.performRequest("PUT", "/test/doc/testdoc1", singletonMap("refresh", "true"), entity);
                oldEs.performRequest("PUT", "/test/doc/testdoc2", singletonMap("refresh", "true"), entity);
                oldEs.performRequest("PUT", "/test/doc/testdoc3", singletonMap("refresh", "true"), entity);
                oldEs.performRequest("PUT", "/test/doc/testdoc4", singletonMap("refresh", "true"), entity);
                oldEs.performRequest("PUT", "/test/doc/testdoc5", singletonMap("refresh", "true"), entity);

                entity = new StringEntity(
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
                      + "}",
                      ContentType.APPLICATION_JSON);
                Map<String, String> params = new TreeMap<>();
                params.put("refresh", "true");
                params.put("pretty", "true");
                if (requestsPerSecond != null) {
                    params.put("requests_per_second", requestsPerSecond);
                }
                client().performRequest("POST", "/_reindex", params, entity);

                Response response = client().performRequest("POST", "test/_search", singletonMap("pretty", "true"));
                String result = EntityUtils.toString(response.getEntity());
                assertThat(result, containsString("\"_id\" : \"testdoc1\""));
            } finally {
                oldEs.performRequest("DELETE", "/test");
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
