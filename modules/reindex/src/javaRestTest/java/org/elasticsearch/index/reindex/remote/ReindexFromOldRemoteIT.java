/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex.remote;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.Constants;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Booleans;
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
        boolean success = false;
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
                    reindex.setJsonEntity(String.format(java.util.Locale.ROOT, """
                        {
                          "source":{
                            "index": "test",
                            "size": 1,
                            "remote": {
                              "host": "http://127.0.0.1:%s"
                            }
                          },
                          "dest": {
                            "index": "test",
                            "version_type": "external"
                          }
                        }""", oldEsPort));
                } else {
                    // Reindex using the default internal version_type
                    reindex.setJsonEntity(String.format(java.util.Locale.ROOT, """
                        {
                          "source":{
                            "index": "test",
                            "size": 1,
                            "remote": {
                              "host": "http://127.0.0.1:%s"
                            }
                          },
                          "dest": {
                            "index": "test"
                          }
                        }""", oldEsPort));
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
                success = true;
            } finally {
                try {
                    oldEs.performRequest(new Request("DELETE", "/test"));
                } catch (Exception deleteException) {
                    logger.warn("Exception deleting index", deleteException);
                    if (success) {
                        // When the test succeeds the delete should not fail. So if it unexpectandly fails
                        // here, we propogate it.
                        throw deleteException;
                    }
                }
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
        assumeFalse("No longer works on Mac", Constants.MAC_OS_X);
        oldEsTestCase("es090.port", null);
    }

    public void testEs2WithFunnyThrottle() throws IOException {
        oldEsTestCase("es2.port", "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

    public void testEs1WithFunnyThrottle() throws IOException {
        oldEsTestCase("es1.port", "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

    public void testEs090WithFunnyThrottle() throws IOException {
        assumeFalse("No longer works on Mac", Constants.MAC_OS_X);
        oldEsTestCase("es090.port", "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

}
