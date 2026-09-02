/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.remote;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.reindex.AbstractReindexIT;
import org.elasticsearch.test.fixtures.oldelasticsearch.OldElasticsearchContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * Reindex-from-remote against real Elasticsearch 0.90.13, 1.7.6, and 2.4.5 clusters running in
 * Testcontainers-managed Docker containers (see {@link OldElasticsearchContainer}). These are the
 * oldest three majors that reindex-from-remote is expected to support; we don't randomize versions
 * within a major since testing the last release of each major is representative enough.
 */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class ReindexFromOldRemoteIT extends AbstractReindexIT {
    /**
     * Number of documents to test when reindexing from an old version.
     */
    private static final int DOCS = 5;

    @ClassRule
    public static OldElasticsearchContainer es090 = new OldElasticsearchContainer("0.90.13", repoLocation("0.90.13"));

    @ClassRule
    public static OldElasticsearchContainer es17 = new OldElasticsearchContainer("1.7.6", repoLocation("1.7.6"));

    @ClassRule
    public static OldElasticsearchContainer es24 = new OldElasticsearchContainer("2.4.5", repoLocation("2.4.5"));

    /**
     * The old-ES fixture doesn't use its snapshot repository directory for these tests, but the
     * container's entrypoint always requires and manages one (see {@code ES_PATH_REPO} in
     * {@code entrypoint.sh}), so each version gets its own scratch subdirectory.
     */
    private static String repoLocation(String version) {
        return PathUtils.get(System.getProperty("java.io.tmpdir"), "reindex-old-es-repo", version).toString();
    }

    private void oldEsTestCase(OldElasticsearchContainer container, String requestsPerSecond) throws IOException {
        // Use the loopback address explicitly (not container.getHost(), which testcontainers may
        // resolve to "localhost") since only 127.0.0.1/[::1] are covered by reindex.remote.whitelist.
        String oldEsHost = "127.0.0.1";
        int oldEsPort = container.getHttpPort();
        boolean success = false;
        try (RestClient oldEs = RestClient.builder(new HttpHost(oldEsHost, oldEsPort)).build()) {
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
                              "host": "http://%s:%s"
                            }
                          },
                          "dest": {
                            "index": "test",
                            "version_type": "external"
                          }
                        }""", oldEsHost, oldEsPort));
                } else {
                    // Reindex using the default internal version_type
                    reindex.setJsonEntity(String.format(java.util.Locale.ROOT, """
                        {
                          "source":{
                            "index": "test",
                            "size": 1,
                            "remote": {
                              "host": "http://%s:%s"
                            }
                          },
                          "dest": {
                            "index": "test"
                          }
                        }""", oldEsHost, oldEsPort));
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
        oldEsTestCase(es24, null);
    }

    public void testEs1() throws IOException {
        oldEsTestCase(es17, null);
    }

    public void testEs090() throws IOException {
        oldEsTestCase(es090, null);
    }

    public void testEs2WithFunnyThrottle() throws IOException {
        oldEsTestCase(es24, "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

    public void testEs1WithFunnyThrottle() throws IOException {
        oldEsTestCase(es17, "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

    public void testEs090WithFunnyThrottle() throws IOException {
        oldEsTestCase(es090, "11"); // 11 requests per second should give us a nice "funny" number on the scroll timeout
    }

}
