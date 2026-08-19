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
 * Reindex-from-remote against Elasticsearch 7.9.3 (remote scroll) and 7.10.0 (remote PIT when supported),
 * each run as a real cluster in a Testcontainers-managed Docker container (see
 * {@link OldElasticsearchContainer}). Docker images bundle the JDK the corresponding release ships,
 * so these run on every architecture the fixture publishes images for, including darwin-aarch64.
 */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class ReindexFromRemote7xIT extends AbstractReindexIT {

    private static final int DOCS = 10;

    @ClassRule
    public static OldElasticsearchContainer es79 = new OldElasticsearchContainer("7.9.3", repoLocation("7.9.3"));

    @ClassRule
    public static OldElasticsearchContainer es710 = new OldElasticsearchContainer("7.10.0", repoLocation("7.10.0"));

    /**
     * The old-ES fixture doesn't use its snapshot repository directory for these tests, but the
     * container's entrypoint always requires and manages one (see {@code ES_PATH_REPO} in
     * {@code entrypoint.sh}), so each version gets its own scratch subdirectory.
     */
    private static String repoLocation(String version) {
        return PathUtils.get(System.getProperty("java.io.tmpdir"), "reindex-old-es-repo", version).toString();
    }

    private void reindexFromRemote7x(OldElasticsearchContainer container, String remoteIndex, String destIndex) throws IOException {
        // Use the loopback address explicitly (not container.getHost(), which testcontainers may
        // resolve to "localhost") since only 127.0.0.1/[::1] are covered by reindex.remote.whitelist.
        String remoteHost = "127.0.0.1";
        int remotePort = container.getHttpPort();
        boolean success = false;
        try (RestClient remote = RestClient.builder(new HttpHost(remoteHost, remotePort)).build()) {
            try {
                Request createIndex = new Request("PUT", "/" + remoteIndex);
                createIndex.setJsonEntity("""
                    {
                      "settings": { "number_of_shards": 1 },
                      "mappings": {
                        "properties": {
                          "id": { "type": "keyword" }
                        }
                      }
                    }""");
                assertOK(remote.performRequest(createIndex));

                StringBuilder bulkBody = new StringBuilder();
                for (int i = 0; i < DOCS; i++) {
                    String id = "doc" + i;
                    bulkBody.append(
                        String.format(
                            java.util.Locale.ROOT,
                            "{\"index\":{\"_index\":\"%s\",\"_id\":\"%s\"}}\n{\"id\":\"%s\"}\n",
                            remoteIndex,
                            id,
                            id
                        )
                    );
                }
                Request bulk = new Request("POST", "/_bulk");
                bulk.addParameter("refresh", "true");
                bulk.setJsonEntity(bulkBody.toString());
                assertOK(remote.performRequest(bulk));

                Request reindex = new Request("POST", "/_reindex");
                reindex.addParameter("refresh", "true");
                reindex.addParameter("pretty", "true");
                reindex.setJsonEntity(String.format(java.util.Locale.ROOT, """
                    {
                      "source": {
                        "index": "%s",
                        "size": 2,
                        "remote": {
                          "host": "http://%s:%s"
                        }
                      },
                      "dest": {
                        "index": "%s"
                      }
                    }""", remoteIndex, remoteHost, remotePort, destIndex));
                assertOK(client().performRequest(reindex));

                Request search = new Request("POST", "/" + destIndex + "/_search");
                search.addParameter("pretty", "true");
                Response response = client().performRequest(search);
                String result = EntityUtils.toString(response.getEntity());
                assertThat(result, containsString("\"hits\" : {"));
                assertThat(result, containsString("\"total\""));
                for (int i = 0; i < DOCS; i++) {
                    String id = "doc" + i;
                    assertThat(result, containsString("\"_id\" : \"" + id + "\""));
                }
                success = true;
            } finally {
                try {
                    remote.performRequest(new Request("DELETE", "/" + remoteIndex));
                } catch (Exception deleteException) {
                    logger.warn("Exception deleting remote index", deleteException);
                    if (success) {
                        throw deleteException;
                    }
                }
            }
        }
    }

    /** Remote is 7.9.3, so the reindex client uses scroll search */
    public void testReindexFromRemote79() throws IOException {
        reindexFromRemote7x(es79, "reindex_remote_79_src", "reindex_remote_79_dest");
    }

    /** Remote is 7.10.0, so the reindex client uses PIT search */
    public void testReindexFromRemote710() throws IOException {
        reindexFromRemote7x(es710, "reindex_remote_710_src", "reindex_remote_710_dest");
    }
}
