/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.remote;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * Reindex-from-remote against Elasticsearch 7.9.x (remote scroll) and 7.10.0 (remote PIT when supported).
 */
public class ReindexFromRemote7xIT extends ESRestTestCase {

    private static final int DOCS = 10;

    private void reindexFromRemote7x(String portProperty, String remoteIndex, String destIndex) throws IOException {
        boolean enabled = Booleans.parseBoolean(System.getProperty("tests.fromRemote7x"));
        assumeTrue("test is disabled (windows, path with spaces, or fixtures not wired)", enabled);

        int remotePort = Integer.parseInt(System.getProperty(portProperty));
        boolean success = false;
        try (RestClient remote = RestClient.builder(new HttpHost("127.0.0.1", remotePort)).build()) {
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
                          "host": "http://127.0.0.1:%s"
                        }
                      },
                      "dest": {
                        "index": "%s"
                      }
                    }""", remoteIndex, remotePort, destIndex));
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

    /** Remote is 7.9.0, so the reindex client uses scroll search */
    public void testReindexFromRemote79() throws IOException {
        assumeTrue(
            "remote 7.9 fixture not run on this platform (e.g. darwin-aarch64 has no 7.9 archive)",
            Booleans.parseBoolean(System.getProperty("tests.fromRemote7xEs79", "true"))
        );
        reindexFromRemote7x("es79.port", "reindex_remote_79_src", "reindex_remote_79_dest");
    }

    /** Remote is 7.10.0, so the reindex client uses PIT search */
    public void testReindexFromRemote710() throws IOException {
        assumeTrue(
            "remote 7.10.0 fixture not run on this platform (e.g. darwin-aarch64 has no 7.10 archive)",
            Booleans.parseBoolean(System.getProperty("tests.fromRemote7xEs710", "true"))
        );
        reindexFromRemote7x("es710.port", "reindex_remote_710_src", "reindex_remote_710_dest");
    }
}
