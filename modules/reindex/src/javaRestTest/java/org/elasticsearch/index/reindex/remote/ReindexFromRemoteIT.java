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
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

/**
 * Reindex-from-remote against external Elasticsearch clusters (see {@code modules/reindex/build.gradle}).
 */
public class ReindexFromRemoteIT extends ESRestTestCase {

    private static final int DOCS = 10;

    /**
     * Index and bulk layout: typed {@code doc} on 5.x, {@code _doc} on 6.x, typeless on 7+.
     */
    private enum RemoteIndexLayout {
        ES_7_PLUS,
        ES_6_TYPED_DOC,
        ES_5_TYPED_DOC
    }

    private void reindexFromRemoteCluster(String portProperty, String remoteIndex, String destIndex, RemoteIndexLayout layout)
        throws IOException {
        boolean enabled = Booleans.parseBoolean(System.getProperty("tests.fromRemote7x"));
        assumeTrue("test is disabled (windows, path with spaces, or fixtures not wired)", enabled);

        int remotePort = Integer.parseInt(System.getProperty(portProperty));
        boolean success = false;
        try (RestClient remote = RestClient.builder(new HttpHost("127.0.0.1", remotePort)).build()) {
            try {
                String createIndexJson = switch (layout) {
                    case ES_7_PLUS -> """
                        {
                          "settings": { "number_of_shards": 1 },
                          "mappings": {
                            "properties": {
                              "id": { "type": "keyword" }
                            }
                          }
                        }""";
                    case ES_6_TYPED_DOC -> """
                        {
                          "settings": { "number_of_shards": 1 },
                          "mappings": {
                            "_doc": {
                              "properties": {
                                "id": { "type": "keyword" }
                              }
                            }
                          }
                        }""";
                    case ES_5_TYPED_DOC -> """
                        {
                          "settings": { "number_of_shards": 1 },
                          "mappings": {
                            "doc": {
                              "properties": {
                                "id": { "type": "keyword" }
                              }
                            }
                          }
                        }""";
                };
                Request createIndex = new Request("PUT", "/" + remoteIndex);
                createIndex.setJsonEntity(createIndexJson);
                assertOK(remote.performRequest(createIndex));

                String bulkTypeField = switch (layout) {
                    case ES_7_PLUS -> "";
                    case ES_6_TYPED_DOC -> ",\"_type\":\"_doc\"";
                    case ES_5_TYPED_DOC -> ",\"_type\":\"doc\"";
                };
                StringBuilder bulkBody = new StringBuilder();
                for (int i = 0; i < DOCS; i++) {
                    String id = "doc" + i;
                    bulkBody.append(
                        String.format(
                            Locale.ROOT,
                            "{\"index\":{\"_index\":\"%s\"%s,\"_id\":\"%s\"}}\n{\"id\":\"%s\"}\n",
                            remoteIndex,
                            bulkTypeField,
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
                reindex.setJsonEntity(String.format(Locale.ROOT, """
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

    /**
     * Remote is 5.0.x, and we expect scroll search to be used
     */
    public void testReindexFromRemote500() throws IOException {
        assumeTrue(
            "remote 5.0 fixture not run (e.g. windows or path with spaces)",
            Booleans.parseBoolean(System.getProperty("tests.fromRemoteEs500", "true"))
        );
        reindexFromRemoteCluster("es500.port", "reindex_remote_500_src", "reindex_remote_500_dest", RemoteIndexLayout.ES_5_TYPED_DOC);
    }

    /**
     * Remote is 6.2.x.
     * We expect scroll search to be used, and {@code allow_partial_search_results} to be omitted since this was added in version 6.3
     */
    public void testReindexFromRemote62() throws IOException {
        assumeTrue(
            "remote 6.2 fixture not run (e.g. windows or path with spaces)",
            Booleans.parseBoolean(System.getProperty("tests.fromRemoteEs62", "true"))
        );
        reindexFromRemoteCluster("es62.port", "reindex_remote_62_src", "reindex_remote_62_dest", RemoteIndexLayout.ES_6_TYPED_DOC);
    }

    /**
     * Remote is 6.3.x.
     * We expect scroll search to be used, and {@code allow_partial_search_results} to be included since this was added in version 6.3
     */
    public void testReindexFromRemote63() throws IOException {
        assumeTrue(
            "remote 6.3 fixture not run (e.g. windows or path with spaces)",
            Booleans.parseBoolean(System.getProperty("tests.fromRemoteEs63", "true"))
        );
        reindexFromRemoteCluster("es63.port", "reindex_remote_63_src", "reindex_remote_63_dest", RemoteIndexLayout.ES_6_TYPED_DOC);
    }

    /**
     * Remote is 7.9.0.
     * We expect scroll search to be used, since the remote version does not support point-in-time search yet
     */
    public void testReindexFromRemote79() throws IOException {
        assumeTrue(
            "remote 7.9 fixture not run on this platform (e.g. darwin-aarch64 has no 7.9 archive)",
            Booleans.parseBoolean(System.getProperty("tests.fromRemote7xEs79", "true"))
        );
        reindexFromRemoteCluster("es79.port", "reindex_remote_79_src", "reindex_remote_79_dest", RemoteIndexLayout.ES_7_PLUS);
    }

    /**
     * Remote is 7.10.0.
     * We expect point-in-time search to be used since the remote cluster supports it
     */
    public void testReindexFromRemote710() throws IOException {
        assumeTrue(
            "remote 7.10.0 fixture not run on this platform (e.g. darwin-aarch64 has no 7.10 archive)",
            Booleans.parseBoolean(System.getProperty("tests.fromRemote7xEs710", "true"))
        );
        reindexFromRemoteCluster("es710.port", "reindex_remote_710_src", "reindex_remote_710_dest", RemoteIndexLayout.ES_7_PLUS);
    }

    /**
     * Remote is 8.12.x.
     * We expect point-in-time search to be used.
     * The open PIT request should now include {@code index_filter} when applicable, since it was introduced in version 8.12
     */
    public void testReindexFromRemote812() throws IOException {
        assumeTrue(
            "remote 8.12 fixture not run",
            Booleans.parseBoolean(System.getProperty("tests.fromRemoteEs812", "true"))
        );
        reindexFromRemoteCluster("es812.port", "reindex_remote_812_src", "reindex_remote_812_dest", RemoteIndexLayout.ES_7_PLUS);
    }

    /**
     * Remote is 8.16.x.
     * We expect point-in-time search to be used.
     * The open PIT request should now include the {@code allow_partial_search_results} query parameter,
     * since it was introduced in version 8.16
     */
    public void testReindexFromRemote816() throws IOException {
        assumeTrue(
            "remote 8.16 fixture not run",
            Booleans.parseBoolean(System.getProperty("tests.fromRemoteEs816", "true"))
        );
        reindexFromRemoteCluster("es816.port", "reindex_remote_816_src", "reindex_remote_816_dest", RemoteIndexLayout.ES_7_PLUS);
    }

    /**
     * Remote is 9.3.x.
     * We expect point-in-time search to be used.
     * This is a defensive test to ensure we correctly gate on the {@code project_routing} query parameter
     */
    public void testReindexFromRemote93() throws IOException {
        assumeTrue(
            "remote 9.3 fixture not run",
            Booleans.parseBoolean(System.getProperty("tests.fromRemoteEs93", "true"))
        );
        reindexFromRemoteCluster("es93.port", "reindex_remote_93_src", "reindex_remote_93_dest", RemoteIndexLayout.ES_7_PLUS);
    }
}
