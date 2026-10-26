/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.backwards;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

/**
 * Documents indexed before a refresh returns must be visible on every searchable copy, including a replica that is still
 * recovering from an old-version primary onto a new-version node while the documents are indexed and refreshed.
 * <p>
 * YAML suites hit this window in mixed clusters by creating an index and immediately indexing, refreshing and searching it,
 * and occasionally see fewer hits than they indexed (https://github.com/elastic/elasticsearch/issues/151793,
 * https://github.com/elastic/elasticsearch/issues/161650). Those suites never look at the refresh's {@code _shards} or at
 * individual copies, so this test does both, many times, with the primary pinned to an old node.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RefreshVisibilityIT extends ESRestTestCase {
    private static final String BWC_NODES_VERSION = System.getProperty("tests.bwc_nodes_version");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.CLUSTER;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testRefreshWhileReplicaRecoversFromOldPrimary() throws Exception {
        MixedClusterTestNodes nodes = MixedClusterTestNodes.buildNodes(client(), BWC_NODES_VERSION);
        assumeFalse("no new node found", nodes.getNewNodes().isEmpty());
        assumeFalse("no bwc node found", nodes.getBWCNodes().isEmpty());
        try (
            RestClient oldNodeClient = buildClient(
                restClientSettings(),
                nodes.getBWCNodes().stream().map(MixedClusterTestNode::publishAddress).toArray(HttpHost[]::new)
            )
        ) {
            int iterations = between(30, 50);
            for (int i = 0; i < iterations; i++) {
                MixedClusterTestNode oldNode = randomFrom(nodes.getBWCNodes());
                MixedClusterTestNode newNode = randomFrom(nodes.getNewNodes());
                String index = "refresh_visibility_" + i;
                createIndex(index, indexSettings(1, 1).put("index.routing.allocation.require._name", oldNode.nodeName()).build());
                updateIndexSettings(
                    index,
                    Settings.builder()
                        .putNull("index.routing.allocation.require._name")
                        .put("index.routing.allocation.include._name", oldNode.nodeName() + "," + newNode.nodeName())
                );

                int numDocs = between(1, 5);
                for (int id = 0; id < numDocs; id++) {
                    Request indexRequest = new Request("PUT", index + "/_doc/" + id);
                    indexRequest.setJsonEntity("{\"f\": " + id + "}");
                    assertOK(oldNodeClient.performRequest(indexRequest));
                }
                Map<String, Object> refreshShards = ObjectPath.createFromResponse(
                    oldNodeClient.performRequest(new Request("POST", index + "/_refresh"))
                ).evaluate("_shards");
                logger.info("[{}] refresh of [{}] docs returned _shards {}", index, numDocs, refreshShards);
                if ((Integer) refreshShards.get("failed") != 0) {
                    fail("refresh of [" + index + "] failed on some copies: " + refreshShards + diagnostics(index));
                }

                for (int attempt = 0; attempt < 4; attempt++) {
                    assertCount(oldNodeClient, index, randomAlphaOfLength(8), numDocs, refreshShards);
                }
                ensureGreen(index);
                for (MixedClusterTestNode node : new MixedClusterTestNode[] { oldNode, newNode }) {
                    assertCount(oldNodeClient, index, "_only_nodes:" + node.nodeName(), numDocs, refreshShards);
                }
                deleteIndex(index);
            }
        }
    }

    private void assertCount(RestClient client, String index, String preference, int expected, Map<String, Object> refreshShards)
        throws IOException {
        Request request = new Request("GET", index + "/_count");
        request.addParameter("preference", preference);
        int count = ObjectPath.createFromResponse(client.performRequest(request)).evaluate("count");
        if (count != expected) {
            fail(
                "["
                    + index
                    + "] preference ["
                    + preference
                    + "] counted ["
                    + count
                    + "] docs but ["
                    + expected
                    + "] were indexed before a refresh that returned _shards "
                    + refreshShards
                    + diagnostics(index)
            );
        }
    }

    private static String diagnostics(String index) throws IOException {
        return "\nshards:\n"
            + EntityUtils.toString(client().performRequest(new Request("GET", "_cat/shards/" + index + "?v")).getEntity())
            + "recoveries:\n"
            + EntityUtils.toString(client().performRequest(new Request("GET", "_cat/recovery/" + index + "?v&detailed")).getEntity());
    }
}
