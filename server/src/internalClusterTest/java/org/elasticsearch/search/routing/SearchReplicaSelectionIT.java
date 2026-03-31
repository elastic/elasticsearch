/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.routing;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponses;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numClientNodes = 1, numDataNodes = 3)
public class SearchReplicaSelectionIT extends ESIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), true)
            .build();
    }

    public void testNodeSelection() throws Exception {
        // We grab a client directly to avoid using a randomizing client that might set a search preference.
        Client client = internalCluster().coordOnlyNodeClient();

        client.admin().indices().prepareCreate("test").setSettings(indexSettings(1, 2)).get();
        ensureGreen();

        client.prepareIndex("test").setSource("field", "value").get();
        refresh();

        // Before we've gathered stats for all nodes, we should try each node once.
        Set<String> nodeIds = new HashSet<>();
        assertResponses(response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            nodeIds.add(response.getHits().getAt(0).getShard().getNodeId());
        },
            client.prepareSearch().setQuery(matchAllQuery()),
            client.prepareSearch().setQuery(matchAllQuery()),
            client.prepareSearch().setQuery(matchAllQuery())
        );
        assertEquals(3, nodeIds.size());

        // After more searches, all replicas have computed stats; rankNodes no longer synthesizes ranks for
        // unknown replicas, so the chosen replica should match the lowest ARS rank from the formula.
        for (int i = 0; i < 5; i++) {
            client.prepareSearch().setQuery(matchAllQuery()).get().decRef();
        }

        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
        Map<String, DiscoveryNode> coordinatingNodes = clusterStateResponse.getState().nodes().getCoordinatingOnlyNodes();
        assertEquals(1, coordinatingNodes.size());

        String coordinatingNodeId = coordinatingNodes.values().iterator().next().getId();
        NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats().setAdaptiveSelection(true).get();
        NodeStats nodeStats = statsResponse.getNodesMap().get(coordinatingNodeId);
        assertNotNull(nodeStats);
        assertEquals(3, nodeStats.getAdaptiveSelectionStats().getComputedStats().size());

        assertBusy(() -> {
            NodesStatsResponse freshStatsResponse = client.admin().cluster().prepareNodesStats().setAdaptiveSelection(true).get();
            NodeStats freshNodeStats = freshStatsResponse.getNodesMap().get(coordinatingNodeId);
            assertNotNull(freshNodeStats);
            Map<String, Double> ranks = freshNodeStats.getAdaptiveSelectionStats().getRanks();
            assertResponse(client.prepareSearch().setQuery(matchAllQuery()), response -> {
                String selectedNodeId = response.getHits().getAt(0).getShard().getNodeId();
                Double selectedRank = ranks.get(selectedNodeId);
                assertNotNull(selectedNodeId + " should have a formula rank in adaptive selection stats", selectedRank);
                for (double rank : ranks.values()) {
                    assertThat(rank, greaterThanOrEqualTo(selectedRank));
                }
            });
        });
    }

    /**
     * Verifies that when a new node joins a cluster that already has ARS stats, the probe cap
     * bounds how many shards the new node wins within a single search. Uses a multi-shard index
     * so that rankShardsAndUpdateStats is called once per shard and the local nodeSearchCounts
     * increment triggers the cap.
     */
    public void testNewNodeProbedButNotFlooded() {
        Client client = internalCluster().coordOnlyNodeClient();

        // 10 shards, 2 replicas across the initial 3 data nodes
        client.admin().indices().prepareCreate("probe_test").setSettings(indexSettings(10, 2)).get();
        ensureGreen("probe_test");

        // Index documents so that most shards have at least one hit. Not all shards are guaranteed
        // to have a document, but that only undercounts the new node's shards — making the
        // assertion more lenient, not flaky.
        for (int i = 0; i < 100; i++) {
            client.prepareIndex("probe_test").setSource("field", "value" + i).get();
        }
        refresh("probe_test");

        // Build ARS stats on the existing 3 nodes
        for (int i = 0; i < 30; i++) {
            client.prepareSearch("probe_test").setQuery(matchAllQuery()).get().decRef();
        }

        // Add a 4th data node and increase replicas so the new node gets shard copies
        String newNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), true).build()
        );
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 3), "probe_test");
        ensureGreen("probe_test");

        // Find the new node's ID
        ClusterStateResponse clusterState = client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
        String newNodeId = null;
        for (Map.Entry<String, DiscoveryNode> entry : clusterState.getState().nodes().getDataNodes().entrySet()) {
            if (entry.getValue().getName().equals(newNode)) {
                newNodeId = entry.getKey();
                break;
            }
        }
        assertNotNull("new node should be in cluster state", newNodeId);

        // Send a search requesting all hits and count how many distinct shards were routed to
        // the new node. With 10 shards and a probe cap of 8, the new node should win at most
        // 8 shards within a single search before the local nodeSearchCounts increment triggers
        // the cap.
        final String targetNodeId = newNodeId;
        assertResponse(client.prepareSearch("probe_test").setQuery(matchAllQuery()).setSize(100), response -> {
            long newNodeShards = java.util.Arrays.stream(response.getHits().getHits())
                .filter(hit -> targetNodeId.equals(hit.getShard().getNodeId()))
                .map(hit -> hit.getShard().getShardId())
                .distinct()
                .count();
            // PROBE_INFLIGHT_CAP is 8 when the ars_probing feature flag is enabled
            assertThat("new node should not win more shards than the probe cap", newNodeShards, lessThanOrEqualTo(8L));
        });
    }
}
