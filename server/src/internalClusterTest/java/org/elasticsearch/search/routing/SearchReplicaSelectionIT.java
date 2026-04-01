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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(numClientNodes = 1, numDataNodes = 3)
public class SearchReplicaSelectionIT extends ESIntegTestCase {

    private static void setTransientSettings(Settings settings) {
        assertAcked(clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setTransientSettings(settings).get());
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), true)
            .build();
    }

    public void testNodeSelection() throws Exception {
        Client client = internalCluster().coordOnlyNodeClient();

        client.admin().indices().prepareCreate("test").setSettings(indexSettings(1, 2)).get();
        ensureGreen();

        client.prepareIndex("test").setSource("field", "value").get();
        refresh();

        // Set exploration probability to 1.0 so every search explores a warming node.
        // With the default warmup threshold (30 responses), nodes stay as exploration candidates
        // for many searches, giving the random selection enough tries to hit all 3.
        setTransientSettings(Settings.builder().put(OperationRouting.ARS_EXPLORATION_PROBABILITY.getKey(), 1.0).build());

        try {
            Set<String> nodeIds = new HashSet<>();
            for (int i = 0; i < 50; i++) {
                assertResponse(client.prepareSearch().setQuery(matchAllQuery()), response -> {
                    nodeIds.add(response.getHits().getAt(0).getShard().getNodeId());
                });
                if (nodeIds.size() == 3) break;
            }
            assertEquals("all 3 nodes should have been explored", 3, nodeIds.size());
        } finally {
            setTransientSettings(Settings.builder().putNull(OperationRouting.ARS_EXPLORATION_PROBABILITY.getKey()).build());
        }

        // After enough searches, all replicas have computed stats and the chosen replica
        // should match the lowest ARS rank from the formula.
        for (int i = 0; i < 50; i++) {
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
     * Verifies that when a new node joins a cluster that already has ARS stats, probabilistic
     * exploration gives the new node some traffic without flooding it.
     */
    public void testWarmingNodeExploredButNotFlooded() {
        Client client = internalCluster().coordOnlyNodeClient();

        // 10 shards, 2 replicas across the initial 3 data nodes
        client.admin().indices().prepareCreate("probe_test").setSettings(indexSettings(10, 2)).get();
        ensureGreen("probe_test");

        for (int i = 0; i < 100; i++) {
            client.prepareIndex("probe_test").setSource("field", "value" + i).get();
        }
        refresh("probe_test");

        // Build ARS stats on the existing 3 nodes
        for (int i = 0; i < 50; i++) {
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

        // Send several searches and count how many distinct shards are routed to the new node.
        final String targetNodeId = newNodeId;
        int totalShardDecisions = 0;
        int newNodeShardDecisions = 0;
        for (int i = 0; i < 10; i++) {
            var response = client.prepareSearch("probe_test").setQuery(matchAllQuery()).setSize(100).get();
            try {
                Set<String> countedShards = new HashSet<>();
                for (var hit : response.getHits().getHits()) {
                    if (hit.getShard() != null) {
                        String shardKey = hit.getShard().getShardId() + ":" + hit.getShard().getNodeId();
                        if (countedShards.add(shardKey)) {
                            totalShardDecisions++;
                            if (targetNodeId.equals(hit.getShard().getNodeId())) {
                                newNodeShardDecisions++;
                            }
                        }
                    }
                }
            } finally {
                response.decRef();
            }
        }

        assertThat("warming node should receive some traffic from exploration", newNodeShardDecisions, greaterThanOrEqualTo(1));
        assertThat("warming node should not dominate shard routing", newNodeShardDecisions, lessThan(totalShardDecisions / 2));
    }
}
