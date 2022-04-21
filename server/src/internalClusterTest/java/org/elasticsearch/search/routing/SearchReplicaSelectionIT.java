/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.routing;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numClientNodes = 1, numDataNodes = 3)
public class SearchReplicaSelectionIT extends ESIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), true)
            .build();
    }

    public void testNodeSelection() {
        // We grab a client directly to avoid using a randomizing client that might set a search preference.
        Client client = internalCluster().coordOnlyNodeClient();

        client.admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 2))
            .get();
        ensureGreen();

        client.prepareIndex("test").setSource("field", "value").get();
        refresh();

        // Before we've gathered stats for all nodes, we should try each node once.
        Set<String> nodeIds = new HashSet<>();
        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        nodeIds.add(searchResponse.getHits().getAt(0).getShard().getNodeId());

        searchResponse = client.prepareSearch().setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        nodeIds.add(searchResponse.getHits().getAt(0).getShard().getNodeId());

        searchResponse = client.prepareSearch().setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        nodeIds.add(searchResponse.getHits().getAt(0).getShard().getNodeId());

        assertEquals(3, nodeIds.size());

        // Now after more searches, we should select a node with the lowest ARS rank.
        for (int i = 0; i < 5; i++) {
            client.prepareSearch().setQuery(matchAllQuery()).get();
        }

        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().get();
        ImmutableOpenMap<String, DiscoveryNode> coordinatingNodes = clusterStateResponse.getState().nodes().getCoordinatingOnlyNodes();
        assertEquals(1, coordinatingNodes.size());

        String coordinatingNodeId = coordinatingNodes.values().iterator().next().getId();
        NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats().setAdaptiveSelection(true).get();
        NodeStats nodeStats = statsResponse.getNodesMap().get(coordinatingNodeId);
        assertNotNull(nodeStats);
        assertEquals(3, nodeStats.getAdaptiveSelectionStats().getComputedStats().size());

        searchResponse = client.prepareSearch().setQuery(matchAllQuery()).get();
        String selectedNodeId = searchResponse.getHits().getAt(0).getShard().getNodeId();
        double selectedRank = nodeStats.getAdaptiveSelectionStats().getRanks().get(selectedNodeId);

        for (Map.Entry<String, Double> entry : nodeStats.getAdaptiveSelectionStats().getRanks().entrySet()) {
            double rank = entry.getValue();
            assertThat(rank, greaterThanOrEqualTo(selectedRank));
        }
    }
}
