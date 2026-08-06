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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numClientNodes = 1, numDataNodes = 3)
public class SearchReplicaSelectionIT extends ESIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), true)
            .put(OperationRouting.ADAPTIVE_REPLICA_SELECTION_PROBE_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testNodeSelection() throws Exception {
        // We grab a client directly to avoid using a randomizing client that might set a search preference.
        Client client = internalCluster().coordOnlyNodeClient();

        client.admin().indices().prepareCreate("test").setSettings(indexSettings(1, 2)).get();
        ensureGreen();

        client.prepareIndex("test").setSource("field", "value").get();
        refresh();

        // Run enough searches to build ARS stats on every node holding this shard. Other tests in
        // this class add data nodes and accumulate stats in the shared cluster, so we cannot rely
        // on a cold-start assumption here.
        for (int i = 0; i < 20; i++) {
            client.prepareSearch().setQuery(matchAllQuery()).get().decRef();
        }

        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
        Map<String, DiscoveryNode> coordinatingNodes = clusterStateResponse.getState().nodes().getCoordinatingOnlyNodes();
        assertEquals(1, coordinatingNodes.size());

        String coordinatingNodeId = coordinatingNodes.values().iterator().next().getId();
        NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats().setAdaptiveSelection(true).get();
        NodeStats nodeStats = statsResponse.getNodesMap().get(coordinatingNodeId);
        assertNotNull(nodeStats);
        // The cluster may have more than 3 data nodes if other tests added nodes, but we always
        // need stats for at least the 3 nodes holding the "test" shard.
        assertThat(nodeStats.getAdaptiveSelectionStats().getComputedStats().size(), greaterThanOrEqualTo(3));

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

}
