/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

public class BatchedQueryPhaseIT extends ESIntegTestCase {

    public void testNumReducePhases() {
        assumeTrue(
            "test skipped because batched query execution disabled by feature flag",
            SearchService.BATCHED_QUERY_PHASE_FEATURE_FLAG.isEnabled()
        );
        String indexName = "test-idx";
        assertAcked(
            prepareCreate(indexName).setMapping("title", "type=keyword")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        for (int i = 0; i < 100; i++) {
            prepareIndex(indexName).setId(Integer.toString(i)).setSource("title", "testing" + i).get();
        }
        refresh();

        final String coordinatorNode = internalCluster().getRandomNodeName();
        final String coordinatorNodeId = getNodeId(coordinatorNode);
        assertNoFailuresAndResponse(
            client(coordinatorNode).prepareSearch(indexName)
                .setBatchedReduceSize(2)
                .addAggregation(terms("terms").field("title"))
                .setSearchType(QUERY_THEN_FETCH),
            response -> {
                Map<String, Integer> shardsPerNode = getNodeToShardCountMap(indexName);
                // Shards are not batched if they are already on the coordinating node or if there is only one per data node.
                final int coordinatorShards = shardsPerNode.getOrDefault(coordinatorNodeId, 0);
                final long otherSingleShardNodes = shardsPerNode.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().equals(coordinatorNodeId) == false)
                    .filter(entry -> entry.getValue() == 1)
                    .count();
                final int numNotBatchedShards = coordinatorShards + (int) otherSingleShardNodes;

                // Because batched_reduce_size = 2, whenever two or more shard results exist on the coordinating node, they will be
                // partially reduced (batched queries do not count towards num_reduce_phases).
                // Hence, the formula: (# of NOT batched shards) - 1.
                final int expectedNumReducePhases = Math.max(1, numNotBatchedShards - 1);
                assertThat(response.getNumReducePhases(), equalTo(expectedNumReducePhases));
            }
        );
    }

    private Map<String, Integer> getNodeToShardCountMap(String indexName) {
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable(ProjectId.DEFAULT).index(indexName);
        if (indexRoutingTable == null) {
            return Collections.emptyMap();
        }

        Map<String, Integer> nodeToShardCount = new HashMap<>();
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = shardRoutingTable.shard(copy);
                String nodeId = shardRouting.currentNodeId();
                if (nodeId != null) {
                    nodeToShardCount.merge(nodeId, 1, Integer::sum);
                }
            }
        }

        return nodeToShardCount;
    }
}
