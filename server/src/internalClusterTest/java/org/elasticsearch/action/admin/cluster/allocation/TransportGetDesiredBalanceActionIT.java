/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TransportGetDesiredBalanceActionIT extends ESIntegTestCase {

    public void testDesiredBalanceOnMultiNodeCluster() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(randomIntBetween(2, 5));

        String index = "test";
        int numberOfShards = 2;
        int numberOfReplicas = 1;
        createIndex(index, 2, 1);

        indexData(index);

        var clusterHealthResponse = clusterAdmin().health(new ClusterHealthRequest().waitForStatus(ClusterHealthStatus.GREEN)).get();
        assertEquals(RestStatus.OK, clusterHealthResponse.status());

        DesiredBalanceResponse desiredBalanceResponse = client().execute(GetDesiredBalanceAction.INSTANCE, new DesiredBalanceRequest())
            .get();

        assertEquals(1, desiredBalanceResponse.getRoutingTable().size());
        Map<Integer, DesiredBalanceResponse.DesiredShards> shardsMap = desiredBalanceResponse.getRoutingTable().get(index);
        assertEquals(numberOfShards, shardsMap.size());
        for (var entry : shardsMap.entrySet()) {
            Integer shardId = entry.getKey();
            DesiredBalanceResponse.DesiredShards desiredShards = entry.getValue();
            IndexShardRoutingTable shardRoutingTable = clusterAdmin().prepareState()
                .get()
                .getState()
                .routingTable()
                .shardRoutingTable(index, shardId);
            for (int i = 0; i < shardRoutingTable.size(); i++) {
                assertShard(shardRoutingTable.shard(i), desiredShards.current().get(i));
            }
            assertEquals(
                new DesiredBalanceResponse.ShardAssignmentView(getShardNodeIds(shardRoutingTable), numberOfReplicas + 1, 0, 0),
                desiredShards.desired()
            );
        }
    }

    public void testDesiredBalanceWithUnassignedShards() throws Exception {
        internalCluster().startNode();

        String index = "test";
        int numberOfShards = 2;
        int numberOfReplicas = 1;
        createIndex(index, numberOfShards, numberOfReplicas);
        indexData(index);
        var clusterHealthResponse = clusterAdmin().health(new ClusterHealthRequest(index).waitForStatus(ClusterHealthStatus.YELLOW)).get();
        assertEquals(RestStatus.OK, clusterHealthResponse.status());

        DesiredBalanceResponse desiredBalanceResponse = client().execute(GetDesiredBalanceAction.INSTANCE, new DesiredBalanceRequest())
            .get();

        assertEquals(1, desiredBalanceResponse.getRoutingTable().size());
        Map<Integer, DesiredBalanceResponse.DesiredShards> shardsMap = desiredBalanceResponse.getRoutingTable().get(index);
        assertEquals(numberOfShards, shardsMap.size());
        for (var entry : shardsMap.entrySet()) {
            Integer shardId = entry.getKey();
            DesiredBalanceResponse.DesiredShards desiredShards = entry.getValue();
            IndexShardRoutingTable shardRoutingTable = clusterAdmin().prepareState()
                .get()
                .getState()
                .routingTable()
                .shardRoutingTable(index, shardId);
            for (int i = 0; i < shardRoutingTable.size(); i++) {
                assertShard(shardRoutingTable.shard(i), desiredShards.current().get(i));
            }
            assertEquals(
                new DesiredBalanceResponse.ShardAssignmentView(
                    getShardNodeIds(shardRoutingTable),
                    numberOfReplicas + 1,
                    numberOfReplicas,
                    numberOfReplicas
                ),
                desiredShards.desired()
            );
        }
    }

    private void assertShard(ShardRouting shard, DesiredBalanceResponse.ShardView shardView) {
        assertEquals(shard.state(), shardView.state());
        assertEquals(shard.primary(), shardView.primary());
        assertEquals(shard.shardId().id(), shardView.shardId());
        assertEquals(shard.shardId().getIndexName(), shardView.index());
        assertEquals(shard.currentNodeId(), shardView.node());
        if (shardView.state() == ShardRoutingState.STARTED) {
            assertTrue(shardView.nodeIsDesired());
        } else if (shardView.state() == ShardRoutingState.UNASSIGNED) {
            assertFalse(shardView.nodeIsDesired());
        }
        assertEquals(shard.relocatingNodeId(), shardView.relocatingNode());
        assertNull(shardView.relocatingNodeIsDesired());
    }

    private static Set<String> getShardNodeIds(IndexShardRoutingTable shardRoutingTable) {
        return IntStream.range(0, shardRoutingTable.size())
            .mapToObj(shardRoutingTable::shard)
            .map(ShardRouting::currentNodeId)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    private static void indexData(String index) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < randomIntBetween(5, 32); i++) {
            bulkRequestBuilder.add(new IndexRequest(index).id(String.valueOf(i)).source("field", "foo " + i));
        }
        var bulkResponse = bulkRequestBuilder.get();
        assertFalse(bulkResponse.hasFailures());
    }
}
