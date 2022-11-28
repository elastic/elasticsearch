/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.node.shutdown.NodePrevalidateShardPathResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateShardPathRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateShardPathResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportPrevalidateShardPathAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrevalidateShardPathIT extends ESIntegTestCase {

    public void testCheckShards() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        String indexName = "index1";
        int index1shards = randomIntBetween(1, 5);
        createIndex("index1", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, index1shards).build());
        ensureGreen(indexName);
        var shardIds = clusterService().state()
            .routingTable()
            .allShards(indexName)
            .stream()
            .map(ShardRouting::shardId)
            .collect(Collectors.toSet());
        String node1Id = internalCluster().clusterService(node1).localNode().getId();
        String node2Id = internalCluster().clusterService(node2).localNode().getId();
        Set<ShardId> shardIdsToCheck = new HashSet<>(shardIds);
        boolean includeUnknownShardId = randomBoolean();
        if (includeUnknownShardId) {
            shardIdsToCheck.add(new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)));
        }
        PrevalidateShardPathRequest req = new PrevalidateShardPathRequest(shardIdsToCheck, node1Id, node2Id);
        PrevalidateShardPathResponse resp = client().execute(TransportPrevalidateShardPathAction.TYPE, req).get();
        var nodeResponses = resp.getNodes();
        assertThat(nodeResponses.size(), equalTo(2));
        assertThat(nodeResponses.stream().map(r -> r.getNode().getId()).collect(Collectors.toSet()), equalTo(Set.of(node1Id, node2Id)));
        assertTrue(resp.failures().isEmpty());
        for (NodePrevalidateShardPathResponse nodeResponse : nodeResponses) {
            assertThat(nodeResponse.getShardIds(), equalTo(shardIds));
        }
        // Check that after relocation the source node doesn't have the shard path
        String node3 = internalCluster().startDataOnlyNode();
        updateIndexSettings(indexName, Settings.builder().put("index.routing.allocation.exclude._name", node2));
        ensureGreen(indexName);
        assertBusy(() -> {
            // The excluded node should eventually delete the shards
            PrevalidateShardPathRequest req2 = new PrevalidateShardPathRequest(shardIdsToCheck, node2Id);
            PrevalidateShardPathResponse resp2 = client().execute(TransportPrevalidateShardPathAction.TYPE, req2).get();
            assertThat(resp2.getNodes().size(), equalTo(1));
            assertTrue(resp.failures().isEmpty());
            assertTrue(resp2.getNodes().get(0).getShardIds().isEmpty());
        });
    }
}
