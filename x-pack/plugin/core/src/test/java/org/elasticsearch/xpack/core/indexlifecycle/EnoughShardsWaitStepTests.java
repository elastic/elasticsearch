/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

public class EnoughShardsWaitStepTests extends ESTestCase {

    public void testConditionMet() {
        int numberOfShards = randomIntBetween(1, 10);
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();

        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numberOfShards; i++) {
            builder.addShard(TestShardRouting.newShardRouting(new ShardId(index, i),
                nodeId, true, ShardRoutingState.STARTED));
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .routingTable(RoutingTable.builder().add(builder.build()).build()).build();

        EnoughShardsWaitStep step = new EnoughShardsWaitStep(null, null, numberOfShards);
        assertTrue(step.isConditionMet(indexMetadata.getIndex(), clusterState));
    }

    public void testConditionNotMetBecauseOfActive() {
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();

        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(index).addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, 0),
                        nodeId, true, ShardRoutingState.INITIALIZING)))
                .build())
            .build();

        EnoughShardsWaitStep step = new EnoughShardsWaitStep(null, null, 1);
        assertFalse(step.isConditionMet(indexMetadata.getIndex(), clusterState));
    }

    public void testConditionNotMetBecauseOfShardCount() {
        int numberOfShards = randomIntBetween(1, 10);
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();

        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numberOfShards; i++) {
            builder.addShard(TestShardRouting.newShardRouting(new ShardId(index, i),
                nodeId, true, ShardRoutingState.INITIALIZING));
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .routingTable(RoutingTable.builder().add(builder.build()).build()).build();

        EnoughShardsWaitStep step = new EnoughShardsWaitStep(null, null, 1);
        assertFalse(step.isConditionMet(indexMetadata.getIndex(), clusterState));
    }
}
