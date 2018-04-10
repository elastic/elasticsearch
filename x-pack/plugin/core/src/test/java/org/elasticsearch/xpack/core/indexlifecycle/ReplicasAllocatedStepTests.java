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
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public class ReplicasAllocatedStepTests extends ESTestCase {

    public ReplicasAllocatedStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        int numberReplicas = randomIntBetween(0, 100);
        return new ReplicasAllocatedStep(stepKey, nextStepKey, numberReplicas);
    }

    public ReplicasAllocatedStep mutateInstance(ReplicasAllocatedStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        int numberReplicas = instance.getNumberReplicas();
        switch (between(0, 2)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            numberReplicas += randomIntBetween(1, 5);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new ReplicasAllocatedStep(key, nextKey, numberReplicas);
    }

    public void testHashcodeAndEquals() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomInstance(),
                instance -> new ReplicasAllocatedStep(instance.getKey(), instance.getNextStepKey(), instance.getNumberReplicas()),
                this::mutateInstance);
    }

    public void testConditionMet() {
        ReplicasAllocatedStep step = createRandomInstance();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(step.getNumberReplicas()).build();
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
                        nodeId, true, ShardRoutingState.STARTED)))
                .build())
            .build();
        assertTrue(step.isConditionMet(indexMetadata.getIndex(), clusterState));
    }

    public void testConditionNotMetAllocation() {
        ReplicasAllocatedStep step = createRandomInstance();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(step.getNumberReplicas()).build();
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

        assertFalse(step.isConditionMet(indexMetadata.getIndex(), clusterState));
    }

    public void testConditionNotMetNumberReplicas() {
        ReplicasAllocatedStep step = createRandomInstance();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(randomValueOtherThan(step.getNumberReplicas(), () -> randomIntBetween(0, 100))).build();
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
                        nodeId, true, ShardRoutingState.STARTED)))
                .build())
            .build();

        assertFalse(step.isConditionMet(indexMetadata.getIndex(), clusterState));
    }

    public void testConditionIndexMissing() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();

        ReplicasAllocatedStep step = createRandomInstance();

        IndexNotFoundException thrownException = expectThrows(IndexNotFoundException.class, () -> step.isConditionMet(index, clusterState));
        assertEquals("Index not found when executing " + step.getKey().getAction() + " lifecycle action.", thrownException.getMessage());
        assertEquals(index.getName(), thrownException.getIndex().getName());
    }
}
