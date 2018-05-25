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
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep.Result;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public class ReplicasAllocatedStepTests extends AbstractStepTestCase<ReplicasAllocatedStep> {

    @Override
    public ReplicasAllocatedStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new ReplicasAllocatedStep(stepKey, nextStepKey);
    }

    @Override
    public ReplicasAllocatedStep mutateInstance(ReplicasAllocatedStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        if (randomBoolean()) {
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }
        return new ReplicasAllocatedStep(key, nextKey);
    }

    @Override
    public ReplicasAllocatedStep copyInstance(ReplicasAllocatedStep instance) {
        return new ReplicasAllocatedStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
        ReplicasAllocatedStep step = createRandomInstance();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(randomIntBetween(0, 5)).build();
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
        Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertTrue(result.isComplete());
        assertNull(result.getInfomationContext());
    }

    public void testConditionNotMetAllocation() {
        ReplicasAllocatedStep step = createRandomInstance();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(randomIntBetween(0, 5)).build();
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

        Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertFalse(result.isComplete());
        assertEquals(new ReplicasAllocatedStep.Info(indexMetadata.getNumberOfReplicas(), false),
                result.getInfomationContext());
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
