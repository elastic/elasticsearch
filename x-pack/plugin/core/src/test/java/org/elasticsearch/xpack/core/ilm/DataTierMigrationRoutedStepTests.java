/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep.Result;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.step.info.AllocationInfo;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.cluster.routing.allocation.DataTier.TIER_PREFERENCE;
import static org.elasticsearch.xpack.core.ilm.CheckShrinkReadyStepTests.randomUnassignedInfo;
import static org.elasticsearch.xpack.core.ilm.step.info.AllocationInfo.waitingForActiveShardsAllocationInfo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DataTierMigrationRoutedStepTests extends AbstractStepTestCase<DataTierMigrationRoutedStep> {

    @Override
    public DataTierMigrationRoutedStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        return new DataTierMigrationRoutedStep(stepKey, nextStepKey);
    }

    @Override
    public DataTierMigrationRoutedStep mutateInstance(DataTierMigrationRoutedStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new DataTierMigrationRoutedStep(key, nextKey);
    }

    @Override
    public DataTierMigrationRoutedStep copyInstance(DataTierMigrationRoutedStep instance) {
        return new DataTierMigrationRoutedStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testExecuteWithUnassignedShard() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Index index = indexMetadata.getIndex();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
            .addShard(
                shardRoutingBuilder(new ShardId(index, 1), null, true, ShardRoutingState.UNASSIGNED).withUnassignedInfo(
                    randomUnassignedInfo("the shard is intentionally unassigned")
                ).build()
            );

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .nodes(DiscoveryNodes.builder().add(newNode("node1", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE))))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
        DataTierMigrationRoutedStep step = createRandomInstance();
        Result expectedResult = new Result(false, waitingForActiveShardsAllocationInfo(1));

        Result actualResult = step.isConditionMet(index, clusterState);
        assertThat(actualResult.isComplete(), is(false));
        assertThat(actualResult.getInfomationContext(), is(expectedResult.getInfomationContext()));
    }

    public void testExecuteWithPendingShards() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(IndexVersion.current()).put(TIER_PREFERENCE, DataTier.DATA_WARM))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Index index = indexMetadata.getIndex();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED));

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)))
                    .add(newNode("node2", Collections.singleton(DiscoveryNodeRole.DATA_WARM_NODE_ROLE)))
            )
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
        DataTierMigrationRoutedStep step = createRandomInstance();
        Result expectedResult = new Result(
            false,
            new AllocationInfo(
                0,
                1,
                true,
                "["
                    + index.getName()
                    + "] lifecycle action ["
                    + step.getKey().action()
                    + "] waiting for "
                    + "[1] shards to be moved to the [data_warm] tier (tier migration preference configuration is [data_warm])"
            )
        );

        Result actualResult = step.isConditionMet(index, clusterState);
        assertThat(actualResult.isComplete(), is(false));
        assertThat(actualResult.getInfomationContext(), is(expectedResult.getInfomationContext()));
    }

    public void testExecuteWithPendingShardsAndTargetRoleNotPresentInCluster() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(IndexVersion.current()).put(TIER_PREFERENCE, DataTier.DATA_WARM))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Index index = indexMetadata.getIndex();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED));

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .nodes(DiscoveryNodes.builder().add(newNode("node1", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE))))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
        DataTierMigrationRoutedStep step = createRandomInstance();
        Result expectedResult = new Result(
            false,
            new AllocationInfo(
                0,
                1,
                true,
                "index ["
                    + index.getName()
                    + "] has a preference for tiers [data_warm], but no nodes for any of those tiers are available "
                    + "in the cluster"
            )
        );

        Result actualResult = step.isConditionMet(index, clusterState);
        assertThat(actualResult.isComplete(), is(false));
        assertThat(actualResult.getInfomationContext(), is(expectedResult.getInfomationContext()));
    }

    public void testExecuteIndexMissing() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();

        DataTierMigrationRoutedStep step = createRandomInstance();

        Result actualResult = step.isConditionMet(index, clusterState);
        assertThat(actualResult.isComplete(), is(false));
        assertThat(actualResult.getInfomationContext(), is(nullValue()));
    }

    public void testExecuteIsComplete() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(IndexVersion.current()).put(TIER_PREFERENCE, DataTier.DATA_WARM))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Index index = indexMetadata.getIndex();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", true, ShardRoutingState.STARTED));

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)))
                    .add(newNode("node2", Collections.singleton(DiscoveryNodeRole.DATA_WARM_NODE_ROLE)))
            )
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
        DataTierMigrationRoutedStep step = createRandomInstance();
        Result result = step.isConditionMet(index, clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), is(nullValue()));
    }

    public void testExecuteWithGenericDataNodes() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(IndexVersion.current()).put(TIER_PREFERENCE, DataTier.DATA_WARM))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Index index = indexMetadata.getIndex();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED));

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .nodes(DiscoveryNodes.builder().add(newNode("node1", Collections.singleton(DiscoveryNodeRole.DATA_ROLE))))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
        DataTierMigrationRoutedStep step = createRandomInstance();
        Result result = step.isConditionMet(index, clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), is(nullValue()));
    }

    public void testExecuteForIndexWithoutTierRoutingInformationWaitsForReplicasToBeActive() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Index index = indexMetadata.getIndex();
        {
            IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
                .addReplica(ShardRouting.Role.DEFAULT);

            ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .nodes(DiscoveryNodes.builder().add(newNode("node1", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE))))
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
                .build();
            DataTierMigrationRoutedStep step = createRandomInstance();
            Result expectedResult = new Result(false, waitingForActiveShardsAllocationInfo(1));

            Result result = step.isConditionMet(index, clusterState);
            assertThat(result.isComplete(), is(false));
            assertThat(result.getInfomationContext(), is(expectedResult.getInfomationContext()));
        }

        {
            IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", false, ShardRoutingState.STARTED));

            ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("node1", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)))
                        .add(newNode("node2", Collections.singleton(DiscoveryNodeRole.DATA_WARM_NODE_ROLE)))
                )
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
                .build();
            DataTierMigrationRoutedStep step = createRandomInstance();

            Result result = step.isConditionMet(index, clusterState);
            assertThat(result.isComplete(), is(true));
            assertThat(result.getInfomationContext(), is(nullValue()));
        }
    }

    private DiscoveryNode newNode(String nodeId, Set<DiscoveryNodeRole> roles) {
        return DiscoveryNodeUtils.builder(nodeId).roles(roles).build();
    }
}
