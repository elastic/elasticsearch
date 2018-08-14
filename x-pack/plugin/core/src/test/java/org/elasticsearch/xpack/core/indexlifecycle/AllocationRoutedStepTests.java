/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;


import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep.Result;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.util.Collections;
import java.util.Map;

public class AllocationRoutedStepTests extends AbstractStepTestCase<AllocationRoutedStep> {

    @Override
    public AllocationRoutedStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        boolean waitOnAllShardCopies = randomBoolean();

        return new AllocationRoutedStep(stepKey, nextStepKey, waitOnAllShardCopies);
    }

    @Override
    public AllocationRoutedStep mutateInstance(AllocationRoutedStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        boolean waitOnAllShardCopies = instance.getWaitOnAllShardCopies();

        switch (between(0, 2)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            waitOnAllShardCopies = waitOnAllShardCopies == false;
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new AllocationRoutedStep(key, nextKey, waitOnAllShardCopies);
    }

    @Override
    public AllocationRoutedStep copyInstance(AllocationRoutedStep instance) {
        return new AllocationRoutedStep(instance.getKey(), instance.getNextStepKey(), instance.getWaitOnAllShardCopies());
    }

    public void testConditionMet() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> excludes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> requires = AllocateActionTests.randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED));

        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(index, 1, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(true, null));
    }

    public void testConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> excludes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> requires = AllocateActionTests.randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false,
                        ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey(), false);
        assertAllocateStatus(index, 1, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(true, null));
    }

    public void testExecuteAllocateNotComplete() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> excludes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> requires = AllocateActionTests.randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), "node2", true, ShardRoutingState.STARTED));

        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(index, 2, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(false, new AllocationRoutedStep.Info(0, 1, true)));
    }

    public void testExecuteAllocateNotCompleteOnlyOneCopyAllocated() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> excludes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> requires = AllocateActionTests.randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false,
                        ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey(), true);
        assertAllocateStatus(index, 2, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(false, new AllocationRoutedStep.Info(0, 1, true)));
    }

    public void testExecuteAllocateUnassigned() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> excludes = AllocateActionTests.randomMap(1, 5);
        Map<String, String> requires = AllocateActionTests.randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), null, null, true, ShardRoutingState.UNASSIGNED,
                        new UnassignedInfo(randomFrom(Reason.values()), "the shard is intentionally unassigned")));

        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(index, 2, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(false, new AllocationRoutedStep.Info(0, -1, false)));
    }

    /**
     * this  tests the scenario where
     *
     * PUT index
     * {
     *  "settings": {
     *   "number_of_replicas": 0,
     *   "number_of_shards": 1
     *  }
     * }
     *
     * PUT index/_settings
     * {
     *  "number_of_replicas": 1,
     *  "index.routing.allocation.include._name": "{node-name}"
     * }
     */
    public void testExecuteReplicasNotAllocatedOnSingleNode() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> requires = Collections.singletonMap("_name", "node1");
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
            .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        requires.forEach((k, v) -> {
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), null, null, false, ShardRoutingState.UNASSIGNED,
                new UnassignedInfo(Reason.REPLICA_ADDED, "no attempt")));

        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(index, 1, 1, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
            new ClusterStateWaitStep.Result(false, new AllocationRoutedStep.Info(1, -1, false)));
    }

    public void testExecuteIndexMissing() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();

        AllocationRoutedStep step = createRandomInstance();

        IndexNotFoundException thrownException = expectThrows(IndexNotFoundException.class, () -> step.isConditionMet(index, clusterState));
        assertEquals("Index not found when executing " + step.getKey().getAction() + " lifecycle action.", thrownException.getMessage());
        assertEquals(index.getName(), thrownException.getIndex().getName());
    }

    private void assertAllocateStatus(Index index, int shards, int replicas, AllocationRoutedStep step, Settings.Builder existingSettings,
            Settings.Builder node1Settings, Settings.Builder node2Settings, IndexRoutingTable.Builder indexRoutingTable,
            ClusterStateWaitStep.Result expectedResult) {
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName()).settings(existingSettings).numberOfShards(shards)
                .numberOfReplicas(replicas).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(DiscoveryNode.createLocal(node1Settings.build(), new TransportAddress(TransportAddress.META_ADDRESS, 9200),
                                "node1"))
                        .add(DiscoveryNode.createLocal(node2Settings.build(), new TransportAddress(TransportAddress.META_ADDRESS, 9201),
                                "node2")))
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
        Result actualResult = step.isConditionMet(index, clusterState);
        assertEquals(expectedResult.isComplete(), actualResult.isComplete());
        assertEquals(expectedResult.getInfomationContext(), actualResult.getInfomationContext());
    }
}
