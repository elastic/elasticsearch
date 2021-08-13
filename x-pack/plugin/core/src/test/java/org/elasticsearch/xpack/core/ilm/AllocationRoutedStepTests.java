/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;


import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep.Result;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.step.info.AllocationInfo.allShardsActiveAllocationInfo;
import static org.elasticsearch.xpack.core.ilm.step.info.AllocationInfo.waitingForActiveShardsAllocationInfo;

public class AllocationRoutedStepTests extends AbstractStepTestCase<AllocationRoutedStep> {

    @Override
    public AllocationRoutedStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        return new AllocationRoutedStep(stepKey, nextStepKey);
    }

    @Override
    public AllocationRoutedStep mutateInstance(AllocationRoutedStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new AllocationRoutedStep(key, nextKey);
    }

    @Override
    public AllocationRoutedStep copyInstance(AllocationRoutedStep instance) {
        return new AllocationRoutedStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Map<String, String> excludes = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Map<String, String> requires = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED));

        logger.info("running test with routing configurations:\n\t includes: [{}]\n\t excludes: [{}]\n\t requires: [{}]",
            includes, excludes, requires);
        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(index, 1, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(true, null));
    }

    public void testRequireConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> requires = Collections.singletonMap(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "foo", "bar");
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false,
                ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        assertAllocateStatus(index, 1, 0, step, existingSettings, node1Settings, Settings.builder(), indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1)));
    }

    public void testClusterExcludeFiltersConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false,
                ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(existingSettings).numberOfShards(1)
            .numberOfReplicas(1).build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder().fPut(index.getName(),
            indexMetadata);

        Settings clusterSettings = Settings.builder()
            .put("cluster.routing.allocation.exclude._id", "node1")
            .build();
        Settings.Builder nodeSettingsBuilder = Settings.builder();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().indices(indices.build()).transientSettings(clusterSettings))
            .nodes(DiscoveryNodes.builder()
                .add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, 9200),
                    "node1"))
                .add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, 9201),
                    "node2")))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
        Result actualResult = step.isConditionMet(index, clusterState);

        Result expectedResult = new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(1, 1));
        assertEquals(expectedResult.isComplete(), actualResult.isComplete());
        assertEquals(expectedResult.getInfomationContext(), actualResult.getInfomationContext());
    }

    public void testExcludeConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> excludes = Collections.singletonMap(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "foo", "bar");
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false,
                ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        assertAllocateStatus(index, 1, 0, step, existingSettings, node1Settings, Settings.builder(), indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1)));
    }

    public void testIncludeConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = Collections.singletonMap(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "foo", "bar");
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false,
                ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        assertAllocateStatus(index, 1, 0, step, existingSettings, node1Settings, Settings.builder(), indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1)));
    }

    public void testConditionNotMetDueToRelocation() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> requires = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._id", "node1")
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        boolean primaryOnNode1 = randomBoolean();
        ShardRouting shardOnNode1 = TestShardRouting.newShardRouting(new ShardId(index, 0),
            "node1", primaryOnNode1, ShardRoutingState.STARTED);
        shardOnNode1 = shardOnNode1.relocate("node3", 230);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(shardOnNode1)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false,
                ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        assertAllocateStatus(index, 1, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 2)));
    }

    public void testExecuteAllocateNotComplete() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Map<String, String> excludes = randomValueOtherThanMany(map -> map.keySet().stream().anyMatch(includes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5));
        Map<String, String> requires = randomValueOtherThanMany(map -> map.keySet().stream().anyMatch(includes::containsKey) ||
                map.keySet().stream().anyMatch(excludes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5));
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), "node2", true, ShardRoutingState.STARTED));

        logger.info("running test with routing configurations:\n\t includes: [{}]\n\t excludes: [{}]\n\t requires: [{}]",
            includes, excludes, requires);
        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(index, 2, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1)));
    }

    public void testExecuteAllocateNotCompleteOnlyOneCopyAllocated() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Map<String, String> excludes = randomValueOtherThanMany(map -> map.keySet().stream().anyMatch(includes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5));
        Map<String, String> requires = randomValueOtherThanMany(map -> map.keySet().stream().anyMatch(includes::containsKey) ||
                map.keySet().stream().anyMatch(excludes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5));
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false,
                        ShardRoutingState.STARTED));


        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        logger.info("running test with routing configurations:\n\t includes: [{}]\n\t excludes: [{}]\n\t requires: [{}]",
            includes, excludes, requires);
        assertAllocateStatus(index, 2, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1)));
    }

    public void testExecuteAllocateUnassigned() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Map<String, String> excludes = randomValueOtherThanMany(map -> map.keySet().stream().anyMatch(includes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5));
        Map<String, String> requires = randomValueOtherThanMany(map -> map.keySet().stream().anyMatch(includes::containsKey) ||
                map.keySet().stream().anyMatch(excludes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5));
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), null, null, true, ShardRoutingState.UNASSIGNED,
                        new UnassignedInfo(randomFrom(Reason.values()), "the shard is intentionally unassigned")));

        logger.info("running test with routing configurations:\n\t includes: [{}]\n\t excludes: [{}]\n\t requires: [{}]",
            includes, excludes, requires);
        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(index, 2, 0, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                new ClusterStateWaitStep.Result(false, waitingForActiveShardsAllocationInfo(0)));
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
        Settings.Builder existingSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), null, null, false, ShardRoutingState.UNASSIGNED,
                new UnassignedInfo(Reason.REPLICA_ADDED, "no attempt")));

        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(index, 1, 1, step, existingSettings, node1Settings, node2Settings, indexRoutingTable,
            new ClusterStateWaitStep.Result(false, waitingForActiveShardsAllocationInfo(1)));
    }

    public void testExecuteIndexMissing() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();

        AllocationRoutedStep step = createRandomInstance();

        Result actualResult = step.isConditionMet(index, clusterState);
        assertFalse(actualResult.isComplete());
        assertNull(actualResult.getInfomationContext());
    }

    private void assertAllocateStatus(Index index, int shards, int replicas, AllocationRoutedStep step, Settings.Builder existingSettings,
            Settings.Builder node1Settings, Settings.Builder node2Settings, IndexRoutingTable.Builder indexRoutingTable,
            ClusterStateWaitStep.Result expectedResult) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(existingSettings).numberOfShards(shards)
                .numberOfReplicas(replicas).build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata> builder().fPut(index.getName(),
                indexMetadata);

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices.build()))
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
