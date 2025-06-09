/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep.Result;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Map;

import static org.elasticsearch.cluster.routing.TestShardRouting.buildUnassignedInfo;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
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
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
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
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> { existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v); });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED));

        logger.info(
            "running test with routing configurations:\n\t includes: [{}]\n\t excludes: [{}]\n\t requires: [{}]",
            includes,
            excludes,
            requires
        );
        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(
            index,
            1,
            0,
            step,
            existingSettings,
            node1Settings,
            node2Settings,
            indexRoutingTable,
            new ClusterStateWaitStep.Result(true, null)
        );
    }

    public void testRequireConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> requires = Map.of(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "foo", "bar");
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false, ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        assertAllocateStatus(
            index,
            1,
            0,
            step,
            existingSettings,
            node1Settings,
            Settings.builder(),
            indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1))
        );
    }

    public void testClusterExcludeFiltersConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false, ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(existingSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Map<String, IndexMetadata> indices = Map.of(index.getName(), indexMetadata);

        Settings clusterSettings = Settings.builder().put("cluster.routing.allocation.exclude._id", "node1").build();
        Settings.Builder nodeSettingsBuilder = Settings.builder();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).indices(indices).build();
        ProjectState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(project).transientSettings(clusterSettings))
            .nodes(
                DiscoveryNodes.builder()
                    .add(
                        DiscoveryNodeUtils.builder("node1")
                            .applySettings(nodeSettingsBuilder.build())
                            .address(new TransportAddress(TransportAddress.META_ADDRESS, 9200))
                            .build()
                    )
                    .add(
                        DiscoveryNodeUtils.builder("node2")
                            .applySettings(nodeSettingsBuilder.build())
                            .address(new TransportAddress(TransportAddress.META_ADDRESS, 9201))
                            .build()
                    )
            )
            .putRoutingTable(project.id(), RoutingTable.builder().add(indexRoutingTable).build())
            .build()
            .projectState(project.id());
        Result actualResult = step.isConditionMet(index, state);

        Result expectedResult = new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(1, 1));
        assertEquals(expectedResult.complete(), actualResult.complete());
        assertEquals(expectedResult.informationContext(), actualResult.informationContext());
    }

    public void testExcludeConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> excludes = Map.of(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "foo", "bar");
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false, ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        assertAllocateStatus(
            index,
            1,
            0,
            step,
            existingSettings,
            node1Settings,
            Settings.builder(),
            indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1))
        );
    }

    public void testIncludeConditionMetOnlyOneCopyAllocated() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = Map.of(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "foo", "bar");
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false, ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        assertAllocateStatus(
            index,
            1,
            0,
            step,
            existingSettings,
            node1Settings,
            Settings.builder(),
            indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1))
        );
    }

    public void testConditionNotMetDueToRelocation() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> requires = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._id", "node1")
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        boolean primaryOnNode1 = randomBoolean();
        ShardRouting shardOnNode1 = TestShardRouting.newShardRouting(
            new ShardId(index, 0),
            "node1",
            primaryOnNode1,
            ShardRoutingState.STARTED
        );
        shardOnNode1 = shardOnNode1.relocate("node3", 230);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(shardOnNode1)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false, ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        assertAllocateStatus(
            index,
            1,
            0,
            step,
            existingSettings,
            node1Settings,
            node2Settings,
            indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 2))
        );
    }

    public void testExecuteAllocateNotComplete() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Map<String, String> excludes = randomValueOtherThanMany(
            map -> map.keySet().stream().anyMatch(includes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5)
        );
        Map<String, String> requires = randomValueOtherThanMany(
            map -> map.keySet().stream().anyMatch(includes::containsKey) || map.keySet().stream().anyMatch(excludes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5)
        );
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> { existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v); });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), "node2", true, ShardRoutingState.STARTED));

        logger.info(
            "running test with routing configurations:\n\t includes: [{}]\n\t excludes: [{}]\n\t requires: [{}]",
            includes,
            excludes,
            requires
        );
        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(
            index,
            2,
            0,
            step,
            existingSettings,
            node1Settings,
            node2Settings,
            indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1))
        );
    }

    public void testExecuteAllocateNotCompleteOnlyOneCopyAllocated() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Map<String, String> excludes = randomValueOtherThanMany(
            map -> map.keySet().stream().anyMatch(includes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5)
        );
        Map<String, String> requires = randomValueOtherThanMany(
            map -> map.keySet().stream().anyMatch(includes::containsKey) || map.keySet().stream().anyMatch(excludes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5)
        );
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> { existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v); });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        boolean primaryOnNode1 = randomBoolean();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", primaryOnNode1, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node2", primaryOnNode1 == false, ShardRoutingState.STARTED));

        AllocationRoutedStep step = new AllocationRoutedStep(randomStepKey(), randomStepKey());
        logger.info(
            "running test with routing configurations:\n\t includes: [{}]\n\t excludes: [{}]\n\t requires: [{}]",
            includes,
            excludes,
            requires
        );
        assertAllocateStatus(
            index,
            2,
            0,
            step,
            existingSettings,
            node1Settings,
            node2Settings,
            indexRoutingTable,
            new ClusterStateWaitStep.Result(false, allShardsActiveAllocationInfo(0, 1))
        );
    }

    public void testExecuteAllocateUnassigned() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = AllocateActionTests.randomAllocationRoutingMap(1, 5);
        Map<String, String> excludes = randomValueOtherThanMany(
            map -> map.keySet().stream().anyMatch(includes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5)
        );
        Map<String, String> requires = randomValueOtherThanMany(
            map -> map.keySet().stream().anyMatch(includes::containsKey) || map.keySet().stream().anyMatch(excludes::containsKey),
            () -> AllocateActionTests.randomAllocationRoutingMap(1, 5)
        );
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> { existingSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v); });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
            .addShard(
                shardRoutingBuilder(new ShardId(index, 1), null, true, ShardRoutingState.UNASSIGNED).withUnassignedInfo(
                    buildUnassignedInfo("the shard is intentionally unassigned")
                ).build()
            );

        logger.info(
            "running test with routing configurations:\n\t includes: [{}]\n\t excludes: [{}]\n\t requires: [{}]",
            includes,
            excludes,
            requires
        );
        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(
            index,
            2,
            0,
            step,
            existingSettings,
            node1Settings,
            node2Settings,
            indexRoutingTable,
            new ClusterStateWaitStep.Result(false, waitingForActiveShardsAllocationInfo(0))
        );
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
        Settings.Builder existingSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
            .addShard(
                shardRoutingBuilder(new ShardId(index, 0), null, false, ShardRoutingState.UNASSIGNED).withUnassignedInfo(
                    new UnassignedInfo(Reason.REPLICA_ADDED, "no attempt")
                ).build()
            );

        AllocationRoutedStep step = createRandomInstance();
        assertAllocateStatus(
            index,
            1,
            1,
            step,
            existingSettings,
            node1Settings,
            node2Settings,
            indexRoutingTable,
            new ClusterStateWaitStep.Result(false, waitingForActiveShardsAllocationInfo(1))
        );
    }

    public void testExecuteIndexMissing() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        ProjectState state = projectStateWithEmptyProject();

        AllocationRoutedStep step = createRandomInstance();

        Result actualResult = step.isConditionMet(index, state);
        assertFalse(actualResult.complete());
        assertNull(actualResult.informationContext());
    }

    private void assertAllocateStatus(
        Index index,
        int shards,
        int replicas,
        AllocationRoutedStep step,
        Settings.Builder existingSettings,
        Settings.Builder node1Settings,
        Settings.Builder node2Settings,
        IndexRoutingTable.Builder indexRoutingTable,
        ClusterStateWaitStep.Result expectedResult
    ) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(existingSettings)
            .numberOfShards(shards)
            .numberOfReplicas(replicas)
            .build();
        Map<String, IndexMetadata> indices = Map.of(index.getName(), indexMetadata);

        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).indices(indices).build();
        ProjectState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .nodes(
                DiscoveryNodes.builder()
                    .add(
                        DiscoveryNodeUtils.builder("node1")
                            .applySettings(node1Settings.build())
                            .address(new TransportAddress(TransportAddress.META_ADDRESS, 9200))
                            .build()
                    )
                    .add(
                        DiscoveryNodeUtils.builder("node2")
                            .applySettings(node2Settings.build())
                            .address(new TransportAddress(TransportAddress.META_ADDRESS, 9201))
                            .build()
                    )
            )
            .putRoutingTable(project.id(), RoutingTable.builder().add(indexRoutingTable).build())
            .build()
            .projectState(project.id());
        Result actualResult = step.isConditionMet(index, state);
        assertEquals(expectedResult.complete(), actualResult.complete());
        assertEquals(expectedResult.informationContext(), actualResult.informationContext());
    }
}
