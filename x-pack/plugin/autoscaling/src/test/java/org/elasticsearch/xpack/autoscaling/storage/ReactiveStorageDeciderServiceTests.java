/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests the primitive methods in {@link ReactiveStorageDeciderService}. Tests of higher level methods are in
 * {@link ReactiveStorageDeciderDecisionTests}
 */
public class ReactiveStorageDeciderServiceTests extends AutoscalingTestCase {
    private static final List<String> SOME_ALLOCATION_DECIDERS = Arrays.asList(
        SameShardAllocationDecider.NAME,
        AwarenessAllocationDecider.NAME,
        EnableAllocationDecider.NAME
    );

    public void testIsDiskOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE));
        }
        decision.add(new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "test"));
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(
                label -> new Decision.Single(
                    randomValueOtherThan(Decision.Type.NO, () -> randomFrom(Decision.Type.values())),
                    label,
                    "test " + label
                )
            )
            .forEach(decision::add);

        assertThat(ReactiveStorageDeciderService.isDiskOnlyNoDecision(decision), is(true));
    }

    public void testIsNotDiskOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE, Decision.NO));
        }
        if (randomBoolean()) {
            decision.add(new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "test"));
            if (randomBoolean()) {
                decision.add(Decision.NO);
            } else {
                decision.add(new Decision.Single(Decision.Type.NO, randomFrom(SOME_ALLOCATION_DECIDERS), "test"));
            }
        } else if (randomBoolean()) {
            decision.add(new Decision.Single(Decision.Type.YES, DiskThresholdDecider.NAME, "test"));
        }
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(label -> new Decision.Single(randomFrom(Decision.Type.values()), label, "test " + label))
            .forEach(decision::add);

        assertThat(ReactiveStorageDeciderService.isDiskOnlyNoDecision(decision), is(false));
    }

    public void testIsFilterTierOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE));
        }
        decision.add(new Decision.Single(Decision.Type.NO, FilterAllocationDecider.NAME, "test"));
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(
                label -> new Decision.Single(
                    randomValueOtherThan(Decision.Type.NO, () -> randomFrom(Decision.Type.values())),
                    label,
                    "test " + label
                )
            )
            .forEach(decision::add);

        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".data", "hot"))
            .numberOfShards(randomIntBetween(1, 10))
            .numberOfReplicas(randomIntBetween(1, 10))
            .build();

        assertThat(ReactiveStorageDeciderService.isFilterTierOnlyDecision(decision, indexMetadata), is(true));
    }

    public void testIsNotTierOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE, Decision.NO));
        }
        Settings.Builder settings = settings(Version.CURRENT);
        if (randomBoolean()) {
            decision.add(new Decision.Single(Decision.Type.NO, FilterAllocationDecider.NAME, "test"));
            if (randomBoolean()) {
                decision.add(Decision.NO);
            } else if (randomBoolean()) {
                if (randomBoolean()) {
                    settings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._id", randomAlphaOfLength(5));
                } else {
                    decision.add(new Decision.Single(Decision.Type.NO, DataTierAllocationDecider.NAME, "test"));
                }
            } else {
                decision.add(
                    new Decision.Single(
                        Decision.Type.NO,
                        randomValueOtherThan(SameShardAllocationDecider.NAME, () -> randomFrom(SOME_ALLOCATION_DECIDERS)),
                        "test"
                    )
                );
            }
        } else if (randomBoolean()) {
            decision.add(new Decision.Single(Decision.Type.YES, FilterAllocationDecider.NAME, "test"));
        }
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(label -> new Decision.Single(randomFrom(Decision.Type.values()), label, "test " + label))
            .forEach(decision::add);

        assertThat(ReactiveStorageDeciderService.isFilterTierOnlyDecision(decision, metaWithSettings(settings)), is(false));
    }

    public void testFilterLooksLikeTier() {
        Settings.Builder settings = settings(Version.CURRENT);
        for (int i = 0; i < between(0, 10); ++i) {
            String key = randomValueOtherThanMany(name -> name.startsWith("_") || name.equals("name"), () -> randomAlphaOfLength(5));
            settings.put(
                randomFrom(
                    IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX,
                    IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX,
                    IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX
                ) + "." + key,
                randomAlphaOfLength(5)
            );
        }

        assertThat(ReactiveStorageDeciderService.filterLooksLikeTier(metaWithSettings(settings)), is(true));

        settings.put(
            randomFrom(
                IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX,
                IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX,
                IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX
            ) + "." + randomFrom("_ip", "_host_ip", "_publish_ip", "host", "_id", "_name", "name"),
            "1.2.3.4"
        );

        assertThat(ReactiveStorageDeciderService.filterLooksLikeTier(metaWithSettings(settings)), is(false));
    }

    private IndexMetadata metaWithSettings(Settings.Builder settings) {
        return IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(randomIntBetween(1, 10))
            .numberOfReplicas(randomIntBetween(0, 10))
            .build();
    }

    public void testSizeOf() {
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 10))
            .numberOfReplicas(randomIntBetween(1, 10))
            .build();
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        stateBuilder.routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build());
        addNode(stateBuilder);
        addNode(stateBuilder);
        ClusterState initialClusterState = stateBuilder.build();

        int shardId = randomInt(indexMetadata.getNumberOfShards() - 1);
        IndexShardRoutingTable subjectRoutings = initialClusterState.routingTable()
            .shardRoutingTable(indexMetadata.getIndex().getName(), shardId);
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            initialClusterState.mutableRoutingNodes(),
            initialClusterState,
            null,
            null,
            System.nanoTime()
        );
        ShardRouting primaryShard = subjectRoutings.primaryShard();
        ShardRouting replicaShard = subjectRoutings.replicaShards().get(0);
        DiscoveryNode[] nodes = initialClusterState.nodes().getAllNodes().toArray(DiscoveryNode[]::new);
        boolean useReplica = randomBoolean();
        if (useReplica || randomBoolean()) {
            startShard(allocation, primaryShard, nodes[0].getId());
            if (randomBoolean()) {
                startShard(allocation, replicaShard, nodes[1].getId());
            }
        }

        final ClusterState clusterState = updateClusterState(initialClusterState, allocation);

        Map<String, Long> shardSize = new HashMap<>();
        IntStream.range(0, randomInt(10))
            .mapToObj(i -> randomFrom(clusterState.routingTable().allShards().toList()))
            .filter(s -> s.shardId().getId() != shardId)
            .forEach(s -> shardSize.put(shardIdentifier(s), randomNonNegativeLong()));

        long expected = randomLongBetween(1, Long.MAX_VALUE);
        if (useReplica == false || randomBoolean()) {
            shardSize.put(shardIdentifier(primaryShard), expected);
        } else {
            shardSize.put(shardIdentifier(replicaShard), expected);
        }

        ShardRouting subjectShard = useReplica ? replicaShard : primaryShard;
        validateSizeOf(clusterState, subjectShard, shardSize, expected);
        validateSizeOf(clusterState, subjectShard, Map.of(), ByteSizeUnit.KB.toBytes(1));

        assertThat(createAllocationState(shardSize, clusterState).maxNodeLockedSize(), equalTo(0L));
    }

    public void testMaxNodeLockedSizeUsingAttributes() {
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        Metadata.Builder metaBuilder = Metadata.builder();
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(addRandomNodeLockUsingAttributes(settings(Version.CURRENT)))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        stateBuilder.routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build());
        ClusterState clusterState = stateBuilder.build();

        long baseSize = between(1, 10);
        Map<String, Long> shardSizes = IntStream.range(0, numberOfShards)
            .mapToObj(s -> clusterState.getRoutingTable().index(indexMetadata.getIndex()).shard(s))
            .flatMap(irt -> Stream.of(irt.primaryShard(), irt.replicaShards().get(0)))
            .collect(
                Collectors.toMap(
                    ClusterInfo::shardIdentifierFromRouting,
                    s -> s.primary() ? s.shardId().getId() + baseSize : between(1, 100)
                )
            );

        // keep the calculation in 2x until the end to avoid rounding.
        long nodeLockedSize = (baseSize * 2 + numberOfShards - 1) * numberOfShards / 2;
        assertThat(createAllocationState(shardSizes, clusterState).maxNodeLockedSize(), equalTo(nodeLockedSize));

        ClusterState withResizeSource = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(
                        IndexMetadata.builder(indexMetadata)
                            .settings(
                                Settings.builder()
                                    .put(indexMetadata.getSettings())
                                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, randomAlphaOfLength(9))
                            )
                    )
            )
            .build();

        assertThat(createAllocationState(shardSizes, withResizeSource).maxNodeLockedSize(), equalTo(nodeLockedSize * 2));
    }

    public void testNodeLockSplitClone() {
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(between(1, 10))
            .build();
        int numberOfShards = randomIntBetween(1, 2);
        int numberOfReplicas = randomIntBetween(1, 10);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, sourceIndexMetadata.getIndexUUID())
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndexMetadata.getIndex().getName())
            )
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();
        metaBuilder.put(sourceIndexMetadata, true);
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        stateBuilder.routingTable(
            RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                .addAsNew(sourceIndexMetadata)
                .addAsNew(indexMetadata)
                .build()
        );
        ClusterState clusterState = stateBuilder.build();

        long sourceSize = between(1, 10);
        Map<String, Long> shardSizes = Map.of(
            ClusterInfo.shardIdentifierFromRouting(
                clusterState.getRoutingTable().index(sourceIndexMetadata.getIndex()).shard(0).primaryShard()
            ),
            sourceSize
        );

        assertThat(createAllocationState(shardSizes, clusterState).maxNodeLockedSize(), equalTo(sourceSize * 2));
    }

    private Settings.Builder addRandomNodeLockUsingAttributes(Settings.Builder settings) {
        String setting = randomFrom(
            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING
        ).getKey();
        String attribute = randomFrom(DiscoveryNodeFilters.SINGLE_NODE_NAMES);
        return settings.put(setting + attribute, randomAlphaOfLength(5));
    }

    public void validateSizeOf(ClusterState clusterState, ShardRouting subjectShard, Map<String, Long> shardSize, long expected) {
        assertThat(createAllocationState(shardSize, clusterState).sizeOf(subjectShard), equalTo(expected));
    }

    private ReactiveStorageDeciderService.AllocationState createAllocationState(Map<String, Long> shardSize, ClusterState clusterState) {
        ClusterInfo info = new ClusterInfo(Map.of(), Map.of(), shardSize, Map.of(), Map.of(), Map.of());
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            null,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            null,
            info,
            null,
            Set.of(),
            Set.of()
        );
        return allocationState;
    }

    private void startShard(RoutingAllocation allocation, ShardRouting unassignedShard, String nodeId) {
        for (RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator(); iterator
            .hasNext();) {
            ShardRouting candidate = iterator.next();
            if (candidate == unassignedShard) {
                ShardRouting initialized = iterator.initialize(
                    nodeId,
                    null,
                    ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                    allocation.changes()
                );
                allocation.routingNodes()
                    .startShard(logger, initialized, allocation.changes(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                return;
            }
        }

        fail("must find shard: " + unassignedShard);
    }

    public void testSizeOfSnapshot() {
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        Metadata.Builder metaBuilder = Metadata.builder();
        RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            new Snapshot(randomAlphaOfLength(5), new SnapshotId(randomAlphaOfLength(5), UUIDs.randomBase64UUID())),
            Version.CURRENT,
            new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID())
        );
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 10))
            .numberOfReplicas(randomIntBetween(0, 10))
            .build();
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        stateBuilder.routingTable(
            RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                .addAsNewRestore(indexMetadata, recoverySource, new HashSet<>())
                .build()
        );
        ClusterState clusterState = stateBuilder.build();

        int shardId = randomInt(indexMetadata.getNumberOfShards() - 1);
        ShardRouting primaryShard = clusterState.routingTable()
            .shardRoutingTable(indexMetadata.getIndex().getName(), shardId)
            .primaryShard();

        Map<InternalSnapshotsInfoService.SnapshotShard, Long> shardSizeBuilder = new HashMap<>();
        IntStream.range(0, randomInt(10))
            .mapToObj(i -> randomFrom(clusterState.routingTable().allShards().toList()))
            .filter(s -> s.shardId().getId() != shardId)
            .forEach(s -> shardSizeBuilder.put(snapshotShardSizeKey(recoverySource, s), randomNonNegativeLong()));

        long expected = randomLongBetween(1, Long.MAX_VALUE);
        shardSizeBuilder.put(snapshotShardSizeKey(recoverySource, primaryShard), expected);

        validateSizeOfSnapshotShard(clusterState, primaryShard, shardSizeBuilder, expected);
        validateSizeOfSnapshotShard(clusterState, primaryShard, Map.of(), ByteSizeUnit.KB.toBytes(1));
    }

    private void validateSizeOfSnapshotShard(
        ClusterState clusterState,
        ShardRouting primaryShard,
        Map<InternalSnapshotsInfoService.SnapshotShard, Long> shardSizeBuilder,
        long expected
    ) {
        SnapshotShardSizeInfo shardSizeInfo = new SnapshotShardSizeInfo(shardSizeBuilder);
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            null,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            null,
            null,
            shardSizeInfo,
            Set.of(),
            Set.of()
        );

        assertThat(allocationState.sizeOf(primaryShard), equalTo(expected));
    }

    private InternalSnapshotsInfoService.SnapshotShard snapshotShardSizeKey(
        RecoverySource.SnapshotRecoverySource recoverySource,
        ShardRouting primaryShard
    ) {
        return new InternalSnapshotsInfoService.SnapshotShard(recoverySource.snapshot(), recoverySource.index(), primaryShard.shardId());
    }

    static void addNode(ClusterState.Builder stateBuilder) {
        addNode(stateBuilder, DiscoveryNodeRole.DATA_ROLE);
    }

    static void addNode(ClusterState.Builder stateBuilder, DiscoveryNodeRole role) {
        stateBuilder.nodes(
            DiscoveryNodes.builder(stateBuilder.nodes())
                .add(DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).name("test").roles(Set.of(role)).build())
        );
    }

    public void testUnmovableSize() {
        Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            // disk is 100 kb. Default is 90 percent. 10KB free is equivalent to default.
            String tenKb = ByteSizeValue.ofKb(10).toString();
            settingsBuilder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), tenKb);
            // also set these to pass validation
            settingsBuilder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), tenKb);
            settingsBuilder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), tenKb);
        }
        Settings settings = settingsBuilder.build();
        DiskThresholdSettings thresholdSettings = new DiskThresholdSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        String nodeId = randomAlphaOfLength(5);

        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(10)
            .build();
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        ClusterState clusterState = stateBuilder.build();

        Set<ShardRouting> shards = IntStream.range(0, between(1, 10))
            .mapToObj(i -> Tuple.tuple(new ShardId(indexMetadata.getIndex(), randomInt(10)), randomBoolean()))
            .distinct()
            .map(t -> TestShardRouting.newShardRouting(t.v1(), nodeId, t.v2(), ShardRoutingState.STARTED))
            .collect(Collectors.toSet());

        long minShardSize = randomLongBetween(1, 10);

        Map<String, DiskUsage> diskUsages = new HashMap<>();
        diskUsages.put(nodeId, new DiskUsage(nodeId, null, null, ByteSizeUnit.KB.toBytes(100), ByteSizeUnit.KB.toBytes(5)));
        Map<String, Long> shardSize = new HashMap<>();
        ShardRouting missingShard = randomBoolean() ? randomFrom(shards) : null;
        Collection<ShardRouting> shardsWithSizes = shards.stream().filter(s -> s != missingShard).collect(Collectors.toSet());
        for (ShardRouting shard : shardsWithSizes) {
            shardSize.put(shardIdentifier(shard), ByteSizeUnit.KB.toBytes(randomLongBetween(minShardSize, 100)));
        }
        if (shardsWithSizes.isEmpty() == false) {
            shardSize.put(shardIdentifier(randomFrom(shardsWithSizes)), ByteSizeUnit.KB.toBytes(minShardSize));
        }
        ClusterInfo info = new ClusterInfo(diskUsages, diskUsages, shardSize, Map.of(), Map.of(), Map.of());

        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            null,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            thresholdSettings,
            info,
            null,
            Set.of(),
            Set.of()
        );

        long result = allocationState.unmovableSize(nodeId, shards);
        if (missingShard != null
            && (missingShard.primary()
                || clusterState.getRoutingNodes().activePrimary(missingShard.shardId()) == null
                || info.getShardSize(clusterState.getRoutingNodes().activePrimary(missingShard.shardId())) == null)
            || minShardSize < 5) {
            // the diff between used and high watermark is 5 KB.
            assertThat(result, equalTo(ByteSizeUnit.KB.toBytes(5)));
        } else {
            assertThat(result, equalTo(ByteSizeUnit.KB.toBytes(minShardSize)));
        }
    }

    public void testCanRemainOnlyHighestTierPreference() {
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        addNode(stateBuilder);
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(10)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        ClusterState clusterState = stateBuilder.build();

        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            indexMetadata.getIndex().getName(),
            randomInt(10),
            clusterState.nodes().iterator().next().getId(),
            randomBoolean(),
            ShardRoutingState.STARTED
        );

        AllocationDecider no = new AllocationDecider() {
            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return Decision.NO;
            }
        };

        assertTrue(canRemainWithNoNodes(clusterState, shardRouting));
        assertFalse(canRemainWithNoNodes(clusterState, shardRouting, no));

        ClusterState clusterStateWithHotPreference = addPreference(indexMetadata, clusterState, "data_hot");
        assertTrue(canRemainWithNoNodes(clusterStateWithHotPreference, shardRouting));
        assertFalse(canRemainWithNoNodes(clusterStateWithHotPreference, shardRouting, no));

        ClusterState clusterStateWithWarmHotPreference = addPreference(indexMetadata, clusterState, "data_warm,data_hot");
        assertFalse(canRemainWithNoNodes(clusterStateWithWarmHotPreference, shardRouting));
        assertFalse(canRemainWithNoNodes(clusterStateWithWarmHotPreference, shardRouting, no));
    }

    public ClusterState addPreference(IndexMetadata indexMetadata, ClusterState clusterState, String preference) {
        IndexMetadata indexMetadataWithPreference = IndexMetadata.builder(indexMetadata)
            .settings(Settings.builder().put(indexMetadata.getSettings()).put(DataTier.TIER_PREFERENCE, preference))
            .build();

        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadataWithPreference, false))
            .build();
    }

    public boolean canRemainWithNoNodes(ClusterState clusterState, ShardRouting shardRouting, AllocationDecider... deciders) {
        AllocationDeciders allocationDeciders = new AllocationDeciders(Arrays.asList(deciders));
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            allocationDeciders,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            new DiskThresholdSettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            ClusterInfo.EMPTY,
            null,
            Set.of(),
            Set.of(DiscoveryNodeRole.DATA_WARM_NODE_ROLE)
        );

        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, null, null, randomLong());
        return allocationState.canRemainOnlyHighestTierPreference(shardRouting, allocation);
    }

    public void testNeedsThisTier() {
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        addNode(stateBuilder, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        Metadata.Builder metaBuilder = Metadata.builder();
        Settings.Builder settings = settings(Version.CURRENT);
        if (randomBoolean()) {
            settings.put(DataTier.TIER_PREFERENCE, randomBoolean() ? DataTier.DATA_HOT : "data_hot,data_warm");
        }
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings)
            .numberOfShards(10)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        ClusterState clusterState = stateBuilder.build();

        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            indexMetadata.getIndex().getName(),
            randomInt(10),
            clusterState.nodes().iterator().next().getId(),
            randomBoolean(),
            ShardRoutingState.STARTED
        );

        verifyNeedsWarmTier(clusterState, shardRouting, false);
        verifyNeedsWarmTier(addPreference(indexMetadata, clusterState, DataTier.DATA_COLD), shardRouting, false);
        verifyNeedsWarmTier(addPreference(indexMetadata, clusterState, DataTier.DATA_WARM), shardRouting, true);
        verifyNeedsWarmTier(addPreference(indexMetadata, clusterState, "data_warm,data_hot"), shardRouting, true);
        verifyNeedsWarmTier(addPreference(indexMetadata, clusterState, "data_warm,data_cold"), shardRouting, true);
    }

    public void testNeedsThisTierLegacy() {
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        addNode(stateBuilder);
        Metadata.Builder metaBuilder = Metadata.builder();
        Settings.Builder settings = settings(Version.CURRENT);
        if (randomBoolean()) {
            settings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data", DataTier.DATA_HOT);
        }
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings)
            .numberOfShards(10)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        ClusterState clusterState = stateBuilder.build();

        boolean primary = randomBoolean();
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            indexMetadata.getIndex().getName(),
            randomInt(10),
            clusterState.nodes().iterator().next().getId(),
            primary,
            ShardRoutingState.STARTED
        );

        AllocationDecider noFilter = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.single(Decision.Type.NO, FilterAllocationDecider.NAME, "test");
            }
        };
        AllocationDecider noSameShard = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.single(Decision.Type.NO, SameShardAllocationDecider.NAME, "test");
            }
        };
        AllocationDecider no = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.single(Decision.Type.NO, AwarenessAllocationDecider.NAME, "test");
            }
        };
        verifyNeedsWarmTier(clusterState, shardRouting, false);
        verifyNeedsWarmTier(clusterState, shardRouting, primary, noFilter);
        verifyNeedsWarmTier(clusterState, shardRouting, primary, noFilter, noSameShard);
        verifyNeedsWarmTier(clusterState, shardRouting, false, noFilter, no);
    }

    private void verifyNeedsWarmTier(
        ClusterState clusterState,
        ShardRouting shardRouting,
        boolean expected,
        AllocationDecider... deciders
    ) {
        AllocationDeciders allocationDeciders = new AllocationDeciders(Arrays.asList(deciders));
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            allocationDeciders,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            new DiskThresholdSettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            ClusterInfo.EMPTY,
            null,
            Set.of(),
            Set.of(DiscoveryNodeRole.DATA_WARM_NODE_ROLE)
        );

        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, null, null, randomLong());

        assertThat(allocationState.needsThisTier(shardRouting, allocation), is(expected));
    }

    public void testMessage() {
        assertThat(ReactiveStorageDeciderService.message(0, 0), equalTo("storage ok"));
        assertThat(ReactiveStorageDeciderService.message(0, 1023), equalTo("not enough storage available, needs 1023b"));
        assertThat(ReactiveStorageDeciderService.message(1024, 0), equalTo("not enough storage available, needs 1kb"));
        assertThat(ReactiveStorageDeciderService.message(0, 1024), equalTo("not enough storage available, needs 1kb"));
        assertThat(ReactiveStorageDeciderService.message(1023, 1), equalTo("not enough storage available, needs 1kb"));
    }

    private String shardIdentifier(ShardRouting s) {
        return ClusterInfo.shardIdentifierFromRouting(s);
    }

    public static ClusterState updateClusterState(ClusterState oldState, RoutingAllocation allocation) {
        assert allocation.metadata() == oldState.metadata();
        if (allocation.routingNodesChanged() == false) {
            return oldState;
        }
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = RoutingTable.of(oldRoutingTable.version(), newRoutingNodes);
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        return ClusterState.builder(oldState).routingTable(newRoutingTable).metadata(newMetadata).build();
    }
}
