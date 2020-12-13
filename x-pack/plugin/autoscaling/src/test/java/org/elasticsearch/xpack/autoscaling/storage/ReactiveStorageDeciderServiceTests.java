/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import com.carrotsearch.hppc.IntHashSet;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

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
        stateBuilder.routingTable(RoutingTable.builder().addAsNew(indexMetadata).build());
        addNode(stateBuilder);
        addNode(stateBuilder);
        ClusterState initialClusterState = stateBuilder.build();

        int shardId = randomInt(indexMetadata.getNumberOfShards() - 1);
        IndexShardRoutingTable subjectRoutings = initialClusterState.routingTable()
            .shardRoutingTable(indexMetadata.getIndex().getName(), shardId);
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            new RoutingNodes(initialClusterState, false),
            initialClusterState,
            null,
            null,
            System.nanoTime()
        );
        ShardRouting primaryShard = subjectRoutings.primaryShard();
        ShardRouting replicaShard = subjectRoutings.replicaShards().get(0);
        DiscoveryNode[] nodes = StreamSupport.stream(initialClusterState.nodes().spliterator(), false).toArray(DiscoveryNode[]::new);
        boolean useReplica = randomBoolean();
        if (useReplica || randomBoolean()) {
            startShard(allocation, primaryShard, nodes[0].getId());
            if (randomBoolean()) {
                startShard(allocation, replicaShard, nodes[1].getId());
            }
        }

        final ClusterState clusterState = updateClusterState(initialClusterState, allocation);

        ImmutableOpenMap.Builder<String, Long> shardSizeBuilder = ImmutableOpenMap.builder();
        IntStream.range(0, randomInt(10))
            .mapToObj(i -> randomFrom(clusterState.routingTable().allShards()))
            .filter(s -> s.shardId().getId() != shardId)
            .forEach(s -> shardSizeBuilder.put(shardIdentifier(s), randomNonNegativeLong()));

        long expected = randomLongBetween(1, Long.MAX_VALUE);
        if (useReplica == false || randomBoolean()) {
            shardSizeBuilder.put(shardIdentifier(primaryShard), expected);
        } else {
            shardSizeBuilder.put(shardIdentifier(replicaShard), expected);
        }

        ShardRouting subjectShard = useReplica ? replicaShard : primaryShard;
        validateSizeOf(clusterState, subjectShard, shardSizeBuilder, expected);
        validateSizeOf(clusterState, subjectShard, ImmutableOpenMap.builder(), ByteSizeUnit.KB.toBytes(1));
    }

    public void validateSizeOf(
        ClusterState clusterState,
        ShardRouting subjectShard,
        ImmutableOpenMap.Builder<String, Long> shardSizeBuilder,
        long expected
    ) {
        ClusterInfo info = new ClusterInfo(null, null, shardSizeBuilder.build(), null, null);
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            null,
            null,
            info,
            null,
            Set.of()
        );

        assertThat(allocationState.sizeOf(subjectShard), equalTo(expected));
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
                allocation.routingNodes().startShard(logger, initialized, allocation.changes());
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
        stateBuilder.routingTable(RoutingTable.builder().addAsNewRestore(indexMetadata, recoverySource, new IntHashSet()).build());
        ClusterState clusterState = stateBuilder.build();

        int shardId = randomInt(indexMetadata.getNumberOfShards() - 1);
        ShardRouting primaryShard = clusterState.routingTable()
            .shardRoutingTable(indexMetadata.getIndex().getName(), shardId)
            .primaryShard();

        ImmutableOpenMap.Builder<InternalSnapshotsInfoService.SnapshotShard, Long> shardSizeBuilder = ImmutableOpenMap.builder();
        IntStream.range(0, randomInt(10))
            .mapToObj(i -> randomFrom(clusterState.routingTable().allShards()))
            .filter(s -> s.shardId().getId() != shardId)
            .forEach(s -> shardSizeBuilder.put(snapshotShardSizeKey(recoverySource, s), randomNonNegativeLong()));

        long expected = randomLongBetween(1, Long.MAX_VALUE);
        shardSizeBuilder.put(snapshotShardSizeKey(recoverySource, primaryShard), expected);

        validateSizeOfSnapshotShard(clusterState, primaryShard, shardSizeBuilder, expected);
        validateSizeOfSnapshotShard(clusterState, primaryShard, ImmutableOpenMap.builder(), ByteSizeUnit.KB.toBytes(1));
    }

    private void validateSizeOfSnapshotShard(
        ClusterState clusterState,
        ShardRouting primaryShard,
        ImmutableOpenMap.Builder<InternalSnapshotsInfoService.SnapshotShard, Long> shardSizeBuilder,
        long expected
    ) {
        SnapshotShardSizeInfo shardSizeInfo = new SnapshotShardSizeInfo(shardSizeBuilder.build());
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            null,
            null,
            null,
            shardSizeInfo,
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

    private void addNode(ClusterState.Builder stateBuilder) {
        stateBuilder.nodes(
            DiscoveryNodes.builder(stateBuilder.nodes())
                .add(
                    new DiscoveryNode(
                        "test",
                        UUIDs.randomBase64UUID(),
                        buildNewFakeTransportAddress(),
                        Map.of(),
                        Set.of(DiscoveryNodeRole.DATA_ROLE),
                        Version.CURRENT
                    )
                )
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

        ImmutableOpenMap.Builder<String, DiskUsage> diskUsagesBuilder = ImmutableOpenMap.builder();
        diskUsagesBuilder.put(nodeId, new DiskUsage(nodeId, null, null, ByteSizeUnit.KB.toBytes(100), ByteSizeUnit.KB.toBytes(5)));
        ImmutableOpenMap<String, DiskUsage> diskUsages = diskUsagesBuilder.build();
        ImmutableOpenMap.Builder<String, Long> shardSizeBuilder = ImmutableOpenMap.builder();
        ShardRouting missingShard = randomBoolean() ? randomFrom(shards) : null;
        Collection<ShardRouting> shardsWithSizes = shards.stream().filter(s -> s != missingShard).collect(Collectors.toSet());
        for (ShardRouting shard : shardsWithSizes) {
            shardSizeBuilder.put(shardIdentifier(shard), ByteSizeUnit.KB.toBytes(randomLongBetween(minShardSize, 100)));
        }
        if (shardsWithSizes.isEmpty() == false) {
            shardSizeBuilder.put(shardIdentifier(randomFrom(shardsWithSizes)), ByteSizeUnit.KB.toBytes(minShardSize));
        }
        ClusterInfo info = new ClusterInfo(diskUsages, diskUsages, shardSizeBuilder.build(), null, null);

        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            null,
            thresholdSettings,
            info,
            null,
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
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        return ClusterState.builder(oldState).routingTable(newRoutingTable).metadata(newMetadata).build();
    }
}
