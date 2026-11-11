/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Allocation during snapshot restores: ESA size gate, capacity decider, and storage monitor,
 * exercised through {@link AllocationService} / desired balance.
 */
public class StatelessRestoreAllocationTests extends ESAllocationTestCase {
    private static final long GB = ByteSizeValue.ofGb(1).getBytes();
    /** Filesystem capacity; default {@code reserved_bytes} is 20% → 20 GiB indexing reserve. */
    private static final long TOTAL = 100 * GB;
    private static final long INDEXING_RESERVED = 20 * GB;
    private static final String NODE = "index-node";
    private static final String PATH = "/data";

    private ClusterState restoreState(int count) {
        var projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT);
        var routing = RoutingTable.builder(new StatelessShardRoutingRoleStrategy());
        for (int i = 0; i < count; i++) {
            var index = IndexMetadata.builder("index-" + i)
                .settings(settings(IndexVersion.current()).put("index.allocation.existing_shards_allocator", "stateless"))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            projectMetadata.put(index, false);
            routing.addAsNewRestore(
                index,
                new RecoverySource.SnapshotRecoverySource(
                    RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                    new Snapshot(ProjectId.DEFAULT, "repo", new SnapshotId("snapshot-" + i, "uuid-" + i)),
                    IndexVersion.current(),
                    new IndexId(index.getIndex().getName(), "repository-index-" + i)
                ),
                Set.of()
            );
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(projectMetadata))
            .routingTable(GlobalRoutingTable.builder().put(ProjectId.DEFAULT, routing).build())
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode(NODE, Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.MASTER_ROLE)))
                    .localNodeId(NODE)
                    .masterNodeId(NODE)
            )
            .build();
    }

    private SnapshotShardSizeInfo sizes(ClusterState state, long size) {
        Map<InternalSnapshotsInfoService.SnapshotShard, Long> sizes = new HashMap<>();
        state.routingTable(ProjectId.DEFAULT).allShards().forEach(shard -> {
            var source = (RecoverySource.SnapshotRecoverySource) shard.recoverySource();
            sizes.put(new InternalSnapshotsInfoService.SnapshotShard(source.snapshot(), source.index(), shard.shardId()), size);
        });
        return new SnapshotShardSizeInfo(sizes);
    }

    private ClusterInfo info(long free) {
        return info(Map.of(NODE, new DiskUsage(NODE, NODE, PATH, TOTAL, free)), Map.of());
    }

    private ClusterInfo info(Map<String, DiskUsage> disks, Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservations) {
        return ClusterInfo.builder().leastAvailableSpaceUsage(disks).mostAvailableSpaceUsage(disks).reservedSpace(reservations).build();
    }

    private AllocationService service(AtomicReference<ClusterInfo> info, AtomicReference<SnapshotShardSizeInfo> sizes) {
        return service(info, sizes, true);
    }

    private AllocationService service(
        AtomicReference<ClusterInfo> info,
        AtomicReference<SnapshotShardSizeInfo> sizes,
        boolean snapshotRestoreAllocationEnabled
    ) {
        final Settings settings = Settings.builder()
            .put("cluster.routing.allocation.type", "desired_balance")
            .put(InternalClusterInfoService.SNAPSHOT_RESTORE_ALLOCATION_ENABLED.getKey(), snapshotRestoreAllocationEnabled)
            .build();
        final Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.add(InternalClusterInfoService.SNAPSHOT_RESTORE_ALLOCATION_ENABLED);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, registered);
        var service = new AllocationService(
            new AllocationDeciders(
                List.of(new SnapshotRestoreAllocationDecider(Settings.EMPTY, clusterSettings), new StatelessAllocationDecider())
            ),
            createShardsAllocator(settings),
            info::get,
            sizes::get,
            new StatelessShardRoutingRoleStrategy(),
            MeterRegistry.NOOP,
            clusterSettings
        );
        service.setExistingShardsAllocators(Map.of("stateless", new StatelessExistingShardsAllocator(clusterSettings)));
        return service;
    }

    private static ShardRouting primary(ClusterState state, String index) {
        return state.routingTable(ProjectId.DEFAULT).index(index).shard(0).primaryShard();
    }

    public void testWaitsWhileSnapshotShardSizeUnknownThenAllocates() {
        var state = restoreState(1);
        var sizeInfo = new AtomicReference<>(SnapshotShardSizeInfo.EMPTY);
        var info = new AtomicReference<>(info(70 * GB));
        var service = service(info, sizeInfo);

        state = service.reroute(state, "size unknown", ActionListener.noop());
        assertTrue(primary(state, "index-0").unassigned());
        assertEquals(
            UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA,
            primary(state, "index-0").unassignedInfo().lastAllocationStatus()
        );

        sizeInfo.set(sizes(state, 50 * GB));
        state = service.reroute(state, "size arrived", ActionListener.noop());
        assertTrue(primary(state, "index-0").initializing());
    }

    public void testDoesNotWaitForSnapshotShardSizeWhenDisabled() {
        var state = restoreState(1);
        var sizeInfo = new AtomicReference<>(SnapshotShardSizeInfo.EMPTY);
        var info = new AtomicReference<>(info(70 * GB));
        var service = service(info, sizeInfo, false);

        state = service.reroute(state, "size unknown, feature disabled", ActionListener.noop());
        assertNotEquals(
            UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA,
            primary(state, "index-0").unassignedInfo().lastAllocationStatus()
        );
    }

    public void testPermanentlyUnavailableShardSizeFailsAllocation() {
        var state = restoreState(1);
        var sizeInfo = new AtomicReference<>(sizes(state, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));
        var info = new AtomicReference<>(info(70 * GB));
        var service = service(info, sizeInfo);

        state = service.reroute(state, "size fetch failed", ActionListener.noop());
        assertTrue(primary(state, "index-0").unassigned());
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_NO, primary(state, "index-0").unassignedInfo().lastAllocationStatus());
    }

    public void testWaitsWhenDiskTooSmallThenAllocatesWhenCapacityAppears() {
        var state = restoreState(1);
        // free 70 - 1: shard 50 leaves less than 20 GiB indexing reserve
        var info = new AtomicReference<>(info(70 * GB - 1));
        var sizes = new AtomicReference<>(sizes(state, 50 * GB));
        var service = service(info, sizes);

        state = service.reroute(state, "disk tight", ActionListener.noop());
        assertTrue(primary(state, "index-0").unassigned());
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED, primary(state, "index-0").unassignedInfo().lastAllocationStatus());

        info.set(info(70 * GB));
        state = service.reroute(state, "exact fit", ActionListener.noop());
        assertTrue(primary(state, "index-0").initializing());
    }

    public void testWaitsWhenDiskStatsMissing() {
        var state = restoreState(1);
        var info = new AtomicReference<>(ClusterInfo.EMPTY);
        var sizes = new AtomicReference<>(sizes(state, 50 * GB));
        var service = service(info, sizes);

        state = service.reroute(state, "no disk stats", ActionListener.noop());
        assertTrue(primary(state, "index-0").unassigned());
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED, primary(state, "index-0").unassignedInfo().lastAllocationStatus());
    }

    public void testIncomingAssignmentsConsumeCapacity() {
        // free 70 holds one 50 GiB restore; a second must wait until capacity increases.
        var state = restoreState(2);
        var sizes = new AtomicReference<>(sizes(state, 50 * GB));
        var info = new AtomicReference<>(info(70 * GB));
        var service = service(info, sizes);

        state = service.reroute(state, "initial", ActionListener.noop());
        assertEquals(1, state.routingTable(ProjectId.DEFAULT).allShards().filter(ShardRouting::initializing).toList().size());
        assertEquals(1, state.getRoutingNodes().unassigned().size());

        info.set(info(120 * GB));
        state = service.reroute(state, "more capacity", ActionListener.noop());
        assertEquals(0, state.getRoutingNodes().unassigned().size());
    }

    public void testAllocatesWhenAnyCandidateHasCapacity() {
        final String fittingNode = "a-fitting-node";
        var state = restoreState(1);
        state = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).add(newNode(fittingNode, Set.of(DiscoveryNodeRole.INDEX_ROLE))))
            .build();
        var sizes = new AtomicReference<>(sizes(state, 50 * GB));
        var info = new AtomicReference<>(
            info(
                Map.of(
                    NODE,
                    new DiskUsage(NODE, NODE, PATH, TOTAL, 70 * GB - 1),
                    fittingNode,
                    new DiskUsage(fittingNode, fittingNode, PATH, TOTAL, 70 * GB)
                ),
                Map.of()
            )
        );
        var service = service(info, sizes);

        state = service.reroute(state, "one node fits", ActionListener.noop());

        assertTrue(primary(state, "index-0").initializing());
    }

    public void testMonitorReroutesWhenStorageChangesWhileRestorePending() {
        var state = new AtomicReference<>(restoreState(1));
        var sizes = new AtomicReference<>(sizes(state.get(), 50 * GB));
        var info = new AtomicReference<>(info(INDEXING_RESERVED));
        var service = service(info, sizes);
        var reroutes = new AtomicInteger();
        var clock = new AtomicLong(TimeValue.timeValueMinutes(10).millis());
        var interval = TimeValue.timeValueSeconds(30);
        var monitor = new SnapshotRestoreStorageMonitor(
            monitorClusterSettings(interval),
            clock::get,
            state::get,
            (reason, priority, listener) -> {
                reroutes.incrementAndGet();
                state.set(service.reroute(state.get(), reason, listener));
            }
        );

        monitor.onNewInfo(info.get());
        assertEquals(1, state.get().getRoutingNodes().unassigned().size());
        monitor.onNewInfo(info.get());
        assertEquals(1, reroutes.get());

        info.set(info(70 * GB));
        monitor.onNewInfo(info.get());
        assertEquals(1, reroutes.get());
        assertTrue(primary(state.get(), "index-0").unassigned());

        clock.addAndGet(interval.millis());
        monitor.onNewInfo(info.get());
        assertTrue(primary(state.get(), "index-0").initializing());
        assertEquals(2, reroutes.get());

        monitor.onNewInfo(info(80 * GB));
        assertEquals(2, reroutes.get());
    }

    public void testMonitorReroutesImmediatelyOnReservedSpaceChange() {
        var state = new AtomicReference<>(restoreState(1));
        var reroutes = new AtomicInteger();
        var clock = new AtomicLong(TimeValue.timeValueMinutes(10).millis());
        var interval = TimeValue.timeValueSeconds(30);
        var monitor = new SnapshotRestoreStorageMonitor(
            monitorClusterSettings(interval),
            clock::get,
            state::get,
            (reason, priority, listener) -> {
                reroutes.incrementAndGet();
                listener.onResponse(null);
            }
        );

        monitor.onNewInfo(info(50 * GB));
        assertEquals(1, reroutes.get());

        clock.addAndGet(1); // well within interval
        var shardId = primary(state.get(), "index-0").shardId();
        monitor.onNewInfo(
            info(
                Map.of(NODE, new DiskUsage(NODE, NODE, PATH, TOTAL, 50 * GB)),
                Map.of(new ClusterInfo.NodeAndPath(NODE, PATH), new ClusterInfo.ReservedSpace(10 * GB, Set.of(shardId)))
            )
        );
        assertEquals(2, reroutes.get());
    }

    public void testMonitorIgnoresSearchNodesAndNonMaster() {
        var withSearch = ClusterState.builder(restoreState(1))
            .nodes(DiscoveryNodes.builder(restoreState(1).nodes()).add(newNode("search", Set.of(DiscoveryNodeRole.SEARCH_ROLE))))
            .build();
        var state = new AtomicReference<>(withSearch);
        var reroutes = new AtomicInteger();
        var clock = new AtomicLong(TimeValue.timeValueMinutes(10).millis());
        var monitor = new SnapshotRestoreStorageMonitor(
            monitorClusterSettings(TimeValue.timeValueSeconds(60)),
            clock::get,
            state::get,
            (reason, priority, listener) -> {
                reroutes.incrementAndGet();
                listener.onResponse(null);
            }
        );
        monitor.onNewInfo(info(70 * GB));
        monitor.onNewInfo(
            info(
                Map.of(
                    NODE,
                    new DiskUsage(NODE, NODE, PATH, TOTAL, 70 * GB),
                    "search",
                    new DiskUsage("search", "search", PATH, TOTAL, 10 * GB)
                ),
                Map.of()
            )
        );
        assertEquals(1, reroutes.get());

        state.set(ClusterState.builder(restoreState(1)).nodes(DiscoveryNodes.builder(state.get().nodes()).masterNodeId(null)).build());
        monitor.onNewInfo(info(80 * GB));
        assertEquals(1, reroutes.get());
    }

    private static ClusterSettings monitorClusterSettings(TimeValue rerouteInterval) {
        return new ClusterSettings(
            Settings.builder()
                .put(InternalClusterInfoService.SNAPSHOT_RESTORE_ALLOCATION_ENABLED.getKey(), true)
                .put(SnapshotRestoreStorageMonitor.REROUTE_INTERVAL_SETTING.getKey(), rerouteInterval)
                .build(),
            Set.of(InternalClusterInfoService.SNAPSHOT_RESTORE_ALLOCATION_ENABLED, SnapshotRestoreStorageMonitor.REROUTE_INTERVAL_SETTING)
        );
    }
}
