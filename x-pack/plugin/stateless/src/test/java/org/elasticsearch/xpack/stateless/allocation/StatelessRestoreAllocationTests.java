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
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.SnapshotRestoreDiskPressure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
        var metadata = Metadata.builder();
        var routing = RoutingTable.builder(new StatelessShardRoutingRoleStrategy());
        for (int i = 0; i < count; i++) {
            var index = IndexMetadata.builder("index-" + i)
                .settings(settings(IndexVersion.current()).put("index.allocation.existing_shards_allocator", "stateless"))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            metadata.put(index, false);
            routing.addAsNewRestore(
                index,
                new RecoverySource.SnapshotRecoverySource(
                    RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                    new Snapshot("repo", new SnapshotId("snapshot-" + i, "uuid-" + i)),
                    IndexVersion.current(),
                    new IndexId(index.getIndex().getName(), "repository-index-" + i)
                ),
                Set.of()
            );
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routing.build())
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
        state.routingTable().allShards().forEach(shard -> {
            var source = (RecoverySource.SnapshotRecoverySource) shard.recoverySource();
            sizes.put(new InternalSnapshotsInfoService.SnapshotShard(source.snapshot(), source.index(), shard.shardId()), size);
        });
        return new SnapshotShardSizeInfo(sizes);
    }

    private ClusterInfo info(long free) {
        return info(Map.of(NODE, new DiskUsage(NODE, NODE, PATH, TOTAL, free)), Map.of());
    }

    private ClusterInfo info(Map<String, DiskUsage> disks, Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservations) {
        return new ClusterInfo(
            disks,
            disks,
            Map.of(),
            Map.of(),
            Map.of(),
            reservations,
            Map.of(),
            Map.of(),
            ShardAndIndexHeapUsage.ZERO,
            Map.of(),
            Map.of(),
            Map.of(),
            Set.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
    }

    private record RestoreDeciderAndPressure(SnapshotRestoreAllocationDecider decider, SnapshotRestoreDiskPressure pressure) {}

    private static RestoreDeciderAndPressure restoreDecider() {
        var pressure = new SnapshotRestoreDiskPressure(Settings.EMPTY);
        return new RestoreDeciderAndPressure(new SnapshotRestoreAllocationDecider(Settings.EMPTY, pressure), pressure);
    }

    private AllocationService service(
        SnapshotRestoreAllocationDecider restoreDecider,
        AtomicReference<ClusterInfo> info,
        AtomicReference<SnapshotShardSizeInfo> sizes
    ) {
        var service = new AllocationService(
            new AllocationDeciders(List.of(restoreDecider, new StatelessAllocationDecider())),
            createShardsAllocator(Settings.builder().put("cluster.routing.allocation.type", "desired_balance").build()),
            info::get,
            sizes::get,
            new StatelessShardRoutingRoleStrategy(),
            MeterRegistry.NOOP
        );
        service.setExistingShardsAllocators(Map.of("stateless", new StatelessExistingShardsAllocator()));
        return service;
    }

    private AllocationService service(AtomicReference<ClusterInfo> info, AtomicReference<SnapshotShardSizeInfo> sizes) {
        return service(restoreDecider().decider(), info, sizes);
    }

    private static ShardRouting primary(ClusterState state, String index) {
        return state.routingTable().index(index).shard(0).primaryShard();
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
        var restore = restoreDecider();
        // free 70 - 1: shard 50 leaves less than 20 GiB indexing reserve
        var info = new AtomicReference<>(info(70 * GB - 1));
        var sizes = new AtomicReference<>(sizes(state, 50 * GB));
        var service = service(restore.decider(), info, sizes);

        state = service.reroute(state, "disk tight", ActionListener.noop());
        assertTrue(primary(state, "index-0").unassigned());
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED, primary(state, "index-0").unassignedInfo().lastAllocationStatus());
        // 1 byte free shortfall; default indexing shared cache is 50% → total disk = 2.
        assertEquals(2L, restore.pressure().unmetTotalDiskBytes());

        info.set(info(70 * GB));
        state = service.reroute(state, "exact fit", ActionListener.noop());
        assertTrue(primary(state, "index-0").initializing());
        assertEquals(0L, restore.pressure().unmetTotalDiskBytes());
    }

    public void testWaitsWhenDiskStatsMissing() {
        var state = restoreState(1);
        var restore = restoreDecider();
        var info = new AtomicReference<>(ClusterInfo.EMPTY);
        var sizes = new AtomicReference<>(sizes(state, 50 * GB));
        var service = service(restore.decider(), info, sizes);

        state = service.reroute(state, "no disk stats", ActionListener.noop());
        assertTrue(primary(state, "index-0").unassigned());
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED, primary(state, "index-0").unassignedInfo().lastAllocationStatus());
        // Missing disk stats throttle without a known shortfall magnitude.
        assertEquals(0L, restore.pressure().unmetTotalDiskBytes());
    }

    public void testIncomingAssignmentsConsumeCapacity() {
        // free 70 holds one 50 GiB restore; a second must wait until capacity increases.
        var state = restoreState(2);
        var restore = restoreDecider();
        var sizes = new AtomicReference<>(sizes(state, 50 * GB));
        var info = new AtomicReference<>(info(70 * GB));
        var service = service(restore.decider(), info, sizes);

        state = service.reroute(state, "initial", ActionListener.noop());
        assertEquals(1, state.routingTable().allShards().filter(ShardRouting::initializing).toList().size());
        assertEquals(1, state.getRoutingNodes().unassigned().size());
        assertTrue(restore.pressure().unmetTotalDiskBytes() > 0);

        info.set(info(120 * GB));
        state = service.reroute(state, "more capacity", ActionListener.noop());
        assertEquals(0, state.getRoutingNodes().unassigned().size());
        assertEquals(0L, restore.pressure().unmetTotalDiskBytes());
    }

    public void testUnmetShortfallNotRecordedWhileFetchingShardSize() {
        var state = restoreState(1);
        var restore = restoreDecider();
        var sizeInfo = new AtomicReference<>(SnapshotShardSizeInfo.EMPTY);
        var info = new AtomicReference<>(info(70 * GB - 1));
        var service = service(restore.decider(), info, sizeInfo);

        state = service.reroute(state, "size unknown", ActionListener.noop());
        assertTrue(primary(state, "index-0").unassigned());
        assertEquals(
            UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA,
            primary(state, "index-0").unassignedInfo().lastAllocationStatus()
        );
        assertEquals(0L, restore.pressure().unmetTotalDiskBytes());
    }

    public void testMonitorReroutesWhenStorageChangesWhileRestorePending() {
        var state = new AtomicReference<>(restoreState(1));
        var sizes = new AtomicReference<>(sizes(state.get(), 50 * GB));
        var info = new AtomicReference<>(info(INDEXING_RESERVED));
        var service = service(info, sizes);
        var reroutes = new AtomicInteger();
        var monitor = new SnapshotRestoreStorageMonitor(state::get, (reason, priority, listener) -> {
            reroutes.incrementAndGet();
            state.set(service.reroute(state.get(), reason, listener));
        });

        monitor.onNewInfo(info.get());
        assertEquals(1, state.get().getRoutingNodes().unassigned().size());
        monitor.onNewInfo(info.get());
        assertEquals(1, reroutes.get());

        info.set(info(70 * GB));
        monitor.onNewInfo(info.get());
        assertTrue(primary(state.get(), "index-0").initializing());

        monitor.onNewInfo(info(80 * GB));
        assertEquals(2, reroutes.get());
    }

    public void testMonitorIgnoresSearchNodesAndNonMaster() {
        var withSearch = ClusterState.builder(restoreState(1))
            .nodes(DiscoveryNodes.builder(restoreState(1).nodes()).add(newNode("search", Set.of(DiscoveryNodeRole.SEARCH_ROLE))))
            .build();
        var state = new AtomicReference<>(withSearch);
        var reroutes = new AtomicInteger();
        var monitor = new SnapshotRestoreStorageMonitor(state::get, (reason, priority, listener) -> {
            reroutes.incrementAndGet();
            listener.onResponse(null);
        });
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
}
