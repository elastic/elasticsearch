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
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.IndexingDiskController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

/** Exercises restore admission through desired-balance simulation and reconciliation. */
public class SnapshotRestoreAllocationDeciderTests extends ESAllocationTestCase {
    private static final long GB = ByteSizeValue.ofGb(1).getBytes();
    /** Filesystem capacity used in ClusterInfo; default reserved_bytes is 20% → 20 GiB headroom. */
    private static final long TOTAL = 100 * GB;
    private static final long HEADROOM = 20 * GB;
    private static final String NODE = "index-node";
    private static final String PATH = "/data";

    private final SnapshotRestoreAllocationDecider decider = new SnapshotRestoreAllocationDecider(
        IndexingDiskController.INDEXING_DISK_RESERVED_BYTES_SETTING.get(Settings.EMPTY)
    );

    private ClusterState state(int count) {
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

    private Decision decide(ClusterState state, ClusterInfo info, SnapshotShardSizeInfo sizes) {
        var allocation = TestRoutingAllocationFactory.forClusterState(state).clusterInfo(info).shardSizeInfo(sizes).build();
        allocation.setDebugMode(RoutingAllocation.DebugMode.ON);
        return decider.canAllocate(
            state.getRoutingNodes().unassigned().iterator().next(),
            allocation.routingNodes().node(NODE),
            allocation
        );
    }

    private AllocationService service(AtomicReference<ClusterInfo> info, AtomicReference<SnapshotShardSizeInfo> sizes) {
        var service = new AllocationService(
            new AllocationDeciders(List.of(decider, new StatelessAllocationDecider())),
            createShardsAllocator(Settings.builder().put("cluster.routing.allocation.type", "desired_balance").build()),
            info::get,
            sizes::get,
            new StatelessShardRoutingRoleStrategy(),
            MeterRegistry.NOOP
        );
        service.setExistingShardsAllocators(Map.of("stateless", new StatelessExistingShardsAllocator()));
        return service;
    }

    public void testMissingInfoAndHeadroom() {
        var state = state(1);
        // free 70, shard 50, headroom 20 → fits exactly
        assertEquals(Decision.Type.YES, decide(state, info(70 * GB), sizes(state, 50 * GB)).type());
        var denied = decide(state, info(70 * GB - 1), sizes(state, 50 * GB));
        assertEquals(Decision.Type.THROTTLE, denied.type());
        assertThat(denied.getExplanation(), containsString("headroom [" + HEADROOM + "]"));

        assertEquals(Decision.Type.NO, decide(state, info(70 * GB), sizes(state, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)).type());
        assertEquals(Decision.Type.THROTTLE, decide(state, ClusterInfo.EMPTY, sizes(state, 50 * GB)).type());
    }

    public void testFailedSnapshotSizeFailsRestoreAllocation() {
        var state = state(1);
        var sizeInfo = new AtomicReference<>(sizes(state, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));
        var info = new AtomicReference<>(info(70 * GB));
        var service = service(info, sizeInfo);
        state = service.reroute(state, "size fetch failed", ActionListener.noop());
        var primary = state.routingTable().index("index-0").shard(0).primaryShard();
        assertTrue(primary.unassigned());
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_NO, primary.unassignedInfo().lastAllocationStatus());
    }

    public void testOnlyUnassignedSnapshotPrimariesOnIndexNodes() {
        var state = state(1);
        var allocation = TestRoutingAllocationFactory.forClusterState(state).clusterInfo(info(0)).build();
        var node = allocation.routingNodes().node(NODE);
        var emptyStore = TestShardRouting.shardRoutingBuilder(
            state.routingTable().index("index-0").shard(0).primaryShard().shardId(),
            null,
            true,
            ShardRoutingState.UNASSIGNED
        ).withRecoverySource(RecoverySource.EmptyStoreRecoverySource.INSTANCE).withRole(ShardRouting.Role.INDEX_ONLY).build();
        assertEquals(Decision.Type.YES, decider.canAllocate(emptyStore, node, allocation).type());
        assertEquals(
            Decision.Type.YES,
            decider.canAllocate(
                state.routingTable().index("index-0").shard(0).primaryShard().initialize(NODE, null, 50 * GB),
                node,
                allocation
            ).type()
        );
        var search = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).add(newNode("search", Set.of(DiscoveryNodeRole.SEARCH_ROLE))))
            .build()
            .getRoutingNodes()
            .node("search");
        assertEquals(
            Decision.Type.YES,
            decider.canAllocate(state.routingTable().index("index-0").shard(0).primaryShard(), search, allocation).type()
        );
    }

    public void testIncomingAssignmentsConsumeCapacity() {
        // free 70 holds one 50 GiB restore (50 + 20 headroom); a second must wait.
        var state = state(2);
        var sizes = new AtomicReference<>(sizes(state, 50 * GB));
        var info = new AtomicReference<>(info(70 * GB));
        var service = service(info, sizes);
        state = service.reroute(state, "initial", ActionListener.noop());
        assertEquals(1, state.routingTable().allShards().filter(ShardRouting::initializing).toList().size());
        assertEquals(1, state.getRoutingNodes().unassigned().size());

        var incoming = state.routingTable().allShards().filter(ShardRouting::initializing).toList().getFirst();
        // Reported reservation replaces the initializing estimate; remaining free after 30 GiB reserved is still too small for 50 + 20.
        info.set(
            info(
                Map.of(NODE, new DiskUsage(NODE, NODE, PATH, TOTAL, 70 * GB)),
                Map.of(new ClusterInfo.NodeAndPath(NODE, PATH), new ClusterInfo.ReservedSpace(30 * GB, Set.of(incoming.shardId())))
            )
        );
        assertEquals(Decision.Type.THROTTLE, decide(state, info.get(), sizes.get()).type());
        // A 20 GiB candidate fits exactly: usable 70 - 30 reserved = 40, headroom 20.
        assertEquals(Decision.Type.YES, decide(state, info.get(), sizes(state, 20 * GB)).type());

        info.set(info(120 * GB));
        state = service.reroute(state, "more capacity", ActionListener.noop());
        assertEquals(0, state.getRoutingNodes().unassigned().size());
    }

    public void testOutgoingShardDoesNotFreeSpace() {
        var state = state(2);
        var snapshotSizes = sizes(state, 50 * GB);
        state = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).add(newNode("destination", Set.of(DiscoveryNodeRole.INDEX_ROLE))))
            .build();
        var nodes = state.mutableRoutingNodes();
        var iterator = nodes.unassigned().iterator();
        iterator.next();
        var started = nodes.startShard(
            iterator.initialize(NODE, null, 50 * GB, RoutingChangesObserver.NOOP),
            RoutingChangesObserver.NOOP,
            50 * GB
        );
        var source = nodes.relocateShard(
            started,
            "destination",
            50 * GB,
            "test relocation",
            RoutingChangesObserver.NOOP,
            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
        ).v1();
        state = ClusterState.builder(state).routingTable(state.globalRoutingTable().rebuild(nodes, state.metadata())).build();
        // Only headroom left free; relocating shard must not be credited until deleted.
        var disks = Map.of(NODE, new DiskUsage(NODE, NODE, PATH, TOTAL, HEADROOM));
        assertEquals(
            Decision.Type.THROTTLE,
            decide(
                state,
                new ClusterInfo(
                    disks,
                    disks,
                    Map.of(ClusterInfo.shardIdentifierFromRouting(source), 50 * GB),
                    Map.of(),
                    Map.of(ClusterInfo.NodeAndShard.from(source), PATH),
                    Map.of(),
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
                ),
                snapshotSizes
            ).type()
        );
    }

    public void testMonitorReroutesWhenStorageChangesWhileRestorePending() {
        var state = new AtomicReference<>(state(1));
        var sizes = new AtomicReference<>(sizes(state.get(), 50 * GB));
        var info = new AtomicReference<>(info(HEADROOM)); // only headroom free → cannot allocate
        var service = service(info, sizes);
        var reroutes = new AtomicInteger();
        var monitor = new SnapshotRestoreStorageMonitor(state::get, (reason, priority, listener) -> {
            reroutes.incrementAndGet();
            state.set(service.reroute(state.get(), reason, listener));
        });

        monitor.onNewInfo(info.get());
        assertEquals(1, state.get().getRoutingNodes().unassigned().size());
        monitor.onNewInfo(info.get()); // unchanged → no second reroute
        assertEquals(1, reroutes.get());

        info.set(info(70 * GB));
        monitor.onNewInfo(info.get());
        assertTrue(state.get().routingTable().index("index-0").shard(0).primaryShard().initializing());

        // No pending unassigned snapshot primary → further capacity changes are ignored.
        monitor.onNewInfo(info(80 * GB));
        assertEquals(2, reroutes.get());
    }

    public void testMonitorIgnoresSearchNodesAndNonMaster() {
        var withSearch = ClusterState.builder(state(1))
            .nodes(DiscoveryNodes.builder(state(1).nodes()).add(newNode("search", Set.of(DiscoveryNodeRole.SEARCH_ROLE))))
            .build();
        var state = new AtomicReference<>(withSearch);
        var reroutes = new AtomicInteger();
        var monitor = new SnapshotRestoreStorageMonitor(state::get, (reason, priority, listener) -> {
            reroutes.incrementAndGet();
            listener.onResponse(null);
        });
        monitor.onNewInfo(info(70 * GB));
        // Search-node free-space change alone is not a storage change for this monitor.
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

        state.set(ClusterState.builder(state(1)).nodes(DiscoveryNodes.builder(state.get().nodes()).masterNodeId(null)).build());
        monitor.onNewInfo(info(80 * GB));
        assertEquals(1, reroutes.get());
    }
}
