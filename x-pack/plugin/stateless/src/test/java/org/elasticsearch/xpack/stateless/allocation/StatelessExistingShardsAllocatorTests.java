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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Defers snapshot restores until InternalSnapshotsInfoService has published a shard size. */
public class StatelessExistingShardsAllocatorTests extends ESAllocationTestCase {
    private static final long GB = ByteSizeValue.ofGb(1).getBytes();
    private static final String NODE = "index-node";
    private static final String PATH = "/data";

    private final StatelessExistingShardsAllocator allocator = new StatelessExistingShardsAllocator();

    private ClusterState restoreState() {
        var index = IndexMetadata.builder("index-0")
            .settings(settings(IndexVersion.current()).put("index.allocation.existing_shards_allocator", "stateless"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(
                RoutingTable.builder(new StatelessShardRoutingRoleStrategy())
                    .addAsNewRestore(
                        index,
                        new RecoverySource.SnapshotRecoverySource(
                            RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                            new Snapshot("repo", new SnapshotId("snapshot", "uuid")),
                            IndexVersion.current(),
                            new IndexId(index.getIndex().getName(), "repository-index")
                        ),
                        Set.of()
                    )
                    .build()
            )
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode(NODE, Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.MASTER_ROLE)))
                    .localNodeId(NODE)
                    .masterNodeId(NODE)
            )
            .build();
    }

    private SnapshotShardSizeInfo sizes(ClusterState state, long size) {
        var shard = state.routingTable().index("index-0").shard(0).primaryShard();
        var source = (RecoverySource.SnapshotRecoverySource) shard.recoverySource();
        return new SnapshotShardSizeInfo(
            Map.of(new InternalSnapshotsInfoService.SnapshotShard(source.snapshot(), source.index(), shard.shardId()), size)
        );
    }

    private ClusterInfo disk(long free) {
        var usage = Map.of(NODE, new DiskUsage(NODE, NODE, PATH, 100 * GB, free));
        return ClusterInfo.builder().leastAvailableSpaceUsage(usage).mostAvailableSpaceUsage(usage).build();
    }

    private AllocationService service(AtomicReference<ClusterInfo> info, AtomicReference<SnapshotShardSizeInfo> sizes) {
        var service = new AllocationService(
            new AllocationDeciders(
                List.of(
                    new SnapshotRestoreAllocationDecider(Settings.EMPTY),
                    new StatelessAllocationDecider()
                )
            ),
            createShardsAllocator(Settings.builder().put("cluster.routing.allocation.type", "desired_balance").build()),
            info::get,
            sizes::get,
            new StatelessShardRoutingRoleStrategy(),
            MeterRegistry.NOOP
        );
        service.setExistingShardsAllocators(Map.of("stateless", allocator));
        return service;
    }

    public void testIgnoresSnapshotPrimaryUntilSizeKnown() {
        var state = restoreState();
        var shard = state.routingTable().index("index-0").shard(0).primaryShard();
        var allocation = TestRoutingAllocationFactory.forClusterState(state).shardSizeInfo(SnapshotShardSizeInfo.EMPTY).build();
        var ignored = new AtomicReference<UnassignedInfo.AllocationStatus>();
        allocator.allocateUnassigned(shard, allocation, new ExistingShardsAllocator.UnassignedAllocationHandler() {
            @Override
            public ShardRouting initialize(String nodeId, String allocationId, long expectedShardSize, RoutingChangesObserver changes) {
                fail("should not initialize while size is unknown");
                return null;
            }

            @Override
            public void removeAndIgnore(UnassignedInfo.AllocationStatus attempt, RoutingChangesObserver changes) {
                ignored.set(attempt);
            }

            @Override
            public ShardRouting updateUnassigned(
                UnassignedInfo unassignedInfo,
                RecoverySource recoverySource,
                RoutingChangesObserver changes
            ) {
                fail("unexpected update");
                return null;
            }
        });
        assertEquals(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, ignored.get());

        var explain = allocator.explainUnassignedShardAllocation(shard, allocation);
        assertTrue(explain.isDecisionTaken());
        assertEquals(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, explain.getAllocationStatus());
    }

    public void testDoesNotIgnoreWhenSizeKnown() {
        var state = restoreState();
        var shard = state.routingTable().index("index-0").shard(0).primaryShard();
        var allocation = TestRoutingAllocationFactory.forClusterState(state).shardSizeInfo(sizes(state, 50 * GB)).build();
        allocator.allocateUnassigned(shard, allocation, new ExistingShardsAllocator.UnassignedAllocationHandler() {
            @Override
            public ShardRouting initialize(String nodeId, String allocationId, long expectedShardSize, RoutingChangesObserver changes) {
                fail("ESA must not initialize; desired balance assigns");
                return null;
            }

            @Override
            public void removeAndIgnore(UnassignedInfo.AllocationStatus attempt, RoutingChangesObserver changes) {
                fail("should not ignore when size is known");
            }

            @Override
            public ShardRouting updateUnassigned(
                UnassignedInfo unassignedInfo,
                RecoverySource recoverySource,
                RoutingChangesObserver changes
            ) {
                fail("unexpected update");
                return null;
            }
        });
        assertEquals(AllocateUnassignedDecision.NOT_TAKEN, allocator.explainUnassignedShardAllocation(shard, allocation));
    }

    public void testAllocationProceedsAfterSizeArrives() {
        var state = restoreState();
        var sizeInfo = new AtomicReference<>(SnapshotShardSizeInfo.EMPTY);
        var info = new AtomicReference<>(disk(70 * GB));
        var service = service(info, sizeInfo);

        state = service.reroute(state, "size unavailable", ActionListener.noop());
        var primary = state.routingTable().index("index-0").shard(0).primaryShard();
        assertTrue(primary.unassigned());
        assertEquals(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, primary.unassignedInfo().lastAllocationStatus());

        sizeInfo.set(sizes(state, 50 * GB));
        state = service.reroute(state, "snapshot size arrived", ActionListener.noop());
        assertTrue(state.routingTable().index("index-0").shard(0).primaryShard().initializing());
    }
}
