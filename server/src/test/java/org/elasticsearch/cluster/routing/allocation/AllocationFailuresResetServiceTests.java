/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Set;

public class AllocationFailuresResetServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    private static ClusterState addNode(ClusterState state) {
        var nodes = DiscoveryNodes.builder(state.nodes()).add(DiscoveryNodeUtils.create("node-" + System.currentTimeMillis()));
        return ClusterState.builder(state).nodes(nodes).build();
    }

    private static ClusterState addShardWithFailures(ClusterState state) {
        var index = "index-1";
        var shard = 0;

        var indexMeta = new IndexMetadata.Builder(index).settings(
            Settings.builder().put(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
        ).numberOfShards(1).numberOfReplicas(0).build();

        var meta = Metadata.builder(state.metadata()).put(indexMeta, false).build();

        var shardId = new ShardId(indexMeta.getIndex(), shard);
        var exhaustFailures = 5;
        var unassignedInfo = new UnassignedInfo(
            UnassignedInfo.Reason.ALLOCATION_FAILED,
            null,
            null,
            exhaustFailures,
            0,
            0,
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Set.of(),
            null
        );
        var shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            new RecoverySource.EmptyStoreRecoverySource(),
            unassignedInfo,
            ShardRouting.Role.DEFAULT
        );

        var routingTable = new RoutingTable.Builder().add(
            new IndexRoutingTable.Builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, indexMeta.getIndex()).initializeAsNew(
                meta.index(index)
            ).addIndexShard(IndexShardRoutingTable.builder(shardId).addShard(shardRouting)).build()
        ).build();

        return ClusterState.builder(state).metadata(meta).routingTable(routingTable).build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("reset-alloc-failures");
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        var allocationService = new AllocationService(
            new AllocationDeciders(List.of(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        AllocationFailuresResetService resetService = new AllocationFailuresResetService(clusterService, allocationService);
        clusterService.getClusterApplierService().addListener(resetService);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.stop();
        threadPool.shutdownNow();
    }

    public void testNoChangesDoesNotResetCounter() throws Exception {
        var initState = clusterService.state();
        var stateWithFailures = addShardWithFailures(initState);
        clusterService.getClusterApplierService().onNewClusterState("add failures", () -> stateWithFailures, ActionListener.noop());

        assertBusy(() -> {
            var resultState = clusterService.state();
            assertFalse(resultState.getRoutingNodes().hasAllocationFailures());
        });

    }

    public void testAddNodeResetsCounter() throws Exception {
        var initState = clusterService.state();
        var stateWithFailures = addShardWithFailures(initState);
        clusterService.getClusterApplierService().onNewClusterState("add failures", () -> stateWithFailures, ActionListener.noop());

        var stateWithNewNode = addNode(stateWithFailures);
        clusterService.getClusterApplierService().onNewClusterState("add node", () -> stateWithNewNode, ActionListener.noop());

        assertBusy(() -> {
            var resultState = clusterService.state();
            assertEquals(1, resultState.getRoutingTable().allShards().count());
            assertFalse(resultState.getRoutingNodes().hasAllocationFailures());
        });
    }
}
