/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

public class AllocationFailuresResetTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    private static ClusterState addNode(ClusterState state, String name) {
        var nodes = DiscoveryNodes.builder(state.nodes()).add(DiscoveryNodeUtils.create(name));
        return ClusterState.builder(state).nodes(nodes).build();
    }

    private static ClusterState removeNode(ClusterState state, String name) {
        var nodes = DiscoveryNodes.builder();
        state.nodes().stream().filter((node) -> node.getId() != name).forEach(nodes::add);
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
        var nonZeroFailures = 5;
        var unassignedInfo = new UnassignedInfo(
            UnassignedInfo.Reason.ALLOCATION_FAILED,
            null,
            null,
            nonZeroFailures,
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
                meta.getProject().index(index)
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
        allocationService.addAllocFailuresResetListenerTo(clusterService);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.stop();
        threadPool.shutdownNow();
    }

    /**
     * Create state with two nodes and allocation failures, and does <b>not</b> reset counter after node removal
     */
    public void testRemoveNodeDoesNotResetCounter() throws Exception {
        var initState = clusterService.state();
        var stateWithNewNode = addNode(initState, "node-2");
        clusterService.getClusterApplierService().onNewClusterState("add node", () -> stateWithNewNode, ActionListener.noop());

        var stateWithFailures = addShardWithFailures(stateWithNewNode);
        clusterService.getClusterApplierService().onNewClusterState("add failures", () -> stateWithFailures, ActionListener.noop());

        assertBusy(() -> {
            var resultState = clusterService.state();
            assertEquals(2, resultState.nodes().size());
            assertEquals(1, resultState.getRoutingTable().allShards().count());
            assertTrue(resultState.getRoutingNodes().hasAllocationFailures());
        });

        var stateWithRemovedNode = removeNode(stateWithFailures, "node-2");
        clusterService.getClusterApplierService().onNewClusterState("remove node", () -> stateWithRemovedNode, ActionListener.noop());
        assertBusy(() -> {
            var resultState = clusterService.state();
            assertEquals(1, resultState.nodes().size());
            assertEquals(1, resultState.getRoutingTable().allShards().count());
            assertTrue(resultState.getRoutingNodes().hasAllocationFailures());
        });
    }

    /**
     * Create state with one node and allocation failures, and reset counter after node addition
     */
    public void testAddNodeResetsCounter() throws Exception {
        var initState = clusterService.state();
        var stateWithFailures = addShardWithFailures(initState);
        clusterService.getClusterApplierService().onNewClusterState("add failures", () -> stateWithFailures, ActionListener.noop());

        var stateWithNewNode = addNode(stateWithFailures, "node-2");
        clusterService.getClusterApplierService().onNewClusterState("add node", () -> stateWithNewNode, ActionListener.noop());

        assertBusy(() -> {
            var resultState = clusterService.state();
            assertEquals(2, resultState.nodes().size());
            assertEquals(1, resultState.getRoutingTable().allShards().count());
            assertFalse(resultState.getRoutingNodes().hasAllocationFailures());
        });
    }
}
