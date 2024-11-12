/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStatsProvider;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceMetrics;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceReconciler;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatelessDesiredBalanceReconcilerTests extends ESAllocationTestCase {

    public void testStatelessFallbackAllocation() {
        final var discoveryNodesBuilder = DiscoveryNodes.builder();
        final var indexNodes = List.of("node-0", "node-1");
        final var searchNodes = List.of("node-2", "node-3", "node-4", "node-5");
        indexNodes.forEach(
            node -> discoveryNodesBuilder.add(newNode(node, node, Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE)))
        );
        searchNodes.forEach(node -> discoveryNodesBuilder.add(newNode(node, node, Set.of(DiscoveryNodeRole.SEARCH_ROLE))));
        discoveryNodesBuilder.masterNodeId("node-0").localNodeId("node-0");
        final var discoveryNodes = discoveryNodesBuilder.build();

        final var indexMetadata = IndexMetadata.builder("index-1").settings(indexSettings(IndexVersion.current(), 1, 2)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).addAsNew(indexMetadata))
            .build();

        final boolean searchShardsIgnored = randomBoolean();
        final Set<String> desiredNodes = searchShardsIgnored ? Set.of("node-0") : Set.of("node-0", "node-2", "node-4");
        final var initialForcedAllocationDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                // allocation on desired nodes is temporarily not possible
                return desiredNodes.contains(node.nodeId()) ? Decision.NO : Decision.YES;
            }
        };

        final var allocation = createRoutingAllocationFrom(clusterState, initialForcedAllocationDecider, new StatelessAllocationDecider());
        final var balance = new DesiredBalance(
            1,
            Map.of(shardId, new ShardAssignment(desiredNodes, 3, searchShardsIgnored ? 2 : 0, searchShardsIgnored ? 2 : 0))
        );

        reconcile(allocation, balance);

        if (searchShardsIgnored) {
            assertThat(allocation.routingNodes().unassigned().ignored(), hasSize(2));
        } else {
            // Only one search shard can fall back to allocation to an undesired node
            assertThat(allocation.routingNodes().unassigned().ignored(), hasSize(1));
        }

        // none of the desired nodes have a shard assigned
        desiredNodes.forEach(node -> assertThat(allocation.routingNodes().node(node).size(), equalTo(0)));
        assertThat(allocation.routingNodes().node("node-1").size(), equalTo(1)); // fall back allocation
        if (searchShardsIgnored) {
            searchNodes.forEach(node -> assertThat(allocation.routingNodes().node("node-2").size(), equalTo(0)));
        } else {
            // only one of the search shards falls back to allocation to an undesired node
            assertThat(allocation.routingNodes().node("node-3").size() + allocation.routingNodes().node("node-5").size(), equalTo(1));
        }
    }

    private static RoutingAllocation createRoutingAllocationFrom(ClusterState clusterState, AllocationDecider... deciders) {
        return new RoutingAllocation(
            new AllocationDeciders(List.of(deciders)),
            clusterState.mutableRoutingNodes(),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
    }

    private static void reconcile(RoutingAllocation routingAllocation, DesiredBalance desiredBalance) {
        final var threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillisSupplier()).thenReturn(new AtomicLong()::incrementAndGet);
        new DesiredBalanceReconciler(createBuiltInClusterSettings(), threadPool, DesiredBalanceMetrics.NOOP, EMPTY_NODE_ALLOCATION_STATS)
            .reconcile(desiredBalance, routingAllocation);
    }

    private static final NodeAllocationStatsProvider EMPTY_NODE_ALLOCATION_STATS = new NodeAllocationStatsProvider(
        WriteLoadForecaster.DEFAULT
    ) {
        @Override
        public Map<String, NodeAllocationStats> stats(
            ClusterState clusterState,
            ClusterInfo clusterInfo,
            @Nullable DesiredBalance desiredBalance
        ) {
            return Map.of();
        }
    };
}
