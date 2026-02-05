/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.addTemporaryStateListener;

/**
 * Supplies an {@link AllocationDecider} with settable sets of data nodes that return certain {@link Decision}s for
 * {@link AllocationDecider#canAllocate} and {@link AllocationDecider#canRemain}. Rebalancing in the integration tests
 * will be disabled because {@link AllocationDecider#canRebalance} is overridden: only the first two phases of the balancer,
 * allocating unassigned shards and moving shards that cannot remain, will run.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public abstract class AbstractAllocationDecisionTestCase extends ESIntegTestCase {

    protected static final Set<String> CAN_ALLOCATE_NOT_PREFERRED_NODE_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected static final Set<String> CAN_ALLOCATE_THROTTLE_NODE_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected static final Set<String> CAN_ALLOCATE_NO_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected static final Set<String> CAN_REMAIN_NO_NODE_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected static final Set<String> CAN_REMAIN_NOT_PREFERRED_NODE_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Before
    public final void clearDeciderState() {
        CAN_ALLOCATE_NOT_PREFERRED_NODE_IDS.clear();
        CAN_ALLOCATE_THROTTLE_NODE_IDS.clear();
        CAN_ALLOCATE_NO_IDS.clear();
        CAN_REMAIN_NO_NODE_IDS.clear();
        CAN_REMAIN_NOT_PREFERRED_NODE_IDS.clear();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestAllocationPlugin.class);
    }

    /**
     * Wait for the index shard to be allocated to a node.
     *
     * @param indexName The single-shard index to monitor
     * @return A {@link SubscribableListener} that will resolve with the node that the shard was allocated to
     */
    protected SubscribableListener<DiscoveryNode> waitForAllocation(String indexName) {
        final var firstAllocationNode = new AtomicReference<DiscoveryNode>();
        return addTemporaryStateListener(internalCluster().clusterService(), state -> {
            final var index = state.routingTable(ProjectId.DEFAULT).index(indexName);
            if (index != null) {
                assert index.allShards().count() == 1 : "expected a single shard, got " + index.allShards().toList();
                final var shardRouting = index.shard(0).primaryShard();
                final var currentNodeId = shardRouting.currentNodeId();
                if (currentNodeId != null && shardRouting.started()) {
                    final var node = state.nodes().get(currentNodeId);
                    firstAllocationNode.set(node);
                    return true;
                }
            }
            return false;
        }).andThenApply(v -> firstAllocationNode.get());
    }

    /**
     * Ensure a shard is eventually allocated to one of a set of nodes
     *
     * @param indexName The single-shard index to monitor
     * @param expectedNodes The set of nodes that will constitute a successful allocation
     */
    protected void ensureShardIsAllocatedToNodes(String indexName, Set<String> expectedNodes) {
        awaitClusterState(state -> {
            final var index = state.routingTable(ProjectId.DEFAULT).index(indexName);
            if (index != null) {
                assert index.allShards().count() == 1 : "expected a single shard, got " + index.allShards().toList();
                final var shardRouting = index.shard(0).primaryShard();
                final var currentNodeId = shardRouting.currentNodeId();
                if (currentNodeId != null && shardRouting.started()) {
                    final var node = state.nodes().get(currentNodeId);
                    return expectedNodes.contains(node.getName());
                }
            }
            return false;
        });
    }

    /**
     * Data structure to hold the results of {@link #createNodes}, lists of which nodes return what {@link AllocationDecider#canAllocate}
     * {@link Decision}.
     */
    protected record CreatedNodes(Set<String> noNodes, Set<String> notPreferredNodes, Set<String> throttleNodes, Set<String> yesNodes) {}

    protected CreatedNodes createNodes(int noNodes, int notPreferredNodes, int throttleNodes, int yesNodes) {
        final var preExistingNodeCount = internalCluster().size();
        final var noNodeNames = new HashSet<String>(noNodes);
        final var notPreferredNodeNames = new HashSet<String>(notPreferredNodes);
        final var throttleNodeNames = new HashSet<String>(throttleNodes);
        final var yesNodeNames = new HashSet<String>(yesNodes);
        final int totalNodes = notPreferredNodes + noNodes + throttleNodes + yesNodes;
        final var allNodeNames = new HashSet<>(internalCluster().startNodes(totalNodes));
        allocateNodesAndUpdateSets(yesNodes, allNodeNames, null, yesNodeNames);
        allocateNodesAndUpdateSets(throttleNodes, allNodeNames, CAN_ALLOCATE_THROTTLE_NODE_IDS, throttleNodeNames);
        allocateNodesAndUpdateSets(notPreferredNodes, allNodeNames, CAN_ALLOCATE_NOT_PREFERRED_NODE_IDS, notPreferredNodeNames);
        allocateNodesAndUpdateSets(noNodes, allNodeNames, CAN_ALLOCATE_NO_IDS, noNodeNames);
        assert allNodeNames.isEmpty() : "all nodes should have been used: " + allNodeNames;
        ensureStableCluster(preExistingNodeCount + totalNodes);
        final var createdNodes = new CreatedNodes(noNodeNames, notPreferredNodeNames, throttleNodeNames, yesNodeNames);
        logger.info("--> created nodes {}", createdNodes);
        return createdNodes;
    }

    private static void allocateNodesAndUpdateSets(
        int nodeCount,
        Set<String> allNodeNames,
        @Nullable Set<String> deciderSet,
        Set<String> nodeNameSet
    ) {
        for (int i = 0; i < nodeCount; i++) {
            final var nodeName = randomFrom(allNodeNames);
            allNodeNames.remove(nodeName);
            if (deciderSet != null) {
                deciderSet.add(getNodeId(nodeName));
            }
            nodeNameSet.add(nodeName);
        }
    }

    public static class TestAllocationPlugin extends Plugin implements ClusterPlugin {

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new TestAllocationDecider());
        }
    }

    public static class TestAllocationDecider extends AllocationDecider {

        /**
         * These tests aren't about rebalancing, disable it so it doesn't interfere with the results
         */
        @Override
        public Decision canRebalance(RoutingAllocation allocation) {
            return Decision.NO;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (CAN_ALLOCATE_NO_IDS.contains(node.nodeId())) {
                return Decision.NO;
            }
            if (CAN_ALLOCATE_NOT_PREFERRED_NODE_IDS.contains(node.nodeId())) {
                return Decision.NOT_PREFERRED;
            }
            if (CAN_ALLOCATE_THROTTLE_NODE_IDS.contains(node.nodeId()) && allocation.isSimulating() == false) {
                return Decision.THROTTLE;
            }
            return Decision.YES;
        }

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (CAN_REMAIN_NO_NODE_IDS.contains(node.nodeId())) {
                return Decision.NO;
            } else if (CAN_REMAIN_NOT_PREFERRED_NODE_IDS.contains(node.nodeId())) {
                return Decision.NOT_PREFERRED;
            }
            return Decision.YES;
        }
    }
}
