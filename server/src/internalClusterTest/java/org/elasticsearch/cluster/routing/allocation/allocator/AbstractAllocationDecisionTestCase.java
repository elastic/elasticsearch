/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
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

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public abstract class AbstractAllocationDecisionTestCase extends ESIntegTestCase {

    protected static final Set<String> NOT_PREFERRED_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected static final Set<String> THROTTLED_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected static final Set<String> NO_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Before
    public final void clearCanAllocateDeciderState() {
        NOT_PREFERRED_NODES.clear();
        THROTTLED_NODES.clear();
        NO_NODES.clear();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestAllocationPlugin.class);
    }

    protected record CreatedNodes(Set<String> noNodes, Set<String> notPreferredNodes, Set<String> throttleNodes, Set<String> yesNodes) {}

    protected CreatedNodes createNodes(int noNodes, int notPreferredNodes, int throttleNodes, int yesNodes) {
        final var preExistingNodeCount = internalCluster().size();
        final var noNodeNames = new HashSet<String>(noNodes);
        final var notPreferredNodeNames = new HashSet<String>(notPreferredNodes);
        final var throttleNodeNames = new HashSet<String>(throttleNodes);
        final var yesNodeNames = new HashSet<String>(yesNodes);
        final int totalNodes = notPreferredNodes + noNodes + throttleNodes + yesNodes;
        final var allNodeNames = new HashSet<>(internalCluster().startNodes(totalNodes));
        for (int i = 0; i < yesNodes; i++) {
            final var yesNode = randomFrom(allNodeNames);
            allNodeNames.remove(yesNode);
            yesNodeNames.add(yesNode);
        }
        allocateNodesAndUpdateSets(throttleNodes, allNodeNames, THROTTLED_NODES, throttleNodeNames);
        allocateNodesAndUpdateSets(notPreferredNodes, allNodeNames, NOT_PREFERRED_NODES, notPreferredNodeNames);
        allocateNodesAndUpdateSets(noNodes, allNodeNames, NO_NODES, noNodeNames);
        assert allNodeNames.isEmpty() : "all nodes should have been used: " + allNodeNames;
        ensureStableCluster(preExistingNodeCount + totalNodes);
        final var createdNodes = new CreatedNodes(noNodeNames, notPreferredNodeNames, throttleNodeNames, yesNodeNames);
        logger.info("--> created nodes {}", createdNodes);
        return createdNodes;
    }

    private static void allocateNodesAndUpdateSets(
        int noNodes,
        Set<String> allNodeNames,
        Set<String> deciderSet,
        HashSet<String> nodeNameSet
    ) {
        for (int i = 0; i < noNodes; i++) {
            final var nodeName = randomFrom(allNodeNames);
            allNodeNames.remove(nodeName);
            deciderSet.add(getNodeId(nodeName));
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
            if (NO_NODES.contains(node.nodeId())) {
                return Decision.NO;
            }
            if (NOT_PREFERRED_NODES.contains(node.nodeId())) {
                return Decision.NOT_PREFERRED;
            }
            if (THROTTLED_NODES.contains(node.nodeId()) && allocation.isSimulating() == false) {
                return Decision.THROTTLE;
            }
            return Decision.YES;
        }
    }
}
