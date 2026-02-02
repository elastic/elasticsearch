/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
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
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MoveShardsIT extends AbstractAllocationDecisionTestCase {

    private static final Set<String> CAN_REMAIN_NO_NODE_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Set<String> CAN_REMAIN_NOT_PREFERRED_NODE_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestCanRemainPlugin.class);
    }

    @Before
    public final void clearCanRemainDeciderState() {
        CAN_REMAIN_NO_NODE_IDS.clear();
        CAN_REMAIN_NOT_PREFERRED_NODE_IDS.clear();
    }

    public void testShardsWillBeMovedToYesNodesWhenPresent() {
        final var initialNode = internalCluster().startNode();
        final var indexName = randomIdentifier();

        // Create index, ensure it's allocated to the initial node
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        CreatedNodes nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 0, randomIntBetween(1, 3));

        randomFrom(CAN_REMAIN_NO_NODE_IDS, CAN_REMAIN_NOT_PREFERRED_NODE_IDS).add(getNodeId(initialNode));
        ClusterRerouteUtils.reroute(client());

        ensureShardAllocatedToAppropriateNode(indexName, nodes.yesNodes());
    }

    public void testShardsWillBeMovedToThrottleNodesWhenNoYesNodesArePresent() {
        final var initialNode = internalCluster().startNode();
        final var indexName = randomIdentifier();

        // Create index, ensure it's allocated to the initial node
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        CreatedNodes nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), randomIntBetween(1, 3), 0);

        randomFrom(CAN_REMAIN_NO_NODE_IDS, CAN_REMAIN_NOT_PREFERRED_NODE_IDS).add(getNodeId(initialNode));
        ClusterRerouteUtils.reroute(client());

        // The shard shouldn't have moved because it's waiting for the throttle to clear
        final var state = clusterService().state();
        assertEquals(
            getNodeId(initialNode),
            state.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
        );

        // Clear the throttle, reroute should result in shard being allocated to the previously throttled node
        CAN_ALLOCATE_THROTTLE_NODE_IDS.clear();
        ClusterRerouteUtils.reroute(client());

        ensureShardAllocatedToAppropriateNode(indexName, nodes.throttleNodes());
    }

    public void testShardsWillBeMovedToNotPreferredNodesWhenCanRemainIsNoAndThereAreNoYesOrThrottleNodes() {
        final var initialNode = internalCluster().startNode();
        final var indexName = randomIdentifier();

        // Create index, ensure it's allocated to the initial node
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        CreatedNodes nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 0, 0);

        CAN_REMAIN_NO_NODE_IDS.add(getNodeId(initialNode));
        ClusterRerouteUtils.reroute(client());

        ensureShardAllocatedToAppropriateNode(indexName, nodes.notPreferredNodes());
    }

    public void testShardsWillNotBeMovedToNotPreferredNodesWhenCanRemainIsNotPreferred() {
        final var initialNode = internalCluster().startNode();
        final var indexName = randomIdentifier();

        // Create index, ensure it's allocated to the initial node
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        CreatedNodes nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 0, 0);

        CAN_REMAIN_NOT_PREFERRED_NODE_IDS.add(getNodeId(initialNode));
        ClusterRerouteUtils.reroute(client());

        // The shard shouldn't have moved because we don't take a not_preferred over a not_preferred
        final var state = clusterService().state();
        assertEquals(
            getNodeId(initialNode),
            state.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
        );

        // Clear the not-preferred flag, reroute should result in shard being moved
        CAN_ALLOCATE_NOT_PREFERRED_NODE_IDS.clear();
        ClusterRerouteUtils.reroute(client());

        ensureShardAllocatedToAppropriateNode(indexName, nodes.notPreferredNodes());
    }

    private void ensureShardAllocatedToAppropriateNode(String indexName, Set<String> expectedNodes) {
        awaitClusterState(state -> {
            final var index = state.routingTable(ProjectId.DEFAULT).index(indexName);
            if (index != null) {
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

    public static class TestCanRemainPlugin extends Plugin implements ClusterPlugin {

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new TestCanRemainDecider());
        }
    }

    public static class TestCanRemainDecider extends AllocationDecider {

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
