/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.addTemporaryStateListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AllocateUnassignedIT extends ESIntegTestCase {

    private static final Set<String> NOT_PREFERRED_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Set<String> THROTTLE_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Set<String> NO_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestAllocationPlugin.class);
    }

    @Before
    public void clearTestDeciderState() {
        NOT_PREFERRED_NODES.clear();
        THROTTLE_NODES.clear();
        NO_NODES.clear();
    }

    public void testNewShardsAreAllocatedToPreferredNodesWhenPresent() {
        final var nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 0, randomIntBetween(1, 3));

        createSingleShardAndAssertItIsAssignedToAppropriateNode(nodes.yesNodes());
    }

    public void testNewShardsAreAllocatedToNotPreferredNodesWhenNoThrottleOrYesNodesArePresent() {
        final var nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 0, 0);

        createSingleShardAndAssertItIsAssignedToAppropriateNode(nodes.notPreferredNodes());
    }

    public void testNewShardsAreAllocatedToThrottleNodesWhenNoYesNodesArePresent() {
        final var nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), randomIntBetween(1, 3), 0);

        final var indexName = randomIdentifier();

        ActionFuture<CreateIndexResponse> createIndexFuture = prepareCreate(indexName).setSettings(indexSettings(1, 0)).execute();
        ensureRed(indexName);

        ClusterState state = internalCluster().clusterService().state();
        IndexRoutingTable index = state.routingTable(ProjectId.DEFAULT).index(indexName);

        // No shards should be assigned (because we're waiting for the throttled nodes)
        assertEquals(0, index.shard(0).assignedShards().size());
        assertFalse(createIndexFuture.isDone());

        final var firstAllocationListener = waitForFirstAllocation(indexName);

        // Un-throttle the nodes, re-route should see them allocated to one of the previously throttled nodes
        THROTTLE_NODES.clear();
        ClusterRerouteUtils.reroute(client());

        final var firstAllocatedNode = safeAwait(firstAllocationListener);
        assertThat(firstAllocatedNode.getName(), in(nodes.throttleNodes()));
        safeGet(createIndexFuture);
    }

    public void testNewShardsAreNotAllocatedToNoNodes() {
        createNodes(randomIntBetween(1, 3), 0, 0, 0);

        final var indexName = randomIdentifier();

        // Cluster should go red when only NO exist
        prepareCreate(indexName).setSettings(indexSettings(1, 0)).execute();
        ensureRed(indexName);
    }

    private void createSingleShardAndAssertItIsAssignedToAppropriateNode(Set<String> expectedNodes) {
        final var indexName = randomIdentifier();
        final var firstAllocationListener = waitForFirstAllocation(indexName);

        // The single-shard should be allocated to one of the expected nodes
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        final var firstAllocatedNode = safeAwait(firstAllocationListener);
        final String message = Strings.format(
            "Expected shard to be allocated to one of %s but it was allocated to %s",
            expectedNodes,
            firstAllocatedNode.getName()
        );
        assertTrue(message, expectedNodes.contains(firstAllocatedNode.getName()));
    }

    private SubscribableListener<DiscoveryNode> waitForFirstAllocation(String indexName) {
        final var firstAllocationNode = new AtomicReference<DiscoveryNode>();
        return addTemporaryStateListener(internalCluster().clusterService(), state -> {
            final var indexMetadata = state.projectState(ProjectId.DEFAULT).metadata().index(indexName);
            if (indexMetadata != null) {
                final var maybeRoutingTable = state.globalRoutingTable().indexRouting(ProjectId.DEFAULT, indexMetadata.getIndex());
                if (maybeRoutingTable.isPresent()) {
                    IndexRoutingTable routingTable = maybeRoutingTable.get();
                    assertThat(routingTable.size(), equalTo(1));
                    final var assignedShards = routingTable.shard(0).assignedShards();
                    if (assignedShards.isEmpty() == false) {
                        ShardRouting shardRouting = assignedShards.getFirst();
                        if (shardRouting.started()) {
                            firstAllocationNode.set(state.nodes().get(shardRouting.currentNodeId()));
                            return true;
                        }
                    }
                }
            }
            return false;
        }).andThenApply(v -> firstAllocationNode.get());
    }

    private CreatedNodes createNodes(int noNodes, int notPreferredNodes, int throttleNodes, int yesNodes) {
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
        allocateNodesAndUpdateSets(throttleNodes, allNodeNames, THROTTLE_NODES, throttleNodeNames);
        allocateNodesAndUpdateSets(notPreferredNodes, allNodeNames, NOT_PREFERRED_NODES, notPreferredNodeNames);
        allocateNodesAndUpdateSets(noNodes, allNodeNames, NO_NODES, noNodeNames);
        assert allNodeNames.isEmpty() : "all nodes should have been used: " + allNodeNames;
        ensureStableCluster(totalNodes);
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

    private record CreatedNodes(Set<String> noNodes, Set<String> notPreferredNodes, Set<String> throttleNodes, Set<String> yesNodes) {}

    public static class TestAllocationPlugin extends Plugin implements ClusterPlugin {
        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new TestAllocationDecider());
        }
    }

    public static class TestAllocationDecider extends AllocationDecider {

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (NO_NODES.contains(node.nodeId())) {
                return Decision.NO;
            }
            if (NOT_PREFERRED_NODES.contains(node.nodeId())) {
                return Decision.NOT_PREFERRED;
            }
            if (THROTTLE_NODES.contains(node.nodeId()) && allocation.isSimulating() == false) {
                return Decision.THROTTLE;
            }
            return Decision.YES;
        }
    }
}
