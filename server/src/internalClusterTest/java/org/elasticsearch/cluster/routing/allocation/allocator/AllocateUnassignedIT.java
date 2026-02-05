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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.addTemporaryStateListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

public class AllocateUnassignedIT extends AbstractAllocationDecisionTestCase {

    public void testNewShardsAreAllocatedToYesNodesWhenPresent() {
        final var nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 0, randomIntBetween(1, 3));

        createSingleShardAndAssertItIsAssignedToNodes(nodes.yesNodes());
    }

    public void testNewShardsAreAllocatedToThrottleNodesWhenNoYesNodesArePresent() {
        final var nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), randomIntBetween(1, 3), 0);
        final var indexName = randomIdentifier();

        final var createIndexFuture = prepareCreate(indexName).setSettings(indexSettings(1, 0)).execute();
        ensureRed(indexName);

        final var state = internalCluster().clusterService().state();
        final var index = state.routingTable(ProjectId.DEFAULT).index(indexName);

        // No shards should be assigned (because we're waiting for the throttled nodes)
        assertEquals(0, index.shard(0).assignedShards().size());
        assertFalse(createIndexFuture.isDone());

        final var firstAllocationListener = waitForFirstAllocation(indexName);

        // Un-throttle the nodes, re-route should see them allocated to one of the previously throttled nodes
        CAN_ALLOCATE_THROTTLE_NODE_IDS.clear();
        ClusterRerouteUtils.reroute(client());

        final var firstAllocatedNode = safeAwait(firstAllocationListener);
        assertThat(firstAllocatedNode.getName(), in(nodes.throttleNodes()));
        safeGet(createIndexFuture);
    }

    public void testNewShardsAreAllocatedToNotPreferredNodesWhenNoThrottleOrYesNodesArePresent() {
        final var nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 0, 0);

        createSingleShardAndAssertItIsAssignedToNodes(nodes.notPreferredNodes());
    }

    public void testNewShardsAreNotAllocatedToNoNodes() {
        createNodes(randomIntBetween(1, 3), 0, 0, 0);

        final var indexName = randomIdentifier();

        // Cluster should go red when only NO exist
        prepareCreate(indexName).setSettings(indexSettings(1, 0)).execute();
        ensureRed(indexName);
    }

    private void createSingleShardAndAssertItIsAssignedToNodes(Set<String> expectedNodeNames) {
        final var indexName = randomIdentifier();
        final var firstAllocationListener = waitForFirstAllocation(indexName);

        // The single-shard should be allocated to one of the expected nodes
        createIndex(indexName, 1, 0);

        final var firstAllocatedNode = safeAwait(firstAllocationListener);
        assertThat(firstAllocatedNode.getName(), in(expectedNodeNames));
    }

    /**
     * We wait for the first allocation of a shard to ensure we're seeing the first node a shard is
     * assigned to, and that we are testing the {@link BalancedShardsAllocator.Balancer#allocateUnassigned()}
     * behaviour, rather than an assignment followed by a movement.
     *
     * @param indexName The single-shard index to monitor
     * @return A {@link SubscribableListener} that will resolve with the first assigned node
     */
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
}
