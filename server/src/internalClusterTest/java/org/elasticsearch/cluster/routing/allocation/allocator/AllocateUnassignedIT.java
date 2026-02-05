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
import org.elasticsearch.cluster.metadata.ProjectId;

import java.util.Set;

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

        final var firstAllocationListener = waitForAllocation(indexName);

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
        final var firstAllocationListener = waitForAllocation(indexName);

        // The single-shard should be allocated to one of the expected nodes
        createIndex(indexName, 1, 0);

        final var firstAllocatedNode = safeAwait(firstAllocationListener);
        assertThat(firstAllocatedNode.getName(), in(expectedNodeNames));
    }
}
