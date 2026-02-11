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

public class MoveShardsIT extends AbstractAllocationDecisionTestCase {

    public void testShardsWillBeMovedToYesNodesWhenPresent() {
        final var initialNode = internalCluster().startNode();
        final var indexName = randomIdentifier();

        // Create index, ensure it's allocated to the initial node
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        CreatedNodes nodes = createNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 0, randomIntBetween(1, 3));

        randomFrom(CAN_REMAIN_NO_NODE_IDS, CAN_REMAIN_NOT_PREFERRED_NODE_IDS).add(getNodeId(initialNode));
        ClusterRerouteUtils.reroute(client());

        ensureShardIsAllocatedToNodes(indexName, nodes.yesNodes());
    }

    public void testShardMovementWillWaitForThrottleNodesWhenNoYesNodesArePresent() {
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

        ensureShardIsAllocatedToNodes(indexName, nodes.throttleNodes());
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

        ensureShardIsAllocatedToNodes(indexName, nodes.notPreferredNodes());
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

        ensureShardIsAllocatedToNodes(indexName, nodes.notPreferredNodes());
    }
}
