/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThrottlingAllocationDeciderTests extends ESAllocationTestCase {

    private record TestHarness(
        ClusterState clusterState,
        RoutingNodes mutableRoutingNodes,
        RoutingNode mutableRoutingNode1,
        RoutingNode mutableRoutingNode2,
        ShardRouting shardRouting1Primary,
        ShardRouting shardRouting1Replica,
        ShardRouting shardRouting2Primary,
        ShardRouting shardRouting2Replica
    ) {}

    private TestHarness setUpTwoNodesAndIndexWithTwoUnassignedPrimariesAndReplicas() {
        int numberOfShards = 2;
        ClusterState clusterState = ClusterStateCreationUtils.stateWithUnassignedPrimariesAndReplicas(
            new String[] { "test-index" },
            numberOfShards,
            1
        );
        // The number of data nodes the util method above creates is numberOfReplicas+2.
        assertEquals(3, clusterState.nodes().size());
        assertEquals(1, clusterState.metadata().getTotalNumberOfIndices());

        var indexIterator = clusterState.metadata().indicesAllProjects().iterator();
        assertTrue(indexIterator.hasNext());
        IndexMetadata testIndexMetadata = indexIterator.next();
        assertFalse(indexIterator.hasNext());
        Index testIndex = testIndexMetadata.getIndex();
        assertEquals(numberOfShards, testIndexMetadata.getNumberOfShards());
        ShardId testShardId1 = new ShardId(testIndex, 0);
        ShardId testShardId2 = new ShardId(testIndex, 1);

        var mutableRoutingNodes = clusterState.mutableRoutingNodes();

        // The RoutingNode references must be to the RoutingAllocation's RoutingNodes, so that changes to one is reflected in the other.
        var routingNodesIterator = mutableRoutingNodes.iterator();
        assertTrue(routingNodesIterator.hasNext());
        var mutableRoutingNode1 = routingNodesIterator.next();
        assertTrue(routingNodesIterator.hasNext());
        var mutableRoutingNode2 = routingNodesIterator.next();
        assertTrue(routingNodesIterator.hasNext());
        routingNodesIterator.next(); // unused third node.
        assertFalse(routingNodesIterator.hasNext());

        ShardRouting shardRouting1Primary = TestShardRouting.newShardRouting(testShardId1, null, null, true, ShardRoutingState.UNASSIGNED);
        ShardRouting shardRouting2Primary = TestShardRouting.newShardRouting(testShardId2, null, null, true, ShardRoutingState.UNASSIGNED);
        ShardRouting shardRouting1Replica = TestShardRouting.newShardRouting(testShardId1, null, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting shardRouting2Replica = TestShardRouting.newShardRouting(testShardId2, null, null, false, ShardRoutingState.UNASSIGNED);

        return new TestHarness(
            clusterState,
            mutableRoutingNodes,
            mutableRoutingNode1,
            mutableRoutingNode2,
            shardRouting1Primary,
            shardRouting1Replica,
            shardRouting2Primary,
            shardRouting2Replica
        );
    }

    public void testPrimaryAndReplicaThrottlingNotSimulation() {
        /* Create cluster state for multiple nodes and an index with _unassigned_ shards. */
        TestHarness harness = setUpTwoNodesAndIndexWithTwoUnassignedPrimariesAndReplicas();

        /* Decider Testing */

        // Set up RoutingAllocation in non-simulation mode.
        var routingAllocation = new RoutingAllocation(
            null,
            harness.mutableRoutingNodes,
            harness.clusterState,
            ClusterInfo.builder().build(),
            null,
            System.nanoTime(),
            false // Turn off isSimulating
        );

        final RoutingChangesObserver NOOP = new RoutingChangesObserver() {
        };
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.unthrottle_replica_assignment_in_simulation", randomBoolean() ? true : false)
            .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 1)
            .build();
        assertFalse(routingAllocation.isSimulating());
        ThrottlingAllocationDecider decider = new ThrottlingAllocationDecider(ClusterSettings.createBuiltInClusterSettings(settings));

        // A single primary can be allocated.
        assertThat(
            decider.canAllocate(harness.shardRouting1Primary, harness.mutableRoutingNode1, routingAllocation),
            equalTo(Decision.YES)
        );
        var shardRouting1PrimaryInitializing = harness.mutableRoutingNodes.initializeShard(
            harness.shardRouting1Primary,
            harness.mutableRoutingNode1.nodeId(),
            null,
            0,
            NOOP
        );

        // Leaving the first shard's primary in an INITIALIZING state should THROTTLE further allocation.
        // Only 1 concurrent allocation is allowed.
        assertThat(
            decider.canAllocate(harness.shardRouting2Primary, harness.mutableRoutingNode1, routingAllocation),
            equalTo(Decision.THROTTLE)
        );

        // The first shard's replica should receive a simple NO because the corresponding primary is not active yet.
        assertThat(decider.canAllocate(harness.shardRouting1Replica, harness.mutableRoutingNode2, routingAllocation), equalTo(Decision.NO));

        // Start the first shard's primary, and initialize the second shard's primary to again reach the 1 concurrency limit.
        harness.mutableRoutingNodes.startShard(shardRouting1PrimaryInitializing, NOOP, 0);
        assertThat(
            decider.canAllocate(harness.shardRouting2Primary, harness.mutableRoutingNode2, routingAllocation),
            equalTo(Decision.YES)
        );
        harness.mutableRoutingNodes.initializeShard(harness.shardRouting2Primary, harness.mutableRoutingNode2.nodeId(), null, 0, NOOP);

        // The first shard's replica should receive THROTTLE now, since the primary is active.
        // There is still already 1 allocation in progress, which is the limit.
        assertThat(
            decider.canAllocate(harness.shardRouting1Replica, harness.mutableRoutingNode2, routingAllocation),
            equalTo(Decision.THROTTLE)
        );
    }

    public void testPrimaryAndReplicaThrottlingInSimulation() {
        /* Create cluster state for multiple nodes and an index with _unassigned_ shards. */
        TestHarness harness = setUpTwoNodesAndIndexWithTwoUnassignedPrimariesAndReplicas();
        var mutableRoutingNodes = harness.clusterState.mutableRoutingNodes();

        /* Decider Testing */

        // Set up RoutingAllocation in simulation mode.
        var routingAllocation = new RoutingAllocation(
            null,
            mutableRoutingNodes,
            harness.clusterState,
            ClusterInfo.builder().build(),
            null,
            System.nanoTime(),
            true // Turn on isSimulating
        );

        final RoutingChangesObserver NOOP = new RoutingChangesObserver() {
        };
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.unthrottle_replica_assignment_in_simulation", true)
            .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 1)
            .build();
        assertTrue(routingAllocation.isSimulating());
        ThrottlingAllocationDecider decider = new ThrottlingAllocationDecider(ClusterSettings.createBuiltInClusterSettings(settings));

        // Primary path is unthrottled during simulation, regardless of the `node_initial_primaries_recoveries` setting
        assertThat(
            decider.canAllocate(harness.shardRouting1Primary, harness.mutableRoutingNode1, routingAllocation),
            equalTo(Decision.YES)
        );
        mutableRoutingNodes.initializeShard(harness.shardRouting1Primary, harness.mutableRoutingNode1.nodeId(), null, 0, NOOP);
        assertThat(
            decider.canAllocate(harness.shardRouting2Primary, harness.mutableRoutingNode1, routingAllocation),
            equalTo(Decision.YES)
        );
        mutableRoutingNodes.initializeShard(harness.shardRouting2Primary, harness.mutableRoutingNode1.nodeId(), null, 0, NOOP);

        // Replica path is unthrottled during simulation AND `unthrottle_replica_assignment_in_simulation` is set to true.
        assertThat(
            decider.canAllocate(harness.shardRouting1Replica, harness.mutableRoutingNode2, routingAllocation),
            equalTo(Decision.YES)
        );
        mutableRoutingNodes.initializeShard(harness.shardRouting1Replica, harness.mutableRoutingNode2.nodeId(), null, 0, NOOP);
        assertThat(
            decider.canAllocate(harness.shardRouting2Replica, harness.mutableRoutingNode2, routingAllocation),
            equalTo(Decision.YES)
        );
        mutableRoutingNodes.initializeShard(harness.shardRouting2Replica, harness.mutableRoutingNode2.nodeId(), null, 0, NOOP);

        // Note: INITIALIZING was chosen above, not STARTED, because the BalancedShardsAllocator only initializes. We want that path to be
        // unthrottled in simulation.
    }
}
