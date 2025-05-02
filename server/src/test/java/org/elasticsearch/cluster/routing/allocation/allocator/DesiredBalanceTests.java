/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance.shardMovementsBecauseOfShutdown;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DesiredBalanceTests extends ESTestCase {

    /** Tests {@code DesiredBalance#shardMovements(ShardAssignment, ShardAssignment)}. */
    public void testShardMovements() {

        assertThat(
            "no shard movements when no changes",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a", "b"), 2, 0, 0)),
            equalTo(0)
        );

        assertThat(
            "no shard movements when starting new shard copy",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a", "b", "c"), 3, 0, 0)),
            equalTo(0)
        );

        assertThat(
            "no shard movements when starting unassigned shard",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 3, 1, 0), new ShardAssignment(Set.of("a", "b", "c"), 3, 0, 0)),
            equalTo(0)
        );

        assertThat(
            "no shard movements when new shard copy is unassigned",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a", "b"), 3, 1, 0)),
            equalTo(0)
        );

        assertThat(
            "1 shard movements when an existing shard copy is moved and new shard copy is unassigned",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a", "c"), 3, 1, 0)),
            equalTo(1)
        );

        assertThat(
            "1 shard movement when an existing shard copy is moved",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a", "c"), 2, 0, 0)),
            equalTo(1)
        );

        assertThat(
            "2 shard movements when both shard copies are move to new nodes",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("c", "d"), 2, 0, 0)),
            equalTo(2)
        );

        assertThat(
            "no shard movements when shard become unassigned",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a"), 2, 1, 0)),
            equalTo(0)
        );

        assertThat(
            "no shard movements when removing shard copy",
            shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a"), 1, 0, 0)),
            equalTo(0)
        );
    }

    /** Tests {@code DesiredBalance#shardMovementsBecauseOfShutdown(ShardAssignment, ShardAssignment, Set)}. */
    public void testShardMovementsForShutdown() {
        /**
         * Test shard assignment changes that _should not_ count as shutdown induced.
         */
        assertThat(
            "The node shutting down does not possess any of these shards",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b"), 2, 0, 0),
                new ShardAssignment(Set.of("a", "d"), 2, 0, 0),
                new HashSet<>(Arrays.asList("c"))
            ),
            equalTo(0)
        );
        assertThat(
            "Adding a shard should return 0 shutdown moves",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b"), 2, 0, 0),
                new ShardAssignment(Set.of("a", "b", "c"), 3, 0, 0),
                new HashSet<>()
            ),
            equalTo(0)
        );
        assertThat(
            "A shard remaining on a shutting down node shouldn't count",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b"), 3, 1, 0),
                new ShardAssignment(Set.of("a", "b", "c"), 3, 0, 0),
                new HashSet<>(Arrays.asList("a"))
            ),
            equalTo(0)
        );
        assertThat(
            "A shard being unassigned should not be counted when the node is not shutting down",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b"), 2, 0, 0),
                new ShardAssignment(Set.of("a"), 2, 1, 0),
                new HashSet<>(Arrays.asList("c", "d", "e"))
            ),
            equalTo(0)
        );
        assertThat(
            "Doesn't count for a shutdown shard movement if a replica is removed instead of reassigned",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b"), 2, 0, 0),
                new ShardAssignment(Set.of("a"), 1, 0, 0),
                new HashSet<>(Arrays.asList("b"))
            ),
            equalTo(0)
        );
        assertThat(
            "One shard becomes unassigned due to shutdown",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b"), 2, 0, 0),
                new ShardAssignment(Set.of("a"), 2, 1, 0),
                new HashSet<>(Arrays.asList("b"))
            ),
            equalTo(0)
        );
        assertThat(
            "One shard becomes unassigned due to shutdown",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b", "c"), 3, 0, 0),
                new ShardAssignment(Set.of("b", "d"), 2, 0, 0),
                new HashSet<>(Arrays.asList("a"))
            ),
            equalTo(0)
        );

        /**
         * Test shard assignment changes that _should_ count as shutdown induced.
         */
        assertThat(
            "Both shards move because of shutdown",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b"), 2, 0, 0),
                new ShardAssignment(Set.of("c", "d"), 3, 1, 0),
                new HashSet<>(Arrays.asList("a", "b"))
            ),
            equalTo(2)
        );
        assertThat(
            "One shard moves because of shutdown, the other relocates",
            shardMovementsBecauseOfShutdown(
                new ShardAssignment(Set.of("a", "b"), 2, 0, 0),
                new ShardAssignment(Set.of("c", "d"), 2, 0, 0),
                new HashSet<>(Arrays.asList("b"))
            ),
            equalTo(1)
        );
    }

    /**
     * Tests {@code DesiredBalance.shardAssignmentChanges}. Sets up two DesiredBalances and tests that all the shard assignment changes
     * ({@link DesiredBalance.ShardMoveCounts}) are observed correctly.
     */
    public void testShardAssignmentChanges() {
        var oldAssignments = new HashMap<ShardId, ShardAssignment>();
        var updateAssignments = new HashMap<ShardId, ShardAssignment>();
        Set<String> shuttingDownNodeIds = new HashSet<>();

        // one shard move, no shutdown -- +1 move
        oldAssignments.put(new ShardId("indexName", "indexUuid", 0), new ShardAssignment(new HashSet<>(Arrays.asList("a", "b")), 2, 0, 0));
        updateAssignments.put(
            new ShardId("indexName", "indexUuid", 0),
            new ShardAssignment(new HashSet<>(Arrays.asList("b", "c")), 2, 0, 0)
        );

        // assign a shard copy for an existing shard -- +1 assign
        oldAssignments.put(new ShardId("indexName", "indexUuid", 1), new ShardAssignment(new HashSet<>(Arrays.asList("b", "c")), 3, 1, 0));
        updateAssignments.put(
            new ShardId("indexName", "indexUuid", 1),
            new ShardAssignment(new HashSet<>(Arrays.asList("b", "c", "d")), 3, 0, 0)
        );

        // unassign a shard copy for an existing shard -- +1 unassign
        oldAssignments.put(new ShardId("indexName", "indexUuid", 2), new ShardAssignment(new HashSet<>(Arrays.asList("a", "b")), 2, 0, 0));
        updateAssignments.put(new ShardId("indexName", "indexUuid", 2), new ShardAssignment(new HashSet<>(Arrays.asList("b")), 2, 1, 0));

        // remove (unassign) a shard (3 copies) -- +3 unassign
        oldAssignments.put(
            new ShardId("indexName", "indexUuid", 3),
            new ShardAssignment(new HashSet<>(Arrays.asList("b", "c", "d")), 3, 0, 0)
        );

        // reassign shards (2) on shutting down node -- this counts as regular shard movement, too -- +2 shutdown move, +2 move
        shuttingDownNodeIds.add("e");
        oldAssignments.put(
            new ShardId("indexName", "indexUuid", 4),
            new ShardAssignment(new HashSet<>(Arrays.asList("b", "c", "e")), 3, 0, 0)
        );
        updateAssignments.put(
            new ShardId("indexName", "indexUuid", 4),
            new ShardAssignment(new HashSet<>(Arrays.asList("b", "c", "a")), 3, 0, 0)
        );
        oldAssignments.put(
            new ShardId("indexName", "indexUuid", 5),
            new ShardAssignment(new HashSet<>(Arrays.asList("c", "e", "a")), 3, 0, 0)
        );
        updateAssignments.put(
            new ShardId("indexName", "indexUuid", 5),
            new ShardAssignment(new HashSet<>(Arrays.asList("c", "d", "a")), 3, 0, 0)
        );

        // create (assign) a new shard -- +1 assign
        updateAssignments.put(new ShardId("indexName", "indexUuid", 6), new ShardAssignment(new HashSet<>(Arrays.asList("d")), 1, 0, 0));

        // Must create the DiscoveryNodes here to pass into both instances of DesiredBalance, so that taking the set difference works.
        var mockNodeA = mock(DiscoveryNode.class);
        when(mockNodeA.getId()).thenReturn("a");
        var mockNodeB = mock(DiscoveryNode.class);
        when(mockNodeB.getId()).thenReturn("b");
        var mockNodeC = mock(DiscoveryNode.class);
        when(mockNodeC.getId()).thenReturn("c");
        var mockNodeD = mock(DiscoveryNode.class);
        when(mockNodeD.getId()).thenReturn("d");
        var mockNodeE = mock(DiscoveryNode.class);
        when(mockNodeE.getId()).thenReturn("e");
        var oldNodes = new HashSet<>(Arrays.asList(mockNodeA, mockNodeB, mockNodeC, mockNodeD, mockNodeE));
        var updatedNodes = new HashSet<>(Arrays.asList(mockNodeA, mockNodeB, mockNodeC, mockNodeD));

        var assignmentChanges = DesiredBalance.shardAssignmentChanges(
            createDesiredBalanceWithAssignments(oldAssignments, oldNodes),
            createDesiredBalanceWithAssignments(updateAssignments, updatedNodes)
        );

        assertThat("", assignmentChanges.movements(), equalTo(3));
        assertThat("", assignmentChanges.newlyAssigned(), equalTo(2));
        assertThat("", assignmentChanges.newlyUnassigned(), equalTo(4));
        assertThat("", assignmentChanges.shutdownInducedMoves(), equalTo(2));
    }

    /**
     * Sets up a {@link DesiredBalance} with sufficient information to run in {@link DesiredBalance#shardAssignmentChanges}.
     */
    private static DesiredBalance createDesiredBalanceWithAssignments(Map<ShardId, ShardAssignment> assignments, Set<DiscoveryNode> nodes) {
        var nodeWeights = new HashMap<DiscoveryNode, DesiredBalanceMetrics.NodeWeightStats>();
        for (var node : nodes) {
            nodeWeights.put(node, DesiredBalanceMetrics.NodeWeightStats.ZERO);
        }
        return new DesiredBalance(1, assignments, nodeWeights, DesiredBalance.ComputationFinishReason.CONVERGED);
    }

    private static int shardMovements(ShardAssignment old, ShardAssignment updated) {
        return DesiredBalance.shardMovements(createDesiredBalanceWith(old), createDesiredBalanceWith(updated));
    }

    private static DesiredBalance createDesiredBalanceWith(ShardAssignment assignment) {
        return new DesiredBalance(1, Map.of(new ShardId("index", "_na_", 0), assignment));
    }
}
