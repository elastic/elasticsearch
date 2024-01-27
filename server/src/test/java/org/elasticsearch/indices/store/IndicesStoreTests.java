/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.store;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;

public class IndicesStoreTests extends ESTestCase {
    private static final ShardRoutingState[] NOT_STARTED_STATES;

    static {
        Set<ShardRoutingState> set = new HashSet<>();
        set.addAll(Arrays.asList(ShardRoutingState.values()));
        set.remove(ShardRoutingState.STARTED);
        NOT_STARTED_STATES = set.toArray(new ShardRoutingState[set.size()]);
    }

    private DiscoveryNode localNode;

    @Before
    public void createLocalNode() {
        localNode = DiscoveryNodeUtils.builder("abc").roles(emptySet()).build();
    }

    public void testShardCanBeDeletedNoShardStarted() {
        final var numShardCopies = randomInt(3);
        final var shardId = new ShardId("test", "_na_", 0);
        final var routingTable = new IndexShardRoutingTable.Builder(shardId);
        final var unStartedShard = randomInt(numShardCopies);
        boolean activePrimary = false;
        for (int j = 0; j <= numShardCopies; j++) {
            ShardRoutingState state;
            if (j == unStartedShard) {
                state = randomFrom(NOT_STARTED_STATES);
            } else {
                state = randomFrom(ShardRoutingState.values());
            }
            if (j == 0) {
                activePrimary = state == ShardRoutingState.STARTED || state == ShardRoutingState.RELOCATING;
            } else if (activePrimary == false) {
                state = ShardRoutingState.UNASSIGNED;
            }
            UnassignedInfo unassignedInfo = null;
            if (state == ShardRoutingState.UNASSIGNED) {
                unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
            }
            String currentNodeId = state == ShardRoutingState.UNASSIGNED ? null : randomAlphaOfLength(10);
            String relocatingNodeId = state == ShardRoutingState.RELOCATING ? randomAlphaOfLength(10) : null;
            routingTable.addShard(
                TestShardRouting.newShardRouting(shardId, currentNodeId, relocatingNodeId, j == 0, state, unassignedInfo)
            );
        }
        assertFalse(IndicesStore.shardCanBeDeleted(localNode.getId(), routingTable.build()));
    }

    public void testShardCanBeDeletedShardExistsLocally() {
        final var numReplicas = randomInt(2);
        final var shardId = new ShardId("test", "_na_", 1);
        final var routingTable = new IndexShardRoutingTable.Builder(shardId);
        final var localNodeIndex = randomInt(numReplicas);
        final var primaryOnLocalNode = localNodeIndex == numReplicas;
        routingTable.addShard(
            TestShardRouting.newShardRouting(
                shardId,
                primaryOnLocalNode ? localNode.getId() : randomAlphaOfLength(10),
                true,
                ShardRoutingState.STARTED
            )
        );
        for (int j = 0; j < numReplicas; j++) {
            final var replicaOnLocalNode = localNodeIndex == j;
            routingTable.addShard(
                TestShardRouting.newShardRouting(
                    shardId,
                    replicaOnLocalNode ? localNode.getId() : randomAlphaOfLength(10),
                    false,
                    ShardRoutingState.STARTED
                )
            );
        }

        // Shard exists locally, can't delete shard
        assertFalse(IndicesStore.shardCanBeDeleted(localNode.getId(), routingTable.build()));
    }
}
