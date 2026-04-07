/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DesiredBalanceTests extends ESTestCase {

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

    private static int shardMovements(ShardAssignment old, ShardAssignment updated) {
        return DesiredBalance.shardMovements(createDesiredBalanceWith(old), createDesiredBalanceWith(updated));
    }

    private static DesiredBalance createDesiredBalanceWith(ShardAssignment assignment) {
        return new DesiredBalance(1, Map.of(new ShardId("index", "_na_", 0), assignment));
    }
}
