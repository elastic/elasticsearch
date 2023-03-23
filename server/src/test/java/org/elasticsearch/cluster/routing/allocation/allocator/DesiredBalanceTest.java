/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DesiredBalanceTest extends ESTestCase {

    public void testshardMovements() {

        assertThat(
            "no shard movements when no changes",
            DesiredBalance.shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a", "b"), 2, 0, 0)),
            equalTo(0)
        );

        assertThat(
            "no shard movements when starting new shard copy",
            DesiredBalance.shardMovements(
                new ShardAssignment(Set.of("a", "b"), 2, 0, 0),
                new ShardAssignment(Set.of("a", "b", "c"), 3, 0, 0)
            ),
            equalTo(0)
        );

        assertThat(
            "no shard movements when starting unassigned shard",
            DesiredBalance.shardMovements(
                new ShardAssignment(Set.of("a", "b"), 3, 1, 0),
                new ShardAssignment(Set.of("a", "b", "c"), 3, 0, 0)
            ),
            equalTo(0)
        );

        assertThat(
            "no shard movements when new shard copy is unassigned",
            DesiredBalance.shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a", "b"), 3, 1, 0)),
            equalTo(0)
        );

        assertThat(
            "1 shard movement",
            DesiredBalance.shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a", "c"), 2, 0, 0)),
            equalTo(1)
        );

        assertThat(
            "2 shard movement",
            DesiredBalance.shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("c", "d"), 2, 0, 0)),
            equalTo(2)
        );

        assertThat(
            "no shard movements when shard become unassigned",
            DesiredBalance.shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a"), 2, 1, 0)),
            equalTo(0)
        );

        assertThat(
            "no shard movements when removing shard copy",
            DesiredBalance.shardMovements(new ShardAssignment(Set.of("a", "b"), 2, 0, 0), new ShardAssignment(Set.of("a"), 1, 0, 0)),
            equalTo(0)
        );
    }

}
