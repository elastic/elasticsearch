/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class NodeHeapUsageCalculatorTests extends ESTestCase {

    public void testHostedShardsIncludesIndexHeapAndNodeLocalPostings() {
        final ShardId nodeAShard = new ShardId(new Index("index-a", "uuid-a"), 0);
        final ShardId nodeBShard = new ShardId(new Index("index-b", "uuid-b"), 0);
        final long nonShardHeapUsage = 1_000L;

        final var result = NodeHeapUsageCalculator.calculateForShardAllocationMap(
            Map.of("node-a", Set.of(nodeAShard), "node-b", Set.of(nodeBShard)),
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(
                Map.of(nodeAShard, new ShardAndIndexHeapUsage(10L, 100L, 5L), nodeBShard, new ShardAndIndexHeapUsage(20L, 200L, 17L)),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        assertThat(result.maxPostingsHeapUsage(), equalTo(17L));
        assertThat(result.nodeHeapEstimates().get("node-a"), equalTo(new NodeHeapEstimates(1_127L, 115L, nonShardHeapUsage)));
        assertThat(result.nodeHeapEstimates().get("node-b"), equalTo(new NodeHeapEstimates(1_237L, 237L, nonShardHeapUsage)));
    }

    public void testIndexHeapIsCountedOncePerNode() {
        final Index index = new Index("index", "uuid");
        final ShardId shard0 = new ShardId(index, 0);
        final ShardId shard1 = new ShardId(index, 1);
        var nonShardHeapUsage = 50L;

        final var result = NodeHeapUsageCalculator.calculateForSingleNode(
            Set.of(shard0, shard1),
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(
                Map.of(shard0, new ShardAndIndexHeapUsage(10L, 100L, 5L), shard1, new ShardAndIndexHeapUsage(20L, 100L, 7L)),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        assertThat(result.totalHeapUsage(), equalTo(192L));
        assertThat(result.hostedShardsHeapUsage(), equalTo(142L));
        assertThat(result.nonShardHeapUsage(), equalTo(50L));
    }
}
