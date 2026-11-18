/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;

public class ShardTimestampOrderTests extends ESTestCase {

    private static final DataNodeRequest.Shard OLD = shard("old");
    private static final DataNodeRequest.Shard MIDDLE = shard("middle");
    private static final DataNodeRequest.Shard RECENT = shard("recent");

    public void testLargestKeyFirst() {
        Map<ShardId, Long> maxima = Map.of(OLD.shardId(), 100L, MIDDLE.shardId(), 200L, RECENT.shardId(), 400L);
        assertThat(order(maxima, false), contains(RECENT, MIDDLE, OLD));
    }

    public void testSmallestKeyFirstForAscendingSorts() {
        Map<ShardId, Long> minima = Map.of(OLD.shardId(), 0L, MIDDLE.shardId(), 50L, RECENT.shardId(), 300L);
        assertThat(order(minima, true), contains(OLD, MIDDLE, RECENT));
    }

    private static List<DataNodeRequest.Shard> order(Map<ShardId, Long> keys, boolean ascending) {
        return ShardTimestampOrder.order(shuffledList(List.of(OLD, MIDDLE, RECENT)), keys::get, ascending);
    }

    private static DataNodeRequest.Shard shard(String index) {
        return new DataNodeRequest.Shard(new ShardId(index, index + "-uuid", 0), null);
    }
}
