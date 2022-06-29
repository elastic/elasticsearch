/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.randomSpecificRange;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class IndexLongFieldRangeTests extends ESTestCase {

    public void testUnknownShardImpliesUnknownIndex() {
        final IndexLongFieldRange range = randomSpecificRange(false);
        assertThat(
            range.extendWithShardRange(IntStream.of(range.getShards()).max().orElse(0) + 1, between(1, 10), ShardLongFieldRange.UNKNOWN),
            sameInstance(IndexLongFieldRange.UNKNOWN)
        );
    }

    public void testExtendWithKnownShardIsNoOp() {
        IndexLongFieldRange range = randomSpecificRange();
        if (range == IndexLongFieldRange.NO_SHARDS) {
            // need at least one known shard
            range = range.extendWithShardRange(between(0, 5), 5, ShardLongFieldRange.EMPTY);
        }

        final ShardLongFieldRange shardRange;
        if (range.getMinUnsafe() == IndexLongFieldRange.EMPTY.getMinUnsafe()
            && range.getMaxUnsafe() == IndexLongFieldRange.EMPTY.getMaxUnsafe()) {
            shardRange = ShardLongFieldRange.EMPTY;
        } else {
            final long min = randomLongBetween(range.getMinUnsafe(), range.getMaxUnsafe());
            final long max = randomLongBetween(min, range.getMaxUnsafe());
            shardRange = randomBoolean() ? ShardLongFieldRange.EMPTY : ShardLongFieldRange.of(min, max);
        }

        assertThat(
            range.extendWithShardRange(
                range.isComplete() ? between(1, 10) : randomFrom(IntStream.of(range.getShards()).boxed().toList()),
                between(1, 10),
                shardRange
            ),
            sameInstance(range)
        );
    }

    public void testExtendUnknownRangeIsNoOp() {
        assertThat(
            IndexLongFieldRange.UNKNOWN.extendWithShardRange(between(0, 10), between(0, 10), ShardLongFieldRangeWireTests.randomRange()),
            sameInstance(IndexLongFieldRange.UNKNOWN)
        );
    }

    public void testCompleteEmptyRangeIsEmptyInstance() {
        final int shardCount = between(1, 5);
        IndexLongFieldRange range = IndexLongFieldRange.NO_SHARDS;
        for (int i = 0; i < shardCount; i++) {
            assertFalse(range.isComplete());
            range = range.extendWithShardRange(i, shardCount, ShardLongFieldRange.EMPTY);
        }
        assertThat(range, sameInstance(IndexLongFieldRange.EMPTY));
        assertTrue(range.isComplete());
    }

    public void testIsCompleteWhenAllShardRangesIncluded() {
        final int shardCount = between(1, 5);
        IndexLongFieldRange range = IndexLongFieldRange.NO_SHARDS;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (int i = 0; i < shardCount; i++) {
            assertFalse(range.isComplete());
            final ShardLongFieldRange shardFieldRange;
            if (randomBoolean()) {
                shardFieldRange = ShardLongFieldRange.EMPTY;
            } else {
                shardFieldRange = ShardLongFieldRangeWireTests.randomSpecificRange();
                min = Math.min(min, shardFieldRange.getMin());
                max = Math.max(max, shardFieldRange.getMax());
            }
            range = range.extendWithShardRange(i, shardCount, shardFieldRange);
        }
        assertTrue(range.isComplete());
        if (range != IndexLongFieldRange.EMPTY) {
            assertThat(range.getMin(), equalTo(min));
            assertThat(range.getMax(), equalTo(max));
        } else {
            assertThat(min, equalTo(Long.MAX_VALUE));
            assertThat(max, equalTo(Long.MIN_VALUE));
        }
    }

    public void testCanRemoveShardRange() {
        assertThat(IndexLongFieldRange.UNKNOWN.removeShard(between(0, 4), 5), sameInstance(IndexLongFieldRange.UNKNOWN));
        assertThat(IndexLongFieldRange.UNKNOWN.removeShard(0, 1), sameInstance(IndexLongFieldRange.NO_SHARDS));

        final IndexLongFieldRange initialRange = randomSpecificRange();
        final int shardCount = initialRange.isComplete()
            ? between(1, 5)
            : Arrays.stream(initialRange.getShards()).max().orElse(0) + between(1, 3);

        final int shard = between(0, shardCount - 1);
        final IndexLongFieldRange rangeWithoutShard = initialRange.removeShard(shard, shardCount);
        assertFalse(rangeWithoutShard.isComplete());
        assertTrue(Arrays.stream(rangeWithoutShard.getShards()).noneMatch(i -> i == shard));
        if (rangeWithoutShard != IndexLongFieldRange.NO_SHARDS) {
            assertThat(rangeWithoutShard.getMinUnsafe(), equalTo(initialRange.getMinUnsafe()));
            assertThat(rangeWithoutShard.getMaxUnsafe(), equalTo(initialRange.getMaxUnsafe()));
        }
    }
}
