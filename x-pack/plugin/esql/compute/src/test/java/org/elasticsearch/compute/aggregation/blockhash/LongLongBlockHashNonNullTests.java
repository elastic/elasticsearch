/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests {@link LongLongBlockHash} for groups that the planner proved to have no null.
 */
public class LongLongBlockHashNonNullTests extends ComputeTestCase {
    private static final GroupingAggregatorFunction.AddInput IGNORE_IDS = new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {}

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {}

        @Override
        public void add(int positionOffset, IntVector groupIds) {}

        @Override
        public void close() {}
    };

    private static List<BlockHash.GroupSpec> specs(boolean nonNull) {
        return List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG).withNonNull(nonNull),
            new BlockHash.GroupSpec(1, ElementType.LONG).withNonNull(nonNull)
        );
    }

    /** Only groups that the planner proved to have no null get this hash in production. */
    public void testBuildPicksItForTwoLongsWithNoNull() {
        BlockFactory blockFactory = blockFactory();
        try (BlockHash hash = BlockHash.build(specs(true), blockFactory, 1024, false)) {
            assertThat(hash, instanceOf(LongLongBlockHash.class));
        }
        try (BlockHash hash = BlockHash.build(specs(false), blockFactory, 1024, false)) {
            assertThat(hash, instanceOf(PackedValuesBlockHash.class));
        }
    }

    public void testKeysEqualTheOnesOfPackedValues() {
        BlockFactory blockFactory = blockFactory();
        List<Page> pages = new ArrayList<>();
        try (
            LongLongBlockHash hash = new LongLongBlockHash(blockFactory, 0, 1, randomFrom(100, 1_000), true);
            PackedValuesBlockHash packed = new PackedValuesBlockHash(specs(false), blockFactory, randomFrom(100, 1_000))
        ) {
            for (int p = 0; p < between(1, 10); p++) {
                int positions = between(1, 500);
                Block[] blocks = new Block[2];
                for (int g = 0; g < 2; g++) {
                    boolean vector = randomBoolean();
                    blocks[g] = RandomBlock.randomBlock(
                        blockFactory,
                        ElementType.LONG,
                        positions,
                        false,
                        1,
                        vector ? 1 : between(1, 3),
                        0,
                        0
                    ).block();
                }
                Page page = new Page(blocks);
                pages.add(page);
                hash.add(page, IGNORE_IDS);
                packed.add(page, IGNORE_IDS);
            }
            assertThat(keys(hash), equalTo(keys(packed)));
            assertThat(hash.numKeys(), equalTo(packed.numKeys()));
        } finally {
            Releasables.close(pages);
        }
    }

    /** Vectors longer than a batch go in more than one batch. */
    public void testManyKeysInBatches() {
        BlockFactory blockFactory = blockFactory();
        try (
            LongLongBlockHash hash = new LongLongBlockHash(blockFactory, 0, 1, 256, true);
            PackedValuesBlockHash packed = new PackedValuesBlockHash(specs(false), blockFactory, 256)
        ) {
            int positions = between(1_000, 5_000);
            long[] a = new long[positions];
            long[] b = new long[positions];
            for (int i = 0; i < positions; i++) {
                a[i] = randomLong();
                b[i] = randomLongBetween(0, 3);
            }
            try (
                Page page = new Page(
                    blockFactory.newLongArrayVector(a, positions).asBlock(),
                    blockFactory.newLongArrayVector(b, positions).asBlock()
                )
            ) {
                hash.add(page, IGNORE_IDS);
                packed.add(page, IGNORE_IDS);
            }
            assertThat(hash.numKeys(), equalTo(packed.numKeys()));
            assertThat(keys(hash), equalTo(keys(packed)));
        }
    }

    /** A null means that the planner was wrong, so it must not become a null group id that the aggregations skip. */
    public void testNullThrows() {
        BlockFactory blockFactory = blockFactory();
        try (
            LongLongBlockHash hash = new LongLongBlockHash(blockFactory, 0, 1, 1024, true);
            LongBlock.Builder longs = blockFactory.newLongBlockBuilder(2)
        ) {
            longs.appendLong(1);
            longs.appendNull();
            try (Page page = new Page(longs.build(), blockFactory.newLongArrayVector(new long[] { 5, 6 }, 2).asBlock())) {
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> hash.add(page, IGNORE_IDS));
                assertThat(e.getMessage(), containsString("a group has a null, but the planner said that no group does"));
            }
        }
    }

    private static Set<List<Long>> keys(BlockHash hash) {
        Set<List<Long>> keys = new HashSet<>();
        try (IntVector all = hash.nonEmpty()) {
            Block[] blocks = hash.getKeys(all);
            try {
                LongBlock first = (LongBlock) blocks[0];
                LongBlock second = (LongBlock) blocks[1];
                for (int p = 0; p < first.getPositionCount(); p++) {
                    keys.add(List.of(first.getLong(first.getFirstValueIndex(p)), second.getLong(second.getFirstValueIndex(p))));
                }
            } finally {
                Releasables.close(blocks);
            }
        }
        return keys;
    }
}
