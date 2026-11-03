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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
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

public class IntIntBlockHashTests extends ComputeTestCase {
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
            new BlockHash.GroupSpec(0, ElementType.INT).withNonNull(nonNull),
            new BlockHash.GroupSpec(1, ElementType.INT).withNonNull(nonNull)
        );
    }

    public void testPack() {
        for (int[] pair : new int[][] {
            { 0, 0 },
            { 1, -1 },
            { -1, 1 },
            { Integer.MIN_VALUE, Integer.MAX_VALUE },
            { Integer.MAX_VALUE, Integer.MIN_VALUE },
            { randomInt(), randomInt() } }) {
            long key = IntIntBlockHash.pack(pair[0], pair[1]);
            assertThat(IntIntBlockHash.first(key), equalTo(pair[0]));
            assertThat(IntIntBlockHash.second(key), equalTo(pair[1]));
        }
        // The key of a pair is not the key of another, including negative numbers, which would borrow from the high half if added.
        assertNotEquals(IntIntBlockHash.pack(0, -1), IntIntBlockHash.pack(-1, -1));
        assertNotEquals(IntIntBlockHash.pack(1, 0), IntIntBlockHash.pack(0, 1));
    }

    /** Only groups that the planner proved to have no null get this hash. */
    public void testBuildPicksItForTwoIntsWithNoNull() {
        BlockFactory blockFactory = blockFactory();
        try (BlockHash hash = BlockHash.build(specs(true), blockFactory, 1024, false)) {
            assertThat(hash, instanceOf(IntIntBlockHash.class));
        }
        try (BlockHash hash = BlockHash.build(specs(false), blockFactory, 1024, false)) {
            assertThat(hash, instanceOf(PackedValuesBlockHash.class));
        }
        try (
            BlockHash hash = BlockHash.build(
                List.of(new BlockHash.GroupSpec(0, ElementType.INT).withNonNull(true), new BlockHash.GroupSpec(1, ElementType.INT)),
                blockFactory,
                1024,
                false
            )
        ) {
            assertThat(hash, instanceOf(PackedValuesBlockHash.class));
        }
    }

    public void testGroupIdsAndKeys() {
        BlockFactory blockFactory = blockFactory();
        try (
            IntIntBlockHash hash = new IntIntBlockHash(blockFactory, 0, 1, 1024);
            Page page = new Page(
                blockFactory.newIntArrayVector(new int[] { 1, 2, 1, -5, 2 }, 5).asBlock(),
                blockFactory.newIntArrayVector(new int[] { 7, 7, 7, 8, 9 }, 5).asBlock()
            );
            IntVector ids = hash.add(page.<IntBlock>getBlock(0).asVector(), page.<IntBlock>getBlock(1).asVector())
        ) {
            // The group ids are the ordinals of the keys: no id is reserved for null.
            assertThat(ids.getInt(0), equalTo(0));
            assertThat(ids.getInt(1), equalTo(1));
            assertThat(ids.getInt(2), equalTo(0));
            assertThat(ids.getInt(3), equalTo(2));
            assertThat(ids.getInt(4), equalTo(3));
            assertThat(hash.numKeys(), equalTo(4));
            try (IntVector all = hash.nonEmpty()) {
                Block[] keys = hash.getKeys(all);
                try (
                    Block expected1 = blockFactory.newIntArrayVector(new int[] { 1, 2, -5, 2 }, 4).asBlock();
                    Block expected2 = blockFactory.newIntArrayVector(new int[] { 7, 7, 8, 9 }, 4).asBlock()
                ) {
                    assertThat(keys[0], equalTo(expected1));
                    assertThat(keys[1], equalTo(expected2));
                } finally {
                    Releasables.close(keys);
                }
            }
        }
    }

    /** The keys are the same as the ones of the hash for groups that may have nulls, for vectors and for multivalued positions. */
    public void testKeysEqualTheOnesOfPackedValues() {
        BlockFactory blockFactory = blockFactory();
        List<Page> pages = new ArrayList<>();
        try (
            IntIntBlockHash hash = new IntIntBlockHash(blockFactory, 0, 1, randomFrom(100, 1_000));
            PackedValuesBlockHash packed = new PackedValuesBlockHash(specs(false), blockFactory, randomFrom(100, 1_000))
        ) {
            for (int p = 0; p < between(1, 10); p++) {
                int positions = between(1, 500);
                Block[] blocks = new Block[2];
                for (int g = 0; g < 2; g++) {
                    boolean vector = randomBoolean();
                    blocks[g] = RandomBlock.randomBlock(
                        blockFactory,
                        ElementType.INT,
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

    /** Enough distinct keys for the table to use the prefetching path of the vectors. */
    public void testManyKeysUsePrefetch() {
        BlockFactory blockFactory = blockFactory();
        try (
            IntIntBlockHash hash = new IntIntBlockHash(blockFactory, 0, 1, 1024);
            PackedValuesBlockHash packed = new PackedValuesBlockHash(specs(false), blockFactory, 1024)
        ) {
            int positions = 100_000;
            for (int p = 0; p < 4; p++) {
                int[] a = new int[positions];
                int[] b = new int[positions];
                for (int i = 0; i < positions; i++) {
                    a[i] = randomInt();
                    b[i] = randomIntBetween(0, 3);
                }
                try (
                    Page page = new Page(
                        blockFactory.newIntArrayVector(a, positions).asBlock(),
                        blockFactory.newIntArrayVector(b, positions).asBlock()
                    )
                ) {
                    hash.add(page, IGNORE_IDS);
                    packed.add(page, IGNORE_IDS);
                }
            }
            assertThat(hash.numKeys(), equalTo(packed.numKeys()));
            assertThat(keys(hash), equalTo(keys(packed)));
        }
    }

    /** A null means that the planner was wrong, so it must not be hashed as if it were a value. */
    public void testNullThrows() {
        BlockFactory blockFactory = blockFactory();
        try (
            IntIntBlockHash hash = new IntIntBlockHash(blockFactory, 0, 1, 1024);
            IntBlock.Builder ints = blockFactory.newIntBlockBuilder(2)
        ) {
            ints.appendInt(1);
            ints.appendNull();
            try (Page page = new Page(ints.build(), blockFactory.newIntArrayVector(new int[] { 5, 6 }, 2).asBlock())) {
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> hash.add(page, IGNORE_IDS));
                assertThat(e.getMessage(), containsString("a group has a null, but the planner said that no group does"));
            }
        }
    }

    private static Set<List<Integer>> keys(BlockHash hash) {
        Set<List<Integer>> keys = new HashSet<>();
        try (IntVector all = hash.nonEmpty()) {
            Block[] blocks = hash.getKeys(all);
            try {
                IntBlock first = (IntBlock) blocks[0];
                IntBlock second = (IntBlock) blocks[1];
                for (int p = 0; p < first.getPositionCount(); p++) {
                    keys.add(List.of(first.getInt(first.getFirstValueIndex(p)), second.getInt(second.getFirstValueIndex(p))));
                }
            } finally {
                Releasables.close(blocks);
            }
        }
        return keys;
    }
}
