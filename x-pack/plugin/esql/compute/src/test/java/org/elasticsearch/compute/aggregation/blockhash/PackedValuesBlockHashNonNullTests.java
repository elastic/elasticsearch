/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
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
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link PackedValuesBlockHash} for groups that the planner proved to have no null: the keys then have no null bytes.
 */
public class PackedValuesBlockHashNonNullTests extends ComputeTestCase {
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

    private static List<BlockHash.GroupSpec> specs(boolean nonNull, ElementType... types) {
        List<BlockHash.GroupSpec> specs = new ArrayList<>();
        for (int c = 0; c < types.length; c++) {
            specs.add(new BlockHash.GroupSpec(c, types[c]).withNonNull(nonNull));
        }
        return specs;
    }

    /** Two ints are 8 bytes, not 9: the null byte is gone. */
    public void testTwoIntsAreEightBytes() {
        BlockFactory blockFactory = blockFactory();
        try (
            PackedValuesBlockHash nonNull = new PackedValuesBlockHash(specs(true, ElementType.INT, ElementType.INT), blockFactory, 1024);
            PackedValuesBlockHash nullable = new PackedValuesBlockHash(specs(false, ElementType.INT, ElementType.INT), blockFactory, 1024);
            Page page = new Page(
                blockFactory.newIntArrayVector(new int[] { 1, -2, 3 }, 3).asBlock(),
                blockFactory.newIntArrayVector(new int[] { 10, 20, Integer.MIN_VALUE }, 3).asBlock()
            )
        ) {
            nonNull.add(page, IGNORE_IDS);
            nullable.add(page, IGNORE_IDS);
            assertThat(nonNull.bytesRefHash.get(0, new BytesRef()).length, equalTo(8));
            assertThat(nullable.bytesRefHash.get(0, new BytesRef()).length, equalTo(9));
        }
    }

    /** The keys come out the same as for groups that may have nulls, including for multivalued positions. */
    public void testKeysEqualTheOnesOfNullableGroups() {
        BlockFactory blockFactory = blockFactory();
        List<ElementType> typeList = List.of(ElementType.BOOLEAN, ElementType.INT, ElementType.LONG, ElementType.DOUBLE);
        ElementType[] types = randomList(1, 4, () -> randomFrom(typeList)).toArray(ElementType[]::new);
        List<Page> pages = new ArrayList<>();
        try (
            PackedValuesBlockHash nonNull = new PackedValuesBlockHash(specs(true, types), blockFactory, randomFrom(100, 1_000));
            PackedValuesBlockHash nullable = new PackedValuesBlockHash(specs(false, types), blockFactory, randomFrom(100, 1_000))
        ) {
            for (int p = 0; p < between(1, 10); p++) {
                int positions = between(1, 500);
                Block[] blocks = new Block[types.length];
                for (int g = 0; g < types.length; g++) {
                    boolean vector = randomBoolean();
                    blocks[g] = RandomBlock.randomBlock(blockFactory, types[g], positions, false, 1, vector ? 1 : between(1, 3), 0, 0)
                        .block();
                }
                Page page = new Page(blocks);
                pages.add(page);
                nonNull.add(page, IGNORE_IDS);
                nullable.add(page, IGNORE_IDS);
            }
            assertThat(nonNull.numKeys(), equalTo(nullable.numKeys()));
            try (IntVector all = nonNull.nonEmpty(); IntVector sameAll = nullable.nonEmpty()) {
                Block[] expected = nullable.getKeys(sameAll);
                Block[] actual = nonNull.getKeys(all);
                try {
                    for (int g = 0; g < types.length; g++) {
                        assertThat("group " + g, actual[g], equalTo(expected[g]));
                    }
                } finally {
                    Releasables.close(expected);
                    Releasables.close(actual);
                }
            }
        } finally {
            Releasables.close(pages);
        }
    }

    /** A null in a group that was said to have none must not be hashed as if it were a value. */
    public void testNullThrows() {
        BlockFactory blockFactory = blockFactory();
        try (
            PackedValuesBlockHash nonNull = new PackedValuesBlockHash(specs(true, ElementType.INT, ElementType.INT), blockFactory, 1024);
            IntBlock.Builder ints = blockFactory.newIntBlockBuilder(2)
        ) {
            ints.appendInt(1);
            ints.appendNull();
            // The page owns the blocks, so only it is closed.
            try (Page page = new Page(ints.build(), blockFactory.newIntArrayVector(new int[] { 5, 6 }, 2).asBlock())) {
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> nonNull.add(page, IGNORE_IDS));
                assertThat(e.getMessage(), containsString("has a null, but the planner said that no group does"));
            }
        }
    }
}
