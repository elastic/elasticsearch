/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;

import java.util.BitSet;
import java.util.List;

public class LongBlockEqualityTests extends ComputeTestCase {

    static final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        List<LongVector> vectors = List.of(
            blockFactory.newLongArrayVector(new long[] {}, 0),
            blockFactory.newLongArrayVector(new long[] { 0 }, 0),
            blockFactory.newConstantLongBlockWith(0, 0).asVector(),
            blockFactory.newConstantLongBlockWith(0, 0).filter().asVector(),
            blockFactory.newLongBlockBuilder(0).build().asVector(),
            blockFactory.newLongBlockBuilder(0).appendLong(1).build().asVector().filter()
        );
        assertAllEquals(vectors);
    }

    public void testEmptyBlock() {
        // all these "empty" vectors should be equivalent
        List<LongBlock> blocks = List.of(
            blockFactory.newLongArrayBlock(
                new long[] {},
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newLongArrayBlock(
                new long[] { 0 },
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newConstantLongBlockWith(0, 0),
            blockFactory.newLongBlockBuilder(0).build(),
            blockFactory.newLongBlockBuilder(0).appendLong(1).build().filter(),
            blockFactory.newLongBlockBuilder(0).appendNull().build().filter()
        );
        assertAllEquals(blocks);
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        List<LongVector> vectors = List.of(
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock().asVector(),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3, 4 }, 3),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).filter(0, 1, 2),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2),
            blockFactory.newLongArrayVector(new long[] { 0, 1, 2, 3 }, 4).filter(1, 2, 3),
            blockFactory.newLongArrayVector(new long[] { 1, 4, 2, 3 }, 4).filter(0, 2, 3),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(2).appendLong(3).build().asVector(),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(2).appendLong(3).build().asVector().filter(0, 1, 2),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(4).appendLong(2).appendLong(3).build().filter(0, 2, 3).asVector(),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(4).appendLong(2).appendLong(3).build().asVector().filter(0, 2, 3)
        );
        assertAllEquals(vectors);

        // all these constant-like vectors should be equivalent
        List<LongVector> moreVectors = List.of(
            blockFactory.newLongArrayVector(new long[] { 1, 1, 1 }, 3),
            blockFactory.newLongArrayVector(new long[] { 1, 1, 1 }, 3).asBlock().asVector(),
            blockFactory.newLongArrayVector(new long[] { 1, 1, 1, 1 }, 3),
            blockFactory.newLongArrayVector(new long[] { 1, 1, 1 }, 3).filter(0, 1, 2),
            blockFactory.newLongArrayVector(new long[] { 1, 1, 1, 4 }, 4).filter(0, 1, 2),
            blockFactory.newLongArrayVector(new long[] { 3, 1, 1, 1 }, 4).filter(1, 2, 3),
            blockFactory.newLongArrayVector(new long[] { 1, 4, 1, 1 }, 4).filter(0, 2, 3),
            blockFactory.newConstantLongBlockWith(1, 3).asVector(),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(1).appendLong(1).build().asVector(),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(1).appendLong(1).build().asVector().filter(0, 1, 2),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(4).appendLong(1).appendLong(1).build().filter(0, 2, 3).asVector(),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(4).appendLong(1).appendLong(1).build().asVector().filter(0, 2, 3)
        );
        assertAllEquals(moreVectors);
    }

    public void testBlockEquality() {
        // all these blocks should be equivalent
        List<LongBlock> blocks = List.of(
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock(),
            blockFactory.newLongArrayBlock(
                new long[] { 1, 2, 3 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newLongArrayBlock(
                new long[] { 1, 2, 3, 4 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b1000 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).filter(0, 1, 2).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3, 4 }, 3).filter(0, 1, 2).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 4, 3 }, 4).filter(0, 1, 3).asBlock(),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(2).appendLong(3).build(),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(2).appendLong(3).build().filter(0, 1, 2),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(4).appendLong(2).appendLong(3).build().filter(0, 2, 3),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendNull().appendLong(2).appendLong(3).build().filter(0, 2, 3)
        );
        assertAllEquals(blocks);

        // all these constant-like blocks should be equivalent
        List<LongBlock> moreBlocks = List.of(
            blockFactory.newLongArrayVector(new long[] { 9, 9 }, 2).asBlock(),
            blockFactory.newLongArrayBlock(
                new long[] { 9, 9 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newLongArrayBlock(
                new long[] { 9, 9, 4 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b100 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newLongArrayVector(new long[] { 9, 9 }, 2).filter(0, 1).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 9, 9, 4 }, 2).filter(0, 1).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 9, 9, 4 }, 3).filter(0, 1).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 9, 4, 9 }, 3).filter(0, 2).asBlock(),
            blockFactory.newConstantLongBlockWith(9, 2),
            blockFactory.newLongBlockBuilder(2).appendLong(9).appendLong(9).build(),
            blockFactory.newLongBlockBuilder(2).appendLong(9).appendLong(9).build().filter(0, 1),
            blockFactory.newLongBlockBuilder(2).appendLong(9).appendLong(4).appendLong(9).build().filter(0, 2),
            blockFactory.newLongBlockBuilder(2).appendLong(9).appendNull().appendLong(9).build().filter(0, 2)
        );
        assertAllEquals(moreBlocks);
    }

    public void testVectorInequality() {
        // all these vectors should NOT be equivalent
        List<LongVector> notEqualVectors = List.of(
            blockFactory.newLongArrayVector(new long[] { 1 }, 1),
            blockFactory.newLongArrayVector(new long[] { 9 }, 1),
            blockFactory.newLongArrayVector(new long[] { 1, 2 }, 2),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 4 }, 3),
            blockFactory.newConstantLongBlockWith(9, 2).asVector(),
            blockFactory.newLongBlockBuilder(2).appendLong(1).appendLong(2).build().asVector().filter(1),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(2).appendLong(5).build().asVector(),
            blockFactory.newLongBlockBuilder(1).appendLong(1).appendLong(2).appendLong(3).appendLong(4).build().asVector()
        );
        assertAllNotEquals(notEqualVectors);
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        List<LongBlock> notEqualBlocks = List.of(
            blockFactory.newLongArrayVector(new long[] { 1 }, 1).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 9 }, 1).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock(),
            blockFactory.newLongArrayVector(new long[] { 1, 2, 4 }, 3).asBlock(),
            blockFactory.newConstantLongBlockWith(9, 2),
            blockFactory.newLongBlockBuilder(2).appendLong(1).appendLong(2).build().filter(1),
            blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(2).appendLong(5).build(),
            blockFactory.newLongBlockBuilder(1).appendLong(1).appendLong(2).appendLong(3).appendLong(4).build(),
            blockFactory.newLongBlockBuilder(1).appendLong(1).appendNull().build(),
            blockFactory.newLongBlockBuilder(1).appendLong(1).appendNull().appendLong(3).build(),
            blockFactory.newLongBlockBuilder(1).appendLong(1).appendLong(3).build(),
            blockFactory.newLongBlockBuilder(3).appendLong(1).beginPositionEntry().appendLong(2).appendLong(3).build()
        );
        assertAllNotEquals(notEqualBlocks);
    }

    public void testSimpleBlockWithSingleNull() {
        List<LongBlock> blocks = List.of(
            blockFactory.newLongBlockBuilder(1).appendLong(1).appendNull().appendLong(3).build(),
            blockFactory.newLongBlockBuilder(1).appendLong(1).appendNull().appendLong(3).build()
        );
        assertEquals(3, blocks.get(0).getPositionCount());
        assertTrue(blocks.get(0).isNull(1));
        assertTrue(blocks.get(0).asVector() == null);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyNulls() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        LongBlock.Builder builder1 = blockFactory.newLongBlockBuilder(grow ? 0 : positions);
        LongBlock.Builder builder2 = blockFactory.newLongBlockBuilder(grow ? 0 : positions);
        for (int p = 0; p < positions; p++) {
            builder1.appendNull();
            builder2.appendNull();
        }
        LongBlock block1 = builder1.build();
        LongBlock block2 = builder2.build();
        assertEquals(positions, block1.getPositionCount());
        assertTrue(block1.mayHaveNulls());
        assertTrue(block1.isNull(0));

        List<LongBlock> blocks = List.of(block1, block2);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithSingleMultiValue() {
        List<LongBlock> blocks = List.of(
            blockFactory.newLongBlockBuilder(1).beginPositionEntry().appendLong(1).appendLong(2).build(),
            blockFactory.newLongBlockBuilder(1).beginPositionEntry().appendLong(1).appendLong(2).build()
        );
        assertEquals(1, blocks.get(0).getPositionCount());
        assertEquals(2, blocks.get(0).getValueCount(0));
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyMultiValues() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        LongBlock.Builder builder1 = blockFactory.newLongBlockBuilder(grow ? 0 : positions);
        LongBlock.Builder builder2 = blockFactory.newLongBlockBuilder(grow ? 0 : positions);
        LongBlock.Builder builder3 = blockFactory.newLongBlockBuilder(grow ? 0 : positions);
        for (int pos = 0; pos < positions; pos++) {
            builder1.beginPositionEntry();
            builder2.beginPositionEntry();
            builder3.beginPositionEntry();
            int valueCount = randomIntBetween(1, 16);
            for (int i = 0; i < valueCount; i++) {
                long value = randomLong();
                builder1.appendLong(value);
                builder2.appendLong(value);
                builder3.appendLong(value);
            }
            builder1.endPositionEntry();
            builder2.endPositionEntry();
            builder3.endPositionEntry();
        }
        LongBlock block1 = builder1.build();
        LongBlock block2 = builder2.build();
        LongBlock block3 = builder3.build();

        assertEquals(positions, block1.getPositionCount());
        assertAllEquals(List.of(block1, block2, block3));
    }

    static void assertAllEquals(List<?> objs) {
        for (Object obj1 : objs) {
            for (Object obj2 : objs) {
                assertEquals(obj1, obj2);
                assertEquals(obj2, obj1);
                // equal objects must generate the same hash code
                assertEquals(obj1.hashCode(), obj2.hashCode());
            }
        }
    }

    static void assertAllNotEquals(List<?> objs) {
        for (Object obj1 : objs) {
            for (Object obj2 : objs) {
                if (obj1 == obj2) {
                    continue; // skip self
                }
                assertNotEquals(obj1, obj2);
                assertNotEquals(obj2, obj1);
                // unequal objects SHOULD generate the different hash code
                assertNotEquals(obj1.hashCode(), obj2.hashCode());
            }
        }
    }
}
