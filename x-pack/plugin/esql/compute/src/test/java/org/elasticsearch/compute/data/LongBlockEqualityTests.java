/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

public class LongBlockEqualityTests extends ESTestCase {

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        List<LongVector> vectors = List.of(
            new LongArrayVector(new long[] {}, 0),
            new LongArrayVector(new long[] { 0 }, 0),
            LongBlock.newConstantBlockWith(0, 0).asVector(),
            LongBlock.newConstantBlockWith(0, 0).filter().asVector(),
            LongBlock.newBlockBuilder(0).build().asVector(),
            LongBlock.newBlockBuilder(0).appendLong(1).build().asVector().filter()
        );
        assertAllEquals(vectors);
    }

    public void testEmptyBlock() {
        // all these "empty" vectors should be equivalent
        List<LongBlock> blocks = List.of(
            new LongArrayBlock(new long[] {}, 0, new int[] {}, BitSet.valueOf(new byte[] { 0b00 }), randomFrom(Block.MvOrdering.values())),
            new LongArrayBlock(
                new long[] { 0 },
                0,
                new int[] {},
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            LongBlock.newConstantBlockWith(0, 0),
            LongBlock.newBlockBuilder(0).build(),
            LongBlock.newBlockBuilder(0).appendLong(1).build().filter(),
            LongBlock.newBlockBuilder(0).appendNull().build().filter()
        );
        assertAllEquals(blocks);
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        List<LongVector> vectors = List.of(
            new LongArrayVector(new long[] { 1, 2, 3 }, 3),
            new LongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock().asVector(),
            new LongArrayVector(new long[] { 1, 2, 3, 4 }, 3),
            new LongArrayVector(new long[] { 1, 2, 3 }, 3).filter(0, 1, 2),
            new LongArrayVector(new long[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2),
            new LongArrayVector(new long[] { 0, 1, 2, 3 }, 4).filter(1, 2, 3),
            new LongArrayVector(new long[] { 1, 4, 2, 3 }, 4).filter(0, 2, 3),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(2).appendLong(3).build().asVector(),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(2).appendLong(3).build().asVector().filter(0, 1, 2),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(4).appendLong(2).appendLong(3).build().filter(0, 2, 3).asVector(),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(4).appendLong(2).appendLong(3).build().asVector().filter(0, 2, 3)
        );
        assertAllEquals(vectors);

        // all these constant-like vectors should be equivalent
        List<LongVector> moreVectors = List.of(
            new LongArrayVector(new long[] { 1, 1, 1 }, 3),
            new LongArrayVector(new long[] { 1, 1, 1 }, 3).asBlock().asVector(),
            new LongArrayVector(new long[] { 1, 1, 1, 1 }, 3),
            new LongArrayVector(new long[] { 1, 1, 1 }, 3).filter(0, 1, 2),
            new LongArrayVector(new long[] { 1, 1, 1, 4 }, 4).filter(0, 1, 2),
            new LongArrayVector(new long[] { 3, 1, 1, 1 }, 4).filter(1, 2, 3),
            new LongArrayVector(new long[] { 1, 4, 1, 1 }, 4).filter(0, 2, 3),
            LongBlock.newConstantBlockWith(1, 3).asVector(),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(1).appendLong(1).build().asVector(),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(1).appendLong(1).build().asVector().filter(0, 1, 2),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(4).appendLong(1).appendLong(1).build().filter(0, 2, 3).asVector(),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(4).appendLong(1).appendLong(1).build().asVector().filter(0, 2, 3)
        );
        assertAllEquals(moreVectors);
    }

    public void testBlockEquality() {
        // all these blocks should be equivalent
        List<LongBlock> blocks = List.of(
            new LongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock(),
            new LongArrayBlock(
                new long[] { 1, 2, 3 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values())
            ),
            new LongArrayBlock(
                new long[] { 1, 2, 3, 4 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b1000 }),
                randomFrom(Block.MvOrdering.values())
            ),
            new LongArrayVector(new long[] { 1, 2, 3 }, 3).filter(0, 1, 2).asBlock(),
            new LongArrayVector(new long[] { 1, 2, 3, 4 }, 3).filter(0, 1, 2).asBlock(),
            new LongArrayVector(new long[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2).asBlock(),
            new LongArrayVector(new long[] { 1, 2, 4, 3 }, 4).filter(0, 1, 3).asBlock(),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(2).appendLong(3).build(),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(2).appendLong(3).build().filter(0, 1, 2),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(4).appendLong(2).appendLong(3).build().filter(0, 2, 3),
            LongBlock.newBlockBuilder(3).appendLong(1).appendNull().appendLong(2).appendLong(3).build().filter(0, 2, 3)
        );
        assertAllEquals(blocks);

        // all these constant-like blocks should be equivalent
        List<LongBlock> moreBlocks = List.of(
            new LongArrayVector(new long[] { 9, 9 }, 2).asBlock(),
            new LongArrayBlock(
                new long[] { 9, 9 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values())
            ),
            new LongArrayBlock(
                new long[] { 9, 9, 4 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b100 }),
                randomFrom(Block.MvOrdering.values())
            ),
            new LongArrayVector(new long[] { 9, 9 }, 2).filter(0, 1).asBlock(),
            new LongArrayVector(new long[] { 9, 9, 4 }, 2).filter(0, 1).asBlock(),
            new LongArrayVector(new long[] { 9, 9, 4 }, 3).filter(0, 1).asBlock(),
            new LongArrayVector(new long[] { 9, 4, 9 }, 3).filter(0, 2).asBlock(),
            LongBlock.newConstantBlockWith(9, 2),
            LongBlock.newBlockBuilder(2).appendLong(9).appendLong(9).build(),
            LongBlock.newBlockBuilder(2).appendLong(9).appendLong(9).build().filter(0, 1),
            LongBlock.newBlockBuilder(2).appendLong(9).appendLong(4).appendLong(9).build().filter(0, 2),
            LongBlock.newBlockBuilder(2).appendLong(9).appendNull().appendLong(9).build().filter(0, 2)
        );
        assertAllEquals(moreBlocks);
    }

    public void testVectorInequality() {
        // all these vectors should NOT be equivalent
        List<LongVector> notEqualVectors = List.of(
            new LongArrayVector(new long[] { 1 }, 1),
            new LongArrayVector(new long[] { 9 }, 1),
            new LongArrayVector(new long[] { 1, 2 }, 2),
            new LongArrayVector(new long[] { 1, 2, 3 }, 3),
            new LongArrayVector(new long[] { 1, 2, 4 }, 3),
            LongBlock.newConstantBlockWith(9, 2).asVector(),
            LongBlock.newBlockBuilder(2).appendLong(1).appendLong(2).build().asVector().filter(1),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(2).appendLong(5).build().asVector(),
            LongBlock.newBlockBuilder(1).appendLong(1).appendLong(2).appendLong(3).appendLong(4).build().asVector()
        );
        assertAllNotEquals(notEqualVectors);
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        List<LongBlock> notEqualBlocks = List.of(
            new LongArrayVector(new long[] { 1 }, 1).asBlock(),
            new LongArrayVector(new long[] { 9 }, 1).asBlock(),
            new LongArrayVector(new long[] { 1, 2 }, 2).asBlock(),
            new LongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock(),
            new LongArrayVector(new long[] { 1, 2, 4 }, 3).asBlock(),
            LongBlock.newConstantBlockWith(9, 2),
            LongBlock.newBlockBuilder(2).appendLong(1).appendLong(2).build().filter(1),
            LongBlock.newBlockBuilder(3).appendLong(1).appendLong(2).appendLong(5).build(),
            LongBlock.newBlockBuilder(1).appendLong(1).appendLong(2).appendLong(3).appendLong(4).build(),
            LongBlock.newBlockBuilder(1).appendLong(1).appendNull().build(),
            LongBlock.newBlockBuilder(1).appendLong(1).appendNull().appendLong(3).build(),
            LongBlock.newBlockBuilder(1).appendLong(1).appendLong(3).build(),
            LongBlock.newBlockBuilder(3).appendLong(1).beginPositionEntry().appendLong(2).appendLong(3).build()
        );
        assertAllNotEquals(notEqualBlocks);
    }

    public void testSimpleBlockWithSingleNull() {
        List<LongBlock> blocks = List.of(
            LongBlock.newBlockBuilder(1).appendLong(1).appendNull().appendLong(3).build(),
            LongBlock.newBlockBuilder(1).appendLong(1).appendNull().appendLong(3).build()
        );
        assertEquals(3, blocks.get(0).getPositionCount());
        assertTrue(blocks.get(0).isNull(1));
        assertTrue(blocks.get(0).asVector() == null);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyNulls() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        var builder = LongBlock.newBlockBuilder(grow ? 0 : positions);
        IntStream.range(0, positions).forEach(i -> builder.appendNull());
        LongBlock block1 = builder.build();
        LongBlock block2 = builder.build();
        assertEquals(positions, block1.getPositionCount());
        assertTrue(block1.mayHaveNulls());
        assertTrue(block1.isNull(0));

        List<LongBlock> blocks = List.of(block1, block2);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithSingleMultiValue() {
        List<LongBlock> blocks = List.of(
            LongBlock.newBlockBuilder(1).beginPositionEntry().appendLong(1).appendLong(2).build(),
            LongBlock.newBlockBuilder(1).beginPositionEntry().appendLong(1).appendLong(2).build()
        );
        assertEquals(1, blocks.get(0).getPositionCount());
        assertEquals(2, blocks.get(0).getValueCount(0));
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyMultiValues() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        var builder = LongBlock.newBlockBuilder(grow ? 0 : positions);
        for (int pos = 0; pos < positions; pos++) {
            builder.beginPositionEntry();
            int values = randomIntBetween(1, 16);
            IntStream.range(0, values).forEach(i -> builder.appendLong(randomLong()));
        }
        LongBlock block1 = builder.build();
        LongBlock block2 = builder.build();
        LongBlock block3 = builder.build();

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
