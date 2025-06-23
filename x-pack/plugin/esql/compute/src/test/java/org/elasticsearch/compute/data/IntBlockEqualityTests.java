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

public class IntBlockEqualityTests extends ComputeTestCase {

    static final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        List<IntVector> vectors = List.of(
            blockFactory.newIntArrayVector(new int[] {}, 0),
            blockFactory.newIntArrayVector(new int[] { 0 }, 0),
            blockFactory.newConstantIntVector(0, 0),
            blockFactory.newConstantIntVector(0, 0).filter(),
            blockFactory.newIntBlockBuilder(0).build().asVector(),
            blockFactory.newIntBlockBuilder(0).appendInt(1).build().asVector().filter()
        );
        assertAllEquals(vectors);
    }

    public void testEmptyBlock() {
        // all these "empty" vectors should be equivalent
        List<IntBlock> blocks = List.of(
            blockFactory.newIntArrayBlock(
                new int[] {},
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newIntArrayBlock(
                new int[] { 0 },
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newConstantIntBlockWith(0, 0),
            blockFactory.newIntBlockBuilder(0).build(),
            blockFactory.newIntBlockBuilder(0).appendInt(1).build().filter(),
            blockFactory.newIntBlockBuilder(0).appendNull().build().filter()
        );
        assertAllEquals(blocks);
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        List<IntVector> vectors = List.of(
            blockFactory.newIntArrayVector(new int[] { 1, 2, 3 }, 3),
            blockFactory.newIntArrayVector(new int[] { 1, 2, 3 }, 3).asBlock().asVector(),
            blockFactory.newIntArrayVector(new int[] { 1, 2, 3, 4 }, 3),
            blockFactory.newIntArrayVector(new int[] { 1, 2, 3 }, 3).filter(0, 1, 2),
            blockFactory.newIntArrayVector(new int[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2),
            blockFactory.newIntArrayVector(new int[] { 0, 1, 2, 3 }, 4).filter(1, 2, 3),
            blockFactory.newIntArrayVector(new int[] { 1, 4, 2, 3 }, 4).filter(0, 2, 3),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build().asVector(),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build().asVector().filter(0, 1, 2),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(4).appendInt(2).appendInt(3).build().filter(0, 2, 3).asVector(),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(4).appendInt(2).appendInt(3).build().asVector().filter(0, 2, 3)
        );
        assertAllEquals(vectors);

        // all these constant-like vectors should be equivalent
        List<IntVector> moreVectors = List.of(
            blockFactory.newIntArrayVector(new int[] { 1, 1, 1 }, 3),
            blockFactory.newIntArrayVector(new int[] { 1, 1, 1 }, 3).asBlock().asVector(),
            blockFactory.newIntArrayVector(new int[] { 1, 1, 1, 1 }, 3),
            blockFactory.newIntArrayVector(new int[] { 1, 1, 1 }, 3).filter(0, 1, 2),
            blockFactory.newIntArrayVector(new int[] { 1, 1, 1, 4 }, 4).filter(0, 1, 2),
            blockFactory.newIntArrayVector(new int[] { 3, 1, 1, 1 }, 4).filter(1, 2, 3),
            blockFactory.newIntArrayVector(new int[] { 1, 4, 1, 1 }, 4).filter(0, 2, 3),
            blockFactory.newConstantIntBlockWith(1, 3).asVector(),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(1).appendInt(1).build().asVector(),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(1).appendInt(1).build().asVector().filter(0, 1, 2),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(4).appendInt(1).appendInt(1).build().filter(0, 2, 3).asVector(),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(4).appendInt(1).appendInt(1).build().asVector().filter(0, 2, 3)
        );
        assertAllEquals(moreVectors);
    }

    public void testBlockEquality() {
        // all these blocks should be equivalent
        List<IntBlock> blocks = List.of(
            new IntArrayVector(new int[] { 1, 2, 3 }, 3, blockFactory).asBlock(),
            new IntArrayBlock(
                new int[] { 1, 2, 3 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new IntArrayBlock(
                new int[] { 1, 2, 3, 4 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b1000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new IntArrayVector(new int[] { 1, 2, 3 }, 3, blockFactory).filter(0, 1, 2).asBlock(),
            new IntArrayVector(new int[] { 1, 2, 3, 4 }, 3, blockFactory).filter(0, 1, 2).asBlock(),
            new IntArrayVector(new int[] { 1, 2, 3, 4 }, 4, blockFactory).filter(0, 1, 2).asBlock(),
            new IntArrayVector(new int[] { 1, 2, 4, 3 }, 4, blockFactory).filter(0, 1, 3).asBlock(),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build(),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build().filter(0, 1, 2),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(4).appendInt(2).appendInt(3).build().filter(0, 2, 3),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendNull().appendInt(2).appendInt(3).build().filter(0, 2, 3)
        );
        assertAllEquals(blocks);

        // all these constant-like blocks should be equivalent
        List<IntBlock> moreBlocks = List.of(
            blockFactory.newIntArrayVector(new int[] { 9, 9 }, 2).asBlock(),
            blockFactory.newIntArrayBlock(
                new int[] { 9, 9 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newIntArrayBlock(
                new int[] { 9, 9, 4 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b100 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newIntArrayVector(new int[] { 9, 9 }, 2).filter(0, 1).asBlock(),
            blockFactory.newIntArrayVector(new int[] { 9, 9, 4 }, 2).filter(0, 1).asBlock(),
            blockFactory.newIntArrayVector(new int[] { 9, 9, 4 }, 3).filter(0, 1).asBlock(),
            blockFactory.newIntArrayVector(new int[] { 9, 4, 9 }, 3).filter(0, 2).asBlock(),
            blockFactory.newConstantIntBlockWith(9, 2),
            blockFactory.newIntBlockBuilder(2).appendInt(9).appendInt(9).build(),
            blockFactory.newIntBlockBuilder(2).appendInt(9).appendInt(9).build().filter(0, 1),
            blockFactory.newIntBlockBuilder(2).appendInt(9).appendInt(4).appendInt(9).build().filter(0, 2),
            blockFactory.newIntBlockBuilder(2).appendInt(9).appendNull().appendInt(9).build().filter(0, 2)
        );
        assertAllEquals(moreBlocks);
    }

    public void testVectorInequality() {
        // all these vectors should NOT be equivalent
        List<IntVector> notEqualVectors = List.of(
            blockFactory.newIntArrayVector(new int[] { 1 }, 1),
            blockFactory.newIntArrayVector(new int[] { 9 }, 1),
            blockFactory.newIntArrayVector(new int[] { 1, 2 }, 2),
            blockFactory.newIntArrayVector(new int[] { 1, 2, 3 }, 3),
            blockFactory.newIntArrayVector(new int[] { 1, 2, 4 }, 3),
            blockFactory.newConstantIntBlockWith(9, 2).asVector(),
            blockFactory.newIntBlockBuilder(2).appendInt(1).appendInt(2).build().asVector().filter(1),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(2).appendInt(5).build().asVector(),
            blockFactory.newIntBlockBuilder(1).appendInt(1).appendInt(2).appendInt(3).appendInt(4).build().asVector()
        );
        assertAllNotEquals(notEqualVectors);
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        List<IntBlock> notEqualBlocks = List.of(
            blockFactory.newIntArrayVector(new int[] { 1 }, 1).asBlock(),
            blockFactory.newIntArrayVector(new int[] { 9 }, 1).asBlock(),
            blockFactory.newIntArrayVector(new int[] { 1, 2 }, 2).asBlock(),
            blockFactory.newIntArrayVector(new int[] { 1, 2, 3 }, 3).asBlock(),
            blockFactory.newIntArrayVector(new int[] { 1, 2, 4 }, 3).asBlock(),
            blockFactory.newConstantIntBlockWith(9, 2),
            blockFactory.newIntBlockBuilder(2).appendInt(1).appendInt(2).build().filter(1),
            blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(2).appendInt(5).build(),
            blockFactory.newIntBlockBuilder(1).appendInt(1).appendInt(2).appendInt(3).appendInt(4).build(),
            blockFactory.newIntBlockBuilder(1).appendInt(1).appendNull().build(),
            blockFactory.newIntBlockBuilder(1).appendInt(1).appendNull().appendInt(3).build(),
            blockFactory.newIntBlockBuilder(1).appendInt(1).appendInt(3).build(),
            blockFactory.newIntBlockBuilder(3).appendInt(1).beginPositionEntry().appendInt(2).appendInt(3).build()
        );
        assertAllNotEquals(notEqualBlocks);
    }

    public void testSimpleBlockWithSingleNull() {
        List<IntBlock> blocks = List.of(
            blockFactory.newIntBlockBuilder(1).appendInt(1).appendNull().appendInt(3).build(),
            blockFactory.newIntBlockBuilder(1).appendInt(1).appendNull().appendInt(3).build()
        );
        assertEquals(3, blocks.get(0).getPositionCount());
        assertTrue(blocks.get(0).isNull(1));
        assertTrue(blocks.get(0).asVector() == null);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyNulls() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        IntBlock.Builder builder1 = blockFactory.newIntBlockBuilder(grow ? 0 : positions);
        IntBlock.Builder builder2 = blockFactory.newIntBlockBuilder(grow ? 0 : positions);
        for (int p = 0; p < positions; p++) {
            builder1.appendNull();
            builder2.appendNull();
        }
        IntBlock block1 = builder1.build();
        IntBlock block2 = builder2.build();
        assertEquals(positions, block1.getPositionCount());
        assertTrue(block1.mayHaveNulls());
        assertTrue(block1.isNull(0));

        List<IntBlock> blocks = List.of(block1, block2);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithSingleMultiValue() {
        List<IntBlock> blocks = List.of(
            blockFactory.newIntBlockBuilder(1).beginPositionEntry().appendInt(1).appendInt(2).build(),
            blockFactory.newIntBlockBuilder(1).beginPositionEntry().appendInt(1).appendInt(2).build()
        );
        assertEquals(1, blocks.get(0).getPositionCount());
        assertEquals(2, blocks.get(0).getValueCount(0));
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyMultiValues() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        IntBlock.Builder builder1 = blockFactory.newIntBlockBuilder(grow ? 0 : positions);
        IntBlock.Builder builder2 = blockFactory.newIntBlockBuilder(grow ? 0 : positions);
        IntBlock.Builder builder3 = blockFactory.newIntBlockBuilder(grow ? 0 : positions);
        for (int pos = 0; pos < positions; pos++) {
            builder1.beginPositionEntry();
            builder2.beginPositionEntry();
            builder3.beginPositionEntry();
            int values = randomIntBetween(1, 16);
            for (int i = 0; i < values; i++) {
                int value = randomInt();
                builder1.appendInt(value);
                builder2.appendInt(value);
                builder3.appendInt(value);
            }
            builder1.endPositionEntry();
            builder2.endPositionEntry();
            builder3.endPositionEntry();
        }
        IntBlock block1 = builder1.build();
        IntBlock block2 = builder2.build();
        IntBlock block3 = builder3.build();

        assertEquals(positions, block1.getPositionCount());
        assertAllEquals(List.of(block1, block2, block3));
    }

    static void assertAllEquals(List<?> objs) {
        for (Object obj1 : objs) {
            for (Object obj2 : objs) {
                assertEquals(obj1, obj2);
                assertEquals(obj2, obj1);
                // equal objects MUST generate the same hash code
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
