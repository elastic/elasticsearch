/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;

import java.util.BitSet;
import java.util.List;

public class DoubleBlockEqualityTests extends ComputeTestCase {

    static final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        List<DoubleVector> vectors = List.of(
            blockFactory.newDoubleArrayVector(new double[] {}, 0),
            blockFactory.newDoubleArrayVector(new double[] { 0 }, 0),
            blockFactory.newConstantDoubleVector(0, 0),
            blockFactory.newConstantDoubleBlockWith(0, 0).filter().asVector(),
            blockFactory.newDoubleBlockBuilder(0).build().asVector(),
            blockFactory.newDoubleBlockBuilder(0).appendDouble(1).build().asVector().filter()
        );
        assertAllEquals(vectors);
    }

    public void testEmptyBlock() {
        // all these "empty" vectors should be equivalent
        List<DoubleBlock> blocks = List.of(
            blockFactory.newDoubleArrayBlock(
                new double[] {},
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newDoubleArrayBlock(
                new double[] { 0 },
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newConstantDoubleBlockWith(0, 0),
            blockFactory.newDoubleBlockBuilder(0).build(),
            blockFactory.newDoubleBlockBuilder(0).appendDouble(1).build().filter(),
            blockFactory.newDoubleBlockBuilder(0).appendNull().build().filter()
        );
        assertAllEquals(blocks);
        Releasables.close(blocks);
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        List<DoubleVector> vectors = List.of(
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3 }, 3),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3 }, 3).asBlock().asVector(),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3, 4 }, 3),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3 }, 3).filter(0, 1, 2),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2),
            blockFactory.newDoubleArrayVector(new double[] { 0, 1, 2, 3 }, 4).filter(1, 2, 3),
            blockFactory.newDoubleArrayVector(new double[] { 1, 4, 2, 3 }, 4).filter(0, 2, 3),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(2).appendDouble(3).build().asVector(),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(2).appendDouble(3).build().asVector().filter(0, 1, 2),
            blockFactory.newDoubleBlockBuilder(3)
                .appendDouble(1)
                .appendDouble(4)
                .appendDouble(2)
                .appendDouble(3)
                .build()
                .filter(0, 2, 3)
                .asVector(),
            blockFactory.newDoubleBlockBuilder(3)
                .appendDouble(1)
                .appendDouble(4)
                .appendDouble(2)
                .appendDouble(3)
                .build()
                .asVector()
                .filter(0, 2, 3)
        );
        assertAllEquals(vectors);

        // all these constant-like vectors should be equivalent
        List<DoubleVector> moreVectors = List.of(
            blockFactory.newDoubleArrayVector(new double[] { 1, 1, 1 }, 3),
            blockFactory.newDoubleArrayVector(new double[] { 1, 1, 1 }, 3).asBlock().asVector(),
            blockFactory.newDoubleArrayVector(new double[] { 1, 1, 1, 1 }, 3),
            blockFactory.newDoubleArrayVector(new double[] { 1, 1, 1 }, 3).filter(0, 1, 2),
            blockFactory.newDoubleArrayVector(new double[] { 1, 1, 1, 4 }, 4).filter(0, 1, 2),
            blockFactory.newDoubleArrayVector(new double[] { 3, 1, 1, 1 }, 4).filter(1, 2, 3),
            blockFactory.newDoubleArrayVector(new double[] { 1, 4, 1, 1 }, 4).filter(0, 2, 3),
            blockFactory.newConstantDoubleBlockWith(1, 3).asVector(),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(1).appendDouble(1).build().asVector(),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(1).appendDouble(1).build().asVector().filter(0, 1, 2),
            blockFactory.newDoubleBlockBuilder(3)
                .appendDouble(1)
                .appendDouble(4)
                .appendDouble(1)
                .appendDouble(1)
                .build()
                .filter(0, 2, 3)
                .asVector(),
            blockFactory.newDoubleBlockBuilder(3)
                .appendDouble(1)
                .appendDouble(4)
                .appendDouble(1)
                .appendDouble(1)
                .build()
                .asVector()
                .filter(0, 2, 3)
        );
        assertAllEquals(moreVectors);
    }

    public void testBlockEquality() {
        // all these blocks should be equivalent
        List<DoubleBlock> blocks = List.of(
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3 }, 3).asBlock(),
            new DoubleArrayBlock(
                new double[] { 1, 2, 3 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new DoubleArrayBlock(
                new double[] { 1, 2, 3, 4 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b1000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3 }, 3).filter(0, 1, 2).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3, 4 }, 3).filter(0, 1, 2).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 4, 3 }, 4).filter(0, 1, 3).asBlock(),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(2).appendDouble(3).build(),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(2).appendDouble(3).build().filter(0, 1, 2),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(4).appendDouble(2).appendDouble(3).build().filter(0, 2, 3),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendNull().appendDouble(2).appendDouble(3).build().filter(0, 2, 3)
        );
        assertAllEquals(blocks);

        // all these constant-like blocks should be equivalent
        List<DoubleBlock> moreBlocks = List.of(
            blockFactory.newDoubleArrayVector(new double[] { 9, 9 }, 2).asBlock(),
            new DoubleArrayBlock(
                new double[] { 9, 9 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new DoubleArrayBlock(
                new double[] { 9, 9, 4 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b100 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            blockFactory.newDoubleArrayVector(new double[] { 9, 9 }, 2).filter(0, 1).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 9, 9, 4 }, 2).filter(0, 1).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 9, 9, 4 }, 3).filter(0, 1).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 9, 4, 9 }, 3).filter(0, 2).asBlock(),
            blockFactory.newConstantDoubleBlockWith(9, 2),
            blockFactory.newDoubleBlockBuilder(2).appendDouble(9).appendDouble(9).build(),
            blockFactory.newDoubleBlockBuilder(2).appendDouble(9).appendDouble(9).build().filter(0, 1),
            blockFactory.newDoubleBlockBuilder(2).appendDouble(9).appendDouble(4).appendDouble(9).build().filter(0, 2),
            blockFactory.newDoubleBlockBuilder(2).appendDouble(9).appendNull().appendDouble(9).build().filter(0, 2)
        );
        assertAllEquals(moreBlocks);
    }

    public void testVectorInequality() {
        // all these vectors should NOT be equivalent
        List<DoubleVector> notEqualVectors = List.of(
            blockFactory.newDoubleArrayVector(new double[] { 1 }, 1),
            blockFactory.newDoubleArrayVector(new double[] { 9 }, 1),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2 }, 2),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3 }, 3),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 4 }, 3),
            blockFactory.newConstantDoubleBlockWith(9, 2).asVector(),
            blockFactory.newDoubleBlockBuilder(2).appendDouble(1).appendDouble(2).build().asVector().filter(1),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(2).appendDouble(5).build().asVector(),
            blockFactory.newDoubleBlockBuilder(1).appendDouble(1).appendDouble(2).appendDouble(3).appendDouble(4).build().asVector()
        );
        assertAllNotEquals(notEqualVectors);
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        List<DoubleBlock> notEqualBlocks = List.of(
            blockFactory.newDoubleArrayVector(new double[] { 1 }, 1).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 9 }, 1).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2 }, 2).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 3 }, 3).asBlock(),
            blockFactory.newDoubleArrayVector(new double[] { 1, 2, 4 }, 3).asBlock(),
            blockFactory.newConstantDoubleBlockWith(9, 2),
            blockFactory.newDoubleBlockBuilder(2).appendDouble(1).appendDouble(2).build().filter(1),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).appendDouble(2).appendDouble(5).build(),
            blockFactory.newDoubleBlockBuilder(1).appendDouble(1).appendDouble(2).appendDouble(3).appendDouble(4).build(),
            blockFactory.newDoubleBlockBuilder(1).appendDouble(1).appendNull().build(),
            blockFactory.newDoubleBlockBuilder(1).appendDouble(1).appendNull().appendDouble(3).build(),
            blockFactory.newDoubleBlockBuilder(1).appendDouble(1).appendDouble(3).build(),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1).beginPositionEntry().appendDouble(2).appendDouble(3).build()
        );
        assertAllNotEquals(notEqualBlocks);
    }

    public void testSimpleBlockWithSingleNull() {
        List<DoubleBlock> blocks = List.of(
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1.1).appendNull().appendDouble(3.1).build(),
            blockFactory.newDoubleBlockBuilder(3).appendDouble(1.1).appendNull().appendDouble(3.1).build()
        );
        assertEquals(3, blocks.get(0).getPositionCount());
        assertTrue(blocks.get(0).isNull(1));
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyNulls() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        DoubleBlock.Builder builder1 = blockFactory.newDoubleBlockBuilder(grow ? 0 : positions);
        DoubleBlock.Builder builder2 = blockFactory.newDoubleBlockBuilder(grow ? 0 : positions);
        for (int p = 0; p < positions; p++) {
            builder1.appendNull();
            builder2.appendNull();
        }
        DoubleBlock block1 = builder1.build();
        DoubleBlock block2 = builder2.build();
        assertEquals(positions, block1.getPositionCount());
        assertTrue(block1.mayHaveNulls());
        assertTrue(block1.isNull(0));

        List<DoubleBlock> blocks = List.of(block1, block2);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithSingleMultiValue() {
        List<DoubleBlock> blocks = List.of(
            blockFactory.newDoubleBlockBuilder(1).beginPositionEntry().appendDouble(1.1).appendDouble(2.2).build(),
            blockFactory.newDoubleBlockBuilder(1).beginPositionEntry().appendDouble(1.1).appendDouble(2.2).build()
        );
        assert blocks.get(0).getPositionCount() == 1 && blocks.get(0).getValueCount(0) == 2;
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyMultiValues() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        DoubleBlock.Builder builder1 = blockFactory.newDoubleBlockBuilder(grow ? 0 : positions);
        DoubleBlock.Builder builder2 = blockFactory.newDoubleBlockBuilder(grow ? 0 : positions);
        DoubleBlock.Builder builder3 = blockFactory.newDoubleBlockBuilder(grow ? 0 : positions);
        for (int pos = 0; pos < positions; pos++) {
            builder1.beginPositionEntry();
            builder2.beginPositionEntry();
            builder3.beginPositionEntry();
            int values = randomIntBetween(1, 16);
            for (int i = 0; i < values; i++) {
                double value = randomDouble();
                builder1.appendDouble(value);
                builder2.appendDouble(value);
                builder3.appendDouble(value);
            }
            builder1.endPositionEntry();
            builder2.endPositionEntry();
            builder3.endPositionEntry();
        }
        DoubleBlock block1 = builder1.build();
        DoubleBlock block2 = builder2.build();
        DoubleBlock block3 = builder3.build();

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
