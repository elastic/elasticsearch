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

public class FloatBlockEqualityTests extends ComputeTestCase {

    static final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        List<FloatVector> vectors = List.of(
            blockFactory.newFloatArrayVector(new float[] {}, 0),
            blockFactory.newFloatArrayVector(new float[] { 0 }, 0),
            blockFactory.newConstantFloatVector(0, 0),
            blockFactory.newConstantFloatBlockWith(0, 0).filter().asVector(),
            blockFactory.newFloatBlockBuilder(0).build().asVector(),
            blockFactory.newFloatBlockBuilder(0).appendFloat(1).build().asVector().filter()
        );
        assertAllEquals(vectors);
    }

    public void testEmptyBlock() {
        // all these "empty" vectors should be equivalent
        List<FloatBlock> blocks = List.of(
            blockFactory.newFloatArrayBlock(
                new float[] {},
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newFloatArrayBlock(
                new float[] { 0 },
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values())
            ),
            blockFactory.newConstantFloatBlockWith(0, 0),
            blockFactory.newFloatBlockBuilder(0).build(),
            blockFactory.newFloatBlockBuilder(0).appendFloat(1).build().filter(),
            blockFactory.newFloatBlockBuilder(0).appendNull().build().filter()
        );
        assertAllEquals(blocks);
        Releasables.close(blocks);
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        List<FloatVector> vectors = List.of(
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3 }, 3),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3 }, 3).asBlock().asVector(),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3, 4 }, 3),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3 }, 3).filter(0, 1, 2),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2),
            blockFactory.newFloatArrayVector(new float[] { 0, 1, 2, 3 }, 4).filter(1, 2, 3),
            blockFactory.newFloatArrayVector(new float[] { 1, 4, 2, 3 }, 4).filter(0, 2, 3),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(2).appendFloat(3).build().asVector(),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(2).appendFloat(3).build().asVector().filter(0, 1, 2),
            blockFactory.newFloatBlockBuilder(3)
                .appendFloat(1)
                .appendFloat(4)
                .appendFloat(2)
                .appendFloat(3)
                .build()
                .filter(0, 2, 3)
                .asVector(),
            blockFactory.newFloatBlockBuilder(3)
                .appendFloat(1)
                .appendFloat(4)
                .appendFloat(2)
                .appendFloat(3)
                .build()
                .asVector()
                .filter(0, 2, 3)
        );
        assertAllEquals(vectors);

        // all these constant-like vectors should be equivalent
        List<FloatVector> moreVectors = List.of(
            blockFactory.newFloatArrayVector(new float[] { 1, 1, 1 }, 3),
            blockFactory.newFloatArrayVector(new float[] { 1, 1, 1 }, 3).asBlock().asVector(),
            blockFactory.newFloatArrayVector(new float[] { 1, 1, 1, 1 }, 3),
            blockFactory.newFloatArrayVector(new float[] { 1, 1, 1 }, 3).filter(0, 1, 2),
            blockFactory.newFloatArrayVector(new float[] { 1, 1, 1, 4 }, 4).filter(0, 1, 2),
            blockFactory.newFloatArrayVector(new float[] { 3, 1, 1, 1 }, 4).filter(1, 2, 3),
            blockFactory.newFloatArrayVector(new float[] { 1, 4, 1, 1 }, 4).filter(0, 2, 3),
            blockFactory.newConstantFloatBlockWith(1, 3).asVector(),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(1).appendFloat(1).build().asVector(),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(1).appendFloat(1).build().asVector().filter(0, 1, 2),
            blockFactory.newFloatBlockBuilder(3)
                .appendFloat(1)
                .appendFloat(4)
                .appendFloat(1)
                .appendFloat(1)
                .build()
                .filter(0, 2, 3)
                .asVector(),
            blockFactory.newFloatBlockBuilder(3)
                .appendFloat(1)
                .appendFloat(4)
                .appendFloat(1)
                .appendFloat(1)
                .build()
                .asVector()
                .filter(0, 2, 3)
        );
        assertAllEquals(moreVectors);
    }

    public void testBlockEquality() {
        // all these blocks should be equivalent
        List<FloatBlock> blocks = List.of(
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3 }, 3).asBlock(),
            new FloatArrayBlock(
                new float[] { 1, 2, 3 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new FloatArrayBlock(
                new float[] { 1, 2, 3, 4 },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b1000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3 }, 3).filter(0, 1, 2).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3, 4 }, 3).filter(0, 1, 2).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 4, 3 }, 4).filter(0, 1, 3).asBlock(),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(2).appendFloat(3).build(),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(2).appendFloat(3).build().filter(0, 1, 2),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(4).appendFloat(2).appendFloat(3).build().filter(0, 2, 3),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendNull().appendFloat(2).appendFloat(3).build().filter(0, 2, 3)
        );
        assertAllEquals(blocks);

        // all these constant-like blocks should be equivalent
        List<FloatBlock> moreBlocks = List.of(
            blockFactory.newFloatArrayVector(new float[] { 9, 9 }, 2).asBlock(),
            new FloatArrayBlock(
                new float[] { 9, 9 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new FloatArrayBlock(
                new float[] { 9, 9, 4 },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b100 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            blockFactory.newFloatArrayVector(new float[] { 9, 9 }, 2).filter(0, 1).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 9, 9, 4 }, 2).filter(0, 1).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 9, 9, 4 }, 3).filter(0, 1).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 9, 4, 9 }, 3).filter(0, 2).asBlock(),
            blockFactory.newConstantFloatBlockWith(9, 2),
            blockFactory.newFloatBlockBuilder(2).appendFloat(9).appendFloat(9).build(),
            blockFactory.newFloatBlockBuilder(2).appendFloat(9).appendFloat(9).build().filter(0, 1),
            blockFactory.newFloatBlockBuilder(2).appendFloat(9).appendFloat(4).appendFloat(9).build().filter(0, 2),
            blockFactory.newFloatBlockBuilder(2).appendFloat(9).appendNull().appendFloat(9).build().filter(0, 2)
        );
        assertAllEquals(moreBlocks);
    }

    public void testVectorInequality() {
        // all these vectors should NOT be equivalent
        List<FloatVector> notEqualVectors = List.of(
            blockFactory.newFloatArrayVector(new float[] { 1 }, 1),
            blockFactory.newFloatArrayVector(new float[] { 9 }, 1),
            blockFactory.newFloatArrayVector(new float[] { 1, 2 }, 2),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3 }, 3),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 4 }, 3),
            blockFactory.newConstantFloatBlockWith(9, 2).asVector(),
            blockFactory.newFloatBlockBuilder(2).appendFloat(1).appendFloat(2).build().asVector().filter(1),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(2).appendFloat(5).build().asVector(),
            blockFactory.newFloatBlockBuilder(1).appendFloat(1).appendFloat(2).appendFloat(3).appendFloat(4).build().asVector()
        );
        assertAllNotEquals(notEqualVectors);
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        List<FloatBlock> notEqualBlocks = List.of(
            blockFactory.newFloatArrayVector(new float[] { 1 }, 1).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 9 }, 1).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 1, 2 }, 2).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 3 }, 3).asBlock(),
            blockFactory.newFloatArrayVector(new float[] { 1, 2, 4 }, 3).asBlock(),
            blockFactory.newConstantFloatBlockWith(9, 2),
            blockFactory.newFloatBlockBuilder(2).appendFloat(1).appendFloat(2).build().filter(1),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).appendFloat(2).appendFloat(5).build(),
            blockFactory.newFloatBlockBuilder(1).appendFloat(1).appendFloat(2).appendFloat(3).appendFloat(4).build(),
            blockFactory.newFloatBlockBuilder(1).appendFloat(1).appendNull().build(),
            blockFactory.newFloatBlockBuilder(1).appendFloat(1).appendNull().appendFloat(3).build(),
            blockFactory.newFloatBlockBuilder(1).appendFloat(1).appendFloat(3).build(),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1).beginPositionEntry().appendFloat(2).appendFloat(3).build()
        );
        assertAllNotEquals(notEqualBlocks);
    }

    public void testSimpleBlockWithSingleNull() {
        List<FloatBlock> blocks = List.of(
            blockFactory.newFloatBlockBuilder(3).appendFloat(1.1f).appendNull().appendFloat(3.1f).build(),
            blockFactory.newFloatBlockBuilder(3).appendFloat(1.1f).appendNull().appendFloat(3.1f).build()
        );
        assertEquals(3, blocks.get(0).getPositionCount());
        assertTrue(blocks.get(0).isNull(1));
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyNulls() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        FloatBlock.Builder builder1 = blockFactory.newFloatBlockBuilder(grow ? 0 : positions);
        FloatBlock.Builder builder2 = blockFactory.newFloatBlockBuilder(grow ? 0 : positions);
        for (int p = 0; p < positions; p++) {
            builder1.appendNull();
            builder2.appendNull();
        }
        FloatBlock block1 = builder1.build();
        FloatBlock block2 = builder2.build();
        assertEquals(positions, block1.getPositionCount());
        assertTrue(block1.mayHaveNulls());
        assertTrue(block1.isNull(0));

        List<FloatBlock> blocks = List.of(block1, block2);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithSingleMultiValue() {
        List<FloatBlock> blocks = List.of(
            blockFactory.newFloatBlockBuilder(1).beginPositionEntry().appendFloat(1.1f).appendFloat(2.2f).build(),
            blockFactory.newFloatBlockBuilder(1).beginPositionEntry().appendFloat(1.1f).appendFloat(2.2f).build()
        );
        assert blocks.get(0).getPositionCount() == 1 && blocks.get(0).getValueCount(0) == 2;
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyMultiValues() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        FloatBlock.Builder builder1 = blockFactory.newFloatBlockBuilder(grow ? 0 : positions);
        FloatBlock.Builder builder2 = blockFactory.newFloatBlockBuilder(grow ? 0 : positions);
        FloatBlock.Builder builder3 = blockFactory.newFloatBlockBuilder(grow ? 0 : positions);
        for (int pos = 0; pos < positions; pos++) {
            builder1.beginPositionEntry();
            builder2.beginPositionEntry();
            builder3.beginPositionEntry();
            int values = randomIntBetween(1, 16);
            for (int i = 0; i < values; i++) {
                float value = randomFloat();
                builder1.appendFloat(value);
                builder2.appendFloat(value);
                builder3.appendFloat(value);
            }
            builder1.endPositionEntry();
            builder2.endPositionEntry();
            builder3.endPositionEntry();
        }
        FloatBlock block1 = builder1.build();
        FloatBlock block2 = builder2.build();
        FloatBlock block3 = builder3.build();

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
