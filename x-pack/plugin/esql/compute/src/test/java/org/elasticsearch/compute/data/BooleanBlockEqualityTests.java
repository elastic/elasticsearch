/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.BitSet;
import java.util.List;

public class BooleanBlockEqualityTests extends ESTestCase {

    static final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        List<BooleanVector> vectors = List.of(
            blockFactory.newBooleanArrayVector(new boolean[] {}, 0),
            blockFactory.newBooleanArrayVector(new boolean[] { randomBoolean() }, 0),
            blockFactory.newConstantBooleanBlockWith(randomBoolean(), 0).asVector(),
            blockFactory.newConstantBooleanBlockWith(randomBoolean(), 0).filter().asVector(),
            blockFactory.newBooleanBlockBuilder(0).build().asVector(),
            blockFactory.newBooleanBlockBuilder(0).appendBoolean(randomBoolean()).build().asVector().filter()
        );
        assertAllEquals(vectors);
    }

    public void testEmptyBlock() {
        // all these "empty" vectors should be equivalent
        List<BooleanBlock> blocks = List.of(
            new BooleanArrayBlock(
                new boolean[] {},
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new BooleanArrayBlock(
                new boolean[] { randomBoolean() },
                0,
                new int[] { 0 },
                BitSet.valueOf(new byte[] { 0b00 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            blockFactory.newConstantBooleanBlockWith(randomBoolean(), 0),
            blockFactory.newBooleanBlockBuilder(0).build(),
            blockFactory.newBooleanBlockBuilder(0).appendBoolean(randomBoolean()).build().filter(),
            blockFactory.newBooleanBlockBuilder(0).appendNull().build().filter()
        );
        assertAllEquals(blocks);
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        List<BooleanVector> vectors = List.of(
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true }, 3),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true }, 3).asBlock().asVector(),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true, false }, 3),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true }, 3).filter(0, 1, 2),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true, false }, 4).filter(0, 1, 2),
            blockFactory.newBooleanArrayVector(new boolean[] { false, true, false, true }, 4).filter(1, 2, 3),
            blockFactory.newBooleanArrayVector(new boolean[] { true, true, false, true }, 4).filter(0, 2, 3),
            blockFactory.newBooleanVectorBuilder(3).appendBoolean(true).appendBoolean(false).appendBoolean(true).build(),
            blockFactory.newBooleanVectorBuilder(3).appendBoolean(true).appendBoolean(false).appendBoolean(true).build().filter(0, 1, 2),
            blockFactory.newBooleanBlockBuilder(3)
                .appendBoolean(true)
                .appendBoolean(true)
                .appendBoolean(false)
                .appendBoolean(true)
                .build()
                .filter(0, 2, 3)
                .asVector(),
            blockFactory.newBooleanBlockBuilder(3)
                .appendBoolean(true)
                .appendBoolean(true)
                .appendBoolean(false)
                .appendBoolean(true)
                .build()
                .asVector()
                .filter(0, 2, 3)
        );
        assertAllEquals(vectors);

        // all these constant-like vectors should be equivalent
        List<BooleanVector> moreVectors = List.of(
            blockFactory.newBooleanArrayVector(new boolean[] { true, true, true }, 3),
            blockFactory.newBooleanArrayVector(new boolean[] { true, true, true }, 3).asBlock().asVector(),
            blockFactory.newBooleanArrayVector(new boolean[] { true, true, true, true }, 3),
            blockFactory.newBooleanArrayVector(new boolean[] { true, true, true }, 3).filter(0, 1, 2),
            blockFactory.newBooleanArrayVector(new boolean[] { true, true, true, false }, 4).filter(0, 1, 2),
            blockFactory.newBooleanArrayVector(new boolean[] { false, true, true, true }, 4).filter(1, 2, 3),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true, true }, 4).filter(0, 2, 3),
            blockFactory.newConstantBooleanBlockWith(true, 3).asVector(),
            blockFactory.newBooleanBlockBuilder(3).appendBoolean(true).appendBoolean(true).appendBoolean(true).build().asVector(),
            blockFactory.newBooleanBlockBuilder(3)
                .appendBoolean(true)
                .appendBoolean(true)
                .appendBoolean(true)
                .build()
                .asVector()
                .filter(0, 1, 2),
            blockFactory.newBooleanBlockBuilder(3)
                .appendBoolean(true)
                .appendBoolean(false)
                .appendBoolean(true)
                .appendBoolean(true)
                .build()
                .filter(0, 2, 3)
                .asVector(),
            blockFactory.newBooleanBlockBuilder(3)
                .appendBoolean(true)
                .appendBoolean(false)
                .appendBoolean(true)
                .appendBoolean(true)
                .build()
                .asVector()
                .filter(0, 2, 3)
        );
        assertAllEquals(moreVectors);
    }

    public void testBlockEquality() {
        // all these blocks should be equivalent
        List<BooleanBlock> blocks = List.of(
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true }, 3).asBlock(),
            new BooleanArrayBlock(
                new boolean[] { true, false, true },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new BooleanArrayBlock(
                new boolean[] { true, false, true, false },
                3,
                new int[] { 0, 1, 2, 3 },
                BitSet.valueOf(new byte[] { 0b1000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true }, 3).filter(0, 1, 2).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true, false }, 3).filter(0, 1, 2).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true, false }, 4).filter(0, 1, 2).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, false, true }, 4).filter(0, 1, 3).asBlock(),
            blockFactory.newBooleanBlockBuilder(3).appendBoolean(true).appendBoolean(false).appendBoolean(true).build(),
            blockFactory.newBooleanBlockBuilder(3).appendBoolean(true).appendBoolean(false).appendBoolean(true).build().filter(0, 1, 2),
            blockFactory.newBooleanBlockBuilder(3)
                .appendBoolean(true)
                .appendBoolean(true)
                .appendBoolean(false)
                .appendBoolean(true)
                .build()
                .filter(0, 2, 3),
            blockFactory.newBooleanBlockBuilder(3)
                .appendBoolean(true)
                .appendNull()
                .appendBoolean(false)
                .appendBoolean(true)
                .build()
                .filter(0, 2, 3)
        );
        assertAllEquals(blocks);

        // all these constant-like blocks should be equivalent
        List<BooleanBlock> moreBlocks = List.of(
            blockFactory.newBooleanArrayVector(new boolean[] { true, true }, 2).asBlock(),
            new BooleanArrayBlock(
                new boolean[] { true, true },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b000 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            new BooleanArrayBlock(
                new boolean[] { true, true, false },
                2,
                new int[] { 0, 1, 2 },
                BitSet.valueOf(new byte[] { 0b100 }),
                randomFrom(Block.MvOrdering.values()),
                blockFactory
            ),
            blockFactory.newBooleanArrayVector(new boolean[] { true, true }, 2).filter(0, 1).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { true, true, false }, 2).filter(0, 1).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { true, true, false }, 3).filter(0, 1).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true }, 3).filter(0, 2).asBlock(),
            blockFactory.newConstantBooleanBlockWith(true, 2),
            blockFactory.newBooleanBlockBuilder(2).appendBoolean(true).appendBoolean(true).build(),
            blockFactory.newBooleanBlockBuilder(2).appendBoolean(true).appendBoolean(true).build().filter(0, 1),
            blockFactory.newBooleanBlockBuilder(2).appendBoolean(true).appendBoolean(true).appendBoolean(true).build().filter(0, 2),
            blockFactory.newBooleanBlockBuilder(2).appendBoolean(true).appendNull().appendBoolean(true).build().filter(0, 2)
        );
        assertAllEquals(moreBlocks);
    }

    public void testVectorInequality() {
        // all these vectors should NOT be equivalent
        List<BooleanVector> notEqualVectors = List.of(
            blockFactory.newBooleanArrayVector(new boolean[] { true }, 1),
            blockFactory.newBooleanArrayVector(new boolean[] { false }, 1),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false }, 2),
            blockFactory.newBooleanArrayVector(new boolean[] { true, false, true }, 3),
            blockFactory.newBooleanArrayVector(new boolean[] { false, true, false }, 3),
            blockFactory.newConstantBooleanBlockWith(true, 2).asVector(),
            blockFactory.newBooleanBlockBuilder(2).appendBoolean(false).appendBoolean(true).build().asVector(),
            blockFactory.newBooleanBlockBuilder(3).appendBoolean(false).appendBoolean(false).appendBoolean(true).build().asVector(),
            blockFactory.newBooleanBlockBuilder(1)
                .appendBoolean(false)
                .appendBoolean(false)
                .appendBoolean(false)
                .appendBoolean(true)
                .build()
                .asVector()
        );
        assertAllNotEquals(notEqualVectors);
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        List<BooleanBlock> notEqualBlocks = List.of(
            blockFactory.newBooleanArrayVector(new boolean[] { false }, 1).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { true }, 1).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { false, true }, 2).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { false, true, false }, 3).asBlock(),
            blockFactory.newBooleanArrayVector(new boolean[] { false, false, true }, 3).asBlock(),
            blockFactory.newConstantBooleanBlockWith(true, 2),
            blockFactory.newBooleanBlockBuilder(3).appendBoolean(true).appendBoolean(false).appendBoolean(false).build(),
            blockFactory.newBooleanBlockBuilder(1)
                .appendBoolean(true)
                .appendBoolean(false)
                .appendBoolean(true)
                .appendBoolean(false)
                .build(),
            blockFactory.newBooleanBlockBuilder(1).appendBoolean(true).appendNull().build(),
            blockFactory.newBooleanBlockBuilder(1).appendBoolean(true).appendNull().appendBoolean(false).build(),
            blockFactory.newBooleanBlockBuilder(1).appendBoolean(true).appendBoolean(false).build(),
            blockFactory.newBooleanBlockBuilder(3)
                .appendBoolean(true)
                .beginPositionEntry()
                .appendBoolean(false)
                .appendBoolean(false)
                .build()
        );
        assertAllNotEquals(notEqualBlocks);
    }

    static void assertAllEquals(List<?> objs) {
        for (Object obj1 : objs) {
            for (Object obj2 : objs) {
                assertEquals(obj1, obj2);
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
                // unequal objects SHOULD generate the different hash code
                assertNotEquals(obj1.hashCode(), obj2.hashCode());
            }
        }
    }
}
