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

public class IntBlockEqualityTests extends ESTestCase {

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        List<IntVector> vectors = List.of(
            new IntArrayVector(new int[] {}, 0),
            new IntArrayVector(new int[] { 0 }, 0),
            IntBlock.newConstantBlockWith(0, 0).asVector(),
            IntBlock.newConstantBlockWith(0, 0).filter().asVector(),
            IntBlock.newBlockBuilder(0).build().asVector(),
            IntBlock.newBlockBuilder(0).appendInt(1).build().asVector().filter()
        );
        assertAllEquals(vectors);
    }

    public void testEmptyBlock() {
        // all these "empty" vectors should be equivalent
        List<IntBlock> blocks = List.of(
            new IntArrayBlock(new int[] {}, 0, new int[] {}, BitSet.valueOf(new byte[] { 0b00 })),
            new IntArrayBlock(new int[] { 0 }, 0, new int[] {}, BitSet.valueOf(new byte[] { 0b00 })),
            IntBlock.newConstantBlockWith(0, 0),
            IntBlock.newBlockBuilder(0).build(),
            IntBlock.newBlockBuilder(0).appendInt(1).build().filter(),
            IntBlock.newBlockBuilder(0).appendNull().build().filter()
        );
        assertAllEquals(blocks);
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        List<IntVector> vectors = List.of(
            new IntArrayVector(new int[] { 1, 2, 3 }, 3),
            new IntArrayVector(new int[] { 1, 2, 3 }, 3).asBlock().asVector(),
            new IntArrayVector(new int[] { 1, 2, 3, 4 }, 3),
            new IntArrayVector(new int[] { 1, 2, 3 }, 3).filter(0, 1, 2),
            new IntArrayVector(new int[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2),
            new IntArrayVector(new int[] { 0, 1, 2, 3 }, 4).filter(1, 2, 3),
            new IntArrayVector(new int[] { 1, 4, 2, 3 }, 4).filter(0, 2, 3),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build().asVector(),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build().asVector().filter(0, 1, 2),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(4).appendInt(2).appendInt(3).build().filter(0, 2, 3).asVector(),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(4).appendInt(2).appendInt(3).build().asVector().filter(0, 2, 3)
        );
        assertAllEquals(vectors);

        // all these constant-like vectors should be equivalent
        List<IntVector> moreVectors = List.of(
            new IntArrayVector(new int[] { 1, 1, 1 }, 3),
            new IntArrayVector(new int[] { 1, 1, 1 }, 3).asBlock().asVector(),
            new IntArrayVector(new int[] { 1, 1, 1, 1 }, 3),
            new IntArrayVector(new int[] { 1, 1, 1 }, 3).filter(0, 1, 2),
            new IntArrayVector(new int[] { 1, 1, 1, 4 }, 4).filter(0, 1, 2),
            new IntArrayVector(new int[] { 3, 1, 1, 1 }, 4).filter(1, 2, 3),
            new IntArrayVector(new int[] { 1, 4, 1, 1 }, 4).filter(0, 2, 3),
            IntBlock.newConstantBlockWith(1, 3).asVector(),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(1).appendInt(1).build().asVector(),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(1).appendInt(1).build().asVector().filter(0, 1, 2),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(4).appendInt(1).appendInt(1).build().filter(0, 2, 3).asVector(),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(4).appendInt(1).appendInt(1).build().asVector().filter(0, 2, 3)
        );
        assertAllEquals(moreVectors);
    }

    public void testBlockEquality() {
        // all these blocks should be equivalent
        List<IntBlock> blocks = List.of(
            new IntArrayVector(new int[] { 1, 2, 3 }, 3).asBlock(),
            new IntArrayBlock(new int[] { 1, 2, 3 }, 3, new int[] { 0, 1, 2, 3 }, BitSet.valueOf(new byte[] { 0b000 })),
            new IntArrayBlock(new int[] { 1, 2, 3, 4 }, 3, new int[] { 0, 1, 2, 3 }, BitSet.valueOf(new byte[] { 0b1000 })),
            new IntArrayVector(new int[] { 1, 2, 3 }, 3).filter(0, 1, 2).asBlock(),
            new IntArrayVector(new int[] { 1, 2, 3, 4 }, 3).filter(0, 1, 2).asBlock(),
            new IntArrayVector(new int[] { 1, 2, 3, 4 }, 4).filter(0, 1, 2).asBlock(),
            new IntArrayVector(new int[] { 1, 2, 4, 3 }, 4).filter(0, 1, 3).asBlock(),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build(),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build().filter(0, 1, 2),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(4).appendInt(2).appendInt(3).build().filter(0, 2, 3),
            IntBlock.newBlockBuilder(3).appendInt(1).appendNull().appendInt(2).appendInt(3).build().filter(0, 2, 3)
        );
        assertAllEquals(blocks);

        // all these constant-like blocks should be equivalent
        List<IntBlock> moreBlocks = List.of(
            new IntArrayVector(new int[] { 9, 9 }, 2).asBlock(),
            new IntArrayBlock(new int[] { 9, 9 }, 2, new int[] { 0, 1, 2 }, BitSet.valueOf(new byte[] { 0b000 })),
            new IntArrayBlock(new int[] { 9, 9, 4 }, 2, new int[] { 0, 1, 2 }, BitSet.valueOf(new byte[] { 0b100 })),
            new IntArrayVector(new int[] { 9, 9 }, 2).filter(0, 1).asBlock(),
            new IntArrayVector(new int[] { 9, 9, 4 }, 2).filter(0, 1).asBlock(),
            new IntArrayVector(new int[] { 9, 9, 4 }, 3).filter(0, 1).asBlock(),
            new IntArrayVector(new int[] { 9, 4, 9 }, 3).filter(0, 2).asBlock(),
            IntBlock.newConstantBlockWith(9, 2),
            IntBlock.newBlockBuilder(2).appendInt(9).appendInt(9).build(),
            IntBlock.newBlockBuilder(2).appendInt(9).appendInt(9).build().filter(0, 1),
            IntBlock.newBlockBuilder(2).appendInt(9).appendInt(4).appendInt(9).build().filter(0, 2),
            IntBlock.newBlockBuilder(2).appendInt(9).appendNull().appendInt(9).build().filter(0, 2)
        );
        assertAllEquals(moreBlocks);
    }

    public void testVectorInequality() {
        // all these vectors should NOT be equivalent
        List<IntVector> notEqualVectors = List.of(
            new IntArrayVector(new int[] { 1 }, 1),
            new IntArrayVector(new int[] { 9 }, 1),
            new IntArrayVector(new int[] { 1, 2 }, 2),
            new IntArrayVector(new int[] { 1, 2, 3 }, 3),
            new IntArrayVector(new int[] { 1, 2, 4 }, 3),
            IntBlock.newConstantBlockWith(9, 2).asVector(),
            IntBlock.newBlockBuilder(2).appendInt(1).appendInt(2).build().asVector().filter(1),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(2).appendInt(5).build().asVector(),
            IntBlock.newBlockBuilder(1).appendInt(1).appendInt(2).appendInt(3).appendInt(4).build().asVector()
        );
        assertAllNotEquals(notEqualVectors);
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        List<IntBlock> notEqualBlocks = List.of(
            new IntArrayVector(new int[] { 1 }, 1).asBlock(),
            new IntArrayVector(new int[] { 9 }, 1).asBlock(),
            new IntArrayVector(new int[] { 1, 2 }, 2).asBlock(),
            new IntArrayVector(new int[] { 1, 2, 3 }, 3).asBlock(),
            new IntArrayVector(new int[] { 1, 2, 4 }, 3).asBlock(),
            IntBlock.newConstantBlockWith(9, 2),
            IntBlock.newBlockBuilder(2).appendInt(1).appendInt(2).build().filter(1),
            IntBlock.newBlockBuilder(3).appendInt(1).appendInt(2).appendInt(5).build(),
            IntBlock.newBlockBuilder(1).appendInt(1).appendInt(2).appendInt(3).appendInt(4).build(),
            IntBlock.newBlockBuilder(1).appendInt(1).appendNull().build(),
            IntBlock.newBlockBuilder(1).appendInt(1).appendNull().appendInt(3).build(),
            IntBlock.newBlockBuilder(1).appendInt(1).appendInt(3).build(),
            IntBlock.newBlockBuilder(3).appendInt(1).beginPositionEntry().appendInt(2).appendInt(3).build()
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
