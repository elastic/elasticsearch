/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

public class BytesRefBlockEqualityTests extends ESTestCase {

    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService());

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        try (var bytesRefArray1 = new BytesRefArray(0, bigArrays); var bytesRefArray2 = new BytesRefArray(1, bigArrays)) {
            List<BytesRefVector> vectors = List.of(
                new BytesRefArrayVector(bytesRefArray1, 0),
                new BytesRefArrayVector(bytesRefArray2, 0),
                BytesRefBlock.newConstantBlockWith(new BytesRef(), 0).asVector(),
                BytesRefBlock.newConstantBlockWith(new BytesRef(), 0).filter().asVector(),
                BytesRefBlock.newBlockBuilder(0).build().asVector(),
                BytesRefBlock.newBlockBuilder(0).appendBytesRef(new BytesRef()).build().asVector().filter()
            );
            assertAllEquals(vectors);
        }
    }

    public void testEmptyBlock() {
        // all these "empty" vectors should be equivalent
        try (var bytesRefArray1 = new BytesRefArray(0, bigArrays); var bytesRefArray2 = new BytesRefArray(1, bigArrays)) {
            List<BytesRefBlock> blocks = List.of(
                new BytesRefArrayBlock(
                    bytesRefArray1,
                    0,
                    new int[] {},
                    BitSet.valueOf(new byte[] { 0b00 }),
                    randomFrom(Block.MvOrdering.values())
                ),
                new BytesRefArrayBlock(
                    bytesRefArray2,
                    0,
                    new int[] {},
                    BitSet.valueOf(new byte[] { 0b00 }),
                    randomFrom(Block.MvOrdering.values())
                ),
                BytesRefBlock.newConstantBlockWith(new BytesRef(), 0),
                BytesRefBlock.newBlockBuilder(0).build(),
                BytesRefBlock.newBlockBuilder(0).appendBytesRef(new BytesRef()).build().filter(),
                BytesRefBlock.newBlockBuilder(0).appendNull().build().filter()
            );
            assertAllEquals(blocks);
        }
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        try (var bytesRefArray1 = arrayOf("1", "2", "3"); var bytesRefArray2 = arrayOf("1", "2", "3", "4")) {
            List<BytesRefVector> vectors = List.of(
                new BytesRefArrayVector(bytesRefArray1, 3),
                new BytesRefArrayVector(bytesRefArray1, 3).asBlock().asVector(),
                new BytesRefArrayVector(bytesRefArray2, 3),
                new BytesRefArrayVector(bytesRefArray1, 3).filter(0, 1, 2),
                new BytesRefArrayVector(bytesRefArray2, 4).filter(0, 1, 2),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .asVector(),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .asVector()
                    .filter(0, 1, 2),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .filter(0, 2, 3)
                    .asVector(),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .asVector()
                    .filter(0, 2, 3)
            );
            assertAllEquals(vectors);
        }

        // all these constant-like vectors should be equivalent
        try (var bytesRefArray1 = arrayOf("1", "1", "1"); var bytesRefArray2 = arrayOf("1", "1", "1", "4")) {
            List<BytesRefVector> moreVectors = List.of(
                new BytesRefArrayVector(bytesRefArray1, 3),
                new BytesRefArrayVector(bytesRefArray1, 3).asBlock().asVector(),
                new BytesRefArrayVector(bytesRefArray2, 3),
                new BytesRefArrayVector(bytesRefArray1, 3).filter(0, 1, 2),
                new BytesRefArrayVector(bytesRefArray2, 4).filter(0, 1, 2),
                BytesRefBlock.newConstantBlockWith(new BytesRef("1"), 3).asVector(),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .build()
                    .asVector(),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .build()
                    .asVector()
                    .filter(0, 1, 2),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .build()
                    .filter(0, 2, 3)
                    .asVector(),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .build()
                    .asVector()
                    .filter(0, 2, 3)
            );
            assertAllEquals(moreVectors);
        }
    }

    public void testBlockEquality() {
        // all these blocks should be equivalent
        try (var bytesRefArray1 = arrayOf("1", "2", "3"); var bytesRefArray2 = arrayOf("1", "2", "3", "4")) {
            List<BytesRefBlock> blocks = List.of(
                new BytesRefArrayVector(bytesRefArray1, 3).asBlock(),
                new BytesRefArrayBlock(
                    bytesRefArray1,
                    3,
                    new int[] { 0, 1, 2, 3 },
                    BitSet.valueOf(new byte[] { 0b000 }),
                    randomFrom(Block.MvOrdering.values())
                ),
                new BytesRefArrayBlock(
                    bytesRefArray2,
                    3,
                    new int[] { 0, 1, 2, 3 },
                    BitSet.valueOf(new byte[] { 0b1000 }),
                    randomFrom(Block.MvOrdering.values())
                ),
                new BytesRefArrayVector(bytesRefArray1, 3).filter(0, 1, 2).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 3).filter(0, 1, 2).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 4).filter(0, 1, 2).asBlock(),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build(),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .filter(0, 1, 2),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .filter(0, 2, 3),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendNull()
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .filter(0, 2, 3)
            );
            assertAllEquals(blocks);
        }

        // all these constant-like blocks should be equivalent
        try (var bytesRefArray1 = arrayOf("9", "9"); var bytesRefArray2 = arrayOf("9", "9", "4")) {
            List<BytesRefBlock> moreBlocks = List.of(
                new BytesRefArrayVector(bytesRefArray1, 2).asBlock(),
                new BytesRefArrayBlock(
                    bytesRefArray1,
                    2,
                    new int[] { 0, 1, 2 },
                    BitSet.valueOf(new byte[] { 0b000 }),
                    randomFrom(Block.MvOrdering.values())
                ),
                new BytesRefArrayBlock(
                    bytesRefArray2,
                    2,
                    new int[] { 0, 1, 2 },
                    BitSet.valueOf(new byte[] { 0b100 }),
                    randomFrom(Block.MvOrdering.values())
                ),
                new BytesRefArrayVector(bytesRefArray1, 2).filter(0, 1).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 2).filter(0, 1).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 3).filter(0, 1).asBlock(),
                BytesRefBlock.newConstantBlockWith(new BytesRef("9"), 2),
                BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("9")).appendBytesRef(new BytesRef("9")).build(),
                BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("9")).appendBytesRef(new BytesRef("9")).build().filter(0, 1),
                BytesRefBlock.newBlockBuilder(2)
                    .appendBytesRef(new BytesRef("9"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("9"))
                    .build()
                    .filter(0, 2),
                BytesRefBlock.newBlockBuilder(2)
                    .appendBytesRef(new BytesRef("9"))
                    .appendNull()
                    .appendBytesRef(new BytesRef("9"))
                    .build()
                    .filter(0, 2)
            );
            assertAllEquals(moreBlocks);
        }
    }

    public void testVectorInequality() {
        // all these vectors should NOT be equivalent
        try (
            var bytesRefArray1 = arrayOf("1");
            var bytesRefArray2 = arrayOf("9");
            var bytesRefArray3 = arrayOf("1", "2");
            var bytesRefArray4 = arrayOf("1", "2", "3");
            var bytesRefArray5 = arrayOf("1", "2", "4")
        ) {
            List<BytesRefVector> notEqualVectors = List.of(
                new BytesRefArrayVector(bytesRefArray1, 1),
                new BytesRefArrayVector(bytesRefArray2, 1),
                new BytesRefArrayVector(bytesRefArray3, 2),
                new BytesRefArrayVector(bytesRefArray4, 3),
                new BytesRefArrayVector(bytesRefArray5, 3),
                BytesRefBlock.newConstantBlockWith(new BytesRef("9"), 2).asVector(),
                BytesRefBlock.newBlockBuilder(2)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .build()
                    .asVector()
                    .filter(1),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("5"))
                    .build()
                    .asVector(),
                BytesRefBlock.newBlockBuilder(1)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .appendBytesRef(new BytesRef("4"))
                    .build()
                    .asVector()
            );
            assertAllNotEquals(notEqualVectors);
        }
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        try (
            var bytesRefArray1 = arrayOf("1");
            var bytesRefArray2 = arrayOf("9");
            var bytesRefArray3 = arrayOf("1", "2");
            var bytesRefArray4 = arrayOf("1", "2", "3");
            var bytesRefArray5 = arrayOf("1", "2", "4")
        ) {
            List<BytesRefBlock> notEqualBlocks = List.of(
                new BytesRefArrayVector(bytesRefArray1, 1).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 1).asBlock(),
                new BytesRefArrayVector(bytesRefArray3, 2).asBlock(),
                new BytesRefArrayVector(bytesRefArray4, 3).asBlock(),
                new BytesRefArrayVector(bytesRefArray5, 3).asBlock(),
                BytesRefBlock.newConstantBlockWith(new BytesRef("9"), 2),
                BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("1")).appendBytesRef(new BytesRef("2")).build().filter(1),
                BytesRefBlock.newBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("5"))
                    .build(),
                BytesRefBlock.newBlockBuilder(1)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .appendBytesRef(new BytesRef("4"))
                    .build(),
                BytesRefBlock.newBlockBuilder(1).appendBytesRef(new BytesRef("1")).appendNull().build(),
                BytesRefBlock.newBlockBuilder(1).appendBytesRef(new BytesRef("1")).appendNull().appendBytesRef(new BytesRef("3")).build(),
                BytesRefBlock.newBlockBuilder(1).appendBytesRef(new BytesRef("1")).appendBytesRef(new BytesRef("3")).build()
            );
            assertAllNotEquals(notEqualBlocks);
        }
    }

    public void testSimpleBlockWithSingleNull() {
        List<BytesRefBlock> blocks = List.of(
            BytesRefBlock.newBlockBuilder(3).appendBytesRef(new BytesRef("1")).appendNull().appendBytesRef(new BytesRef("3")).build(),
            BytesRefBlock.newBlockBuilder(3).appendBytesRef(new BytesRef("1")).appendNull().appendBytesRef(new BytesRef("3")).build()
        );
        assertEquals(3, blocks.get(0).getPositionCount());
        assertTrue(blocks.get(0).isNull(1));
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyNulls() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        var builder = BytesRefBlock.newBlockBuilder(grow ? 0 : positions);
        IntStream.range(0, positions).forEach(i -> builder.appendNull());
        BytesRefBlock block1 = builder.build();
        BytesRefBlock block2 = builder.build();
        assertEquals(positions, block1.getPositionCount());
        assertTrue(block1.mayHaveNulls());
        assertTrue(block1.isNull(0));

        List<BytesRefBlock> blocks = List.of(block1, block2);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithSingleMultiValue() {
        List<BytesRefBlock> blocks = List.of(
            BytesRefBlock.newBlockBuilder(1)
                .beginPositionEntry()
                .appendBytesRef(new BytesRef("1a"))
                .appendBytesRef(new BytesRef("2b"))
                .build(),
            BytesRefBlock.newBlockBuilder(1)
                .beginPositionEntry()
                .appendBytesRef(new BytesRef("1a"))
                .appendBytesRef(new BytesRef("2b"))
                .build()
        );
        assertEquals(1, blocks.get(0).getPositionCount());
        assertEquals(2, blocks.get(0).getValueCount(0));
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyMultiValues() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        var builder = BytesRefBlock.newBlockBuilder(grow ? 0 : positions);
        for (int pos = 0; pos < positions; pos++) {
            builder.beginPositionEntry();
            int values = randomIntBetween(1, 16);
            IntStream.range(0, values).forEach(i -> builder.appendBytesRef(new BytesRef(Integer.toHexString(randomInt()))));
        }
        BytesRefBlock block1 = builder.build();
        BytesRefBlock block2 = builder.build();
        BytesRefBlock block3 = builder.build();

        assertEquals(positions, block1.getPositionCount());
        assertAllEquals(List.of(block1, block2, block3));
    }

    BytesRefArray arrayOf(String... values) {
        var array = new BytesRefArray(values.length, bigArrays);
        Arrays.stream(values).map(BytesRef::new).forEach(array::append);
        return array;
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
