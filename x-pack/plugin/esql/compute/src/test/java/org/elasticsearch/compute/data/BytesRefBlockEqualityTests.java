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
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class BytesRefBlockEqualityTests extends ComputeTestCase {

    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService());
    final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testEmptyVector() {
        // all these "empty" vectors should be equivalent
        try (var bytesRefArray1 = new BytesRefArray(0, bigArrays); var bytesRefArray2 = new BytesRefArray(1, bigArrays)) {
            List<BytesRefVector> vectors = List.of(
                new BytesRefArrayVector(bytesRefArray1, 0, blockFactory),
                new BytesRefArrayVector(bytesRefArray2, 0, blockFactory),
                blockFactory.newConstantBytesRefBlockWith(new BytesRef(), 0).asVector(),
                blockFactory.newConstantBytesRefBlockWith(new BytesRef(), 0).filter().asVector(),
                blockFactory.newBytesRefBlockBuilder(0).build().asVector(),
                blockFactory.newBytesRefBlockBuilder(0).appendBytesRef(new BytesRef()).build().asVector().filter()
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
                    new int[] { 0 },
                    BitSet.valueOf(new byte[] { 0b00 }),
                    randomFrom(Block.MvOrdering.values()),
                    blockFactory
                ),
                new BytesRefArrayBlock(
                    bytesRefArray2,
                    0,
                    new int[] { 0 },
                    BitSet.valueOf(new byte[] { 0b00 }),
                    randomFrom(Block.MvOrdering.values()),
                    blockFactory
                ),
                blockFactory.newConstantBytesRefBlockWith(new BytesRef(), 0),
                blockFactory.newBytesRefBlockBuilder(0).build(),
                blockFactory.newBytesRefBlockBuilder(0).appendBytesRef(new BytesRef()).build().filter(),
                blockFactory.newBytesRefBlockBuilder(0).appendNull().build().filter()
            );
            assertAllEquals(blocks);
        }
    }

    public void testVectorEquality() {
        // all these vectors should be equivalent
        try (var bytesRefArray1 = arrayOf("1", "2", "3"); var bytesRefArray2 = arrayOf("1", "2", "3", "4")) {
            List<BytesRefVector> vectors = List.of(
                new BytesRefArrayVector(bytesRefArray1, 3, blockFactory),
                new BytesRefArrayVector(bytesRefArray1, 3, blockFactory).asBlock().asVector(),
                new BytesRefArrayVector(bytesRefArray2, 3, blockFactory),
                new BytesRefArrayVector(bytesRefArray1, 3, blockFactory).filter(0, 1, 2),
                new BytesRefArrayVector(bytesRefArray2, 4, blockFactory).filter(0, 1, 2),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .asVector(),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .asVector()
                    .filter(0, 1, 2),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .filter(0, 2, 3)
                    .asVector(),
                blockFactory.newBytesRefBlockBuilder(3)
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
                new BytesRefArrayVector(bytesRefArray1, 3, blockFactory),
                new BytesRefArrayVector(bytesRefArray1, 3, blockFactory).asBlock().asVector(),
                new BytesRefArrayVector(bytesRefArray2, 3, blockFactory),
                new BytesRefArrayVector(bytesRefArray1, 3, blockFactory).filter(0, 1, 2),
                new BytesRefArrayVector(bytesRefArray2, 4, blockFactory).filter(0, 1, 2),
                blockFactory.newConstantBytesRefBlockWith(new BytesRef("1"), 3).asVector(),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .build()
                    .asVector(),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .build()
                    .asVector()
                    .filter(0, 1, 2),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("1"))
                    .build()
                    .filter(0, 2, 3)
                    .asVector(),
                blockFactory.newBytesRefBlockBuilder(3)
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
                new BytesRefArrayVector(bytesRefArray1, 3, blockFactory).asBlock(),
                new BytesRefArrayBlock(
                    bytesRefArray1,
                    3,
                    new int[] { 0, 1, 2, 3 },
                    BitSet.valueOf(new byte[] { 0b000 }),
                    randomFrom(Block.MvOrdering.values()),
                    blockFactory
                ),
                new BytesRefArrayBlock(
                    bytesRefArray2,
                    3,
                    new int[] { 0, 1, 2, 3 },
                    BitSet.valueOf(new byte[] { 0b1000 }),
                    randomFrom(Block.MvOrdering.values()),
                    blockFactory
                ),
                new BytesRefArrayVector(bytesRefArray1, 3, blockFactory).filter(0, 1, 2).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 3, blockFactory).filter(0, 1, 2).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 4, blockFactory).filter(0, 1, 2).asBlock(),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build(),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .filter(0, 1, 2),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .build()
                    .filter(0, 2, 3),
                blockFactory.newBytesRefBlockBuilder(3)
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
                new BytesRefArrayVector(bytesRefArray1, 2, blockFactory).asBlock(),
                new BytesRefArrayBlock(
                    bytesRefArray1,
                    2,
                    new int[] { 0, 1, 2 },
                    BitSet.valueOf(new byte[] { 0b000 }),
                    randomFrom(Block.MvOrdering.values()),
                    blockFactory
                ),
                new BytesRefArrayBlock(
                    bytesRefArray2,
                    2,
                    new int[] { 0, 1, 2 },
                    BitSet.valueOf(new byte[] { 0b100 }),
                    randomFrom(Block.MvOrdering.values()),
                    blockFactory
                ),
                new BytesRefArrayVector(bytesRefArray1, 2, blockFactory).filter(0, 1).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 2, blockFactory).filter(0, 1).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 3, blockFactory).filter(0, 1).asBlock(),
                blockFactory.newConstantBytesRefBlockWith(new BytesRef("9"), 2),
                blockFactory.newBytesRefBlockBuilder(2).appendBytesRef(new BytesRef("9")).appendBytesRef(new BytesRef("9")).build(),
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(new BytesRef("9"))
                    .appendBytesRef(new BytesRef("9"))
                    .build()
                    .filter(0, 1),
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(new BytesRef("9"))
                    .appendBytesRef(new BytesRef("4"))
                    .appendBytesRef(new BytesRef("9"))
                    .build()
                    .filter(0, 2),
                blockFactory.newBytesRefBlockBuilder(2)
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
                new BytesRefArrayVector(bytesRefArray1, 1, blockFactory),
                new BytesRefArrayVector(bytesRefArray2, 1, blockFactory),
                new BytesRefArrayVector(bytesRefArray3, 2, blockFactory),
                new BytesRefArrayVector(bytesRefArray4, 3, blockFactory),
                new BytesRefArrayVector(bytesRefArray5, 3, blockFactory),
                blockFactory.newConstantBytesRefBlockWith(new BytesRef("9"), 2).asVector(),
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .build()
                    .asVector()
                    .filter(1),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("5"))
                    .build()
                    .asVector(),
                blockFactory.newBytesRefBlockBuilder(1)
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
                new BytesRefArrayVector(bytesRefArray1, 1, blockFactory).asBlock(),
                new BytesRefArrayVector(bytesRefArray2, 1, blockFactory).asBlock(),
                new BytesRefArrayVector(bytesRefArray3, 2, blockFactory).asBlock(),
                new BytesRefArrayVector(bytesRefArray4, 3, blockFactory).asBlock(),
                new BytesRefArrayVector(bytesRefArray5, 3, blockFactory).asBlock(),
                blockFactory.newConstantBytesRefBlockWith(new BytesRef("9"), 2),
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .build()
                    .filter(1),
                blockFactory.newBytesRefBlockBuilder(3)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("5"))
                    .build(),
                blockFactory.newBytesRefBlockBuilder(1)
                    .appendBytesRef(new BytesRef("1"))
                    .appendBytesRef(new BytesRef("2"))
                    .appendBytesRef(new BytesRef("3"))
                    .appendBytesRef(new BytesRef("4"))
                    .build(),
                blockFactory.newBytesRefBlockBuilder(1).appendBytesRef(new BytesRef("1")).appendNull().build(),
                blockFactory.newBytesRefBlockBuilder(1)
                    .appendBytesRef(new BytesRef("1"))
                    .appendNull()
                    .appendBytesRef(new BytesRef("3"))
                    .build(),
                blockFactory.newBytesRefBlockBuilder(1).appendBytesRef(new BytesRef("1")).appendBytesRef(new BytesRef("3")).build()
            );
            assertAllNotEquals(notEqualBlocks);
        }
    }

    public void testSimpleBlockWithSingleNull() {
        List<BytesRefBlock> blocks = List.of(
            blockFactory.newBytesRefBlockBuilder(3)
                .appendBytesRef(new BytesRef("1"))
                .appendNull()
                .appendBytesRef(new BytesRef("3"))
                .build(),
            blockFactory.newBytesRefBlockBuilder(3).appendBytesRef(new BytesRef("1")).appendNull().appendBytesRef(new BytesRef("3")).build()
        );
        assertEquals(3, blocks.get(0).getPositionCount());
        assertTrue(blocks.get(0).isNull(1));
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithManyNulls() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        BytesRefBlock.Builder builder1 = blockFactory.newBytesRefBlockBuilder(grow ? 0 : positions);
        BytesRefBlock.Builder builder2 = blockFactory.newBytesRefBlockBuilder(grow ? 0 : positions);
        for (int p = 0; p < positions; p++) {
            builder1.appendNull();
            builder2.appendNull();
        }
        BytesRefBlock block1 = builder1.build();
        BytesRefBlock block2 = builder2.build();
        assertEquals(positions, block1.getPositionCount());
        assertTrue(block1.mayHaveNulls());
        assertTrue(block1.isNull(0));

        List<BytesRefBlock> blocks = List.of(block1, block2);
        assertAllEquals(blocks);
    }

    public void testSimpleBlockWithSingleMultiValue() {
        List<BytesRefBlock> blocks = List.of(
            blockFactory.newBytesRefBlockBuilder(1)
                .beginPositionEntry()
                .appendBytesRef(new BytesRef("1a"))
                .appendBytesRef(new BytesRef("2b"))
                .build(),
            blockFactory.newBytesRefBlockBuilder(1)
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
        BytesRefBlock.Builder builder1 = blockFactory.newBytesRefBlockBuilder(grow ? 0 : positions);
        BytesRefBlock.Builder builder2 = blockFactory.newBytesRefBlockBuilder(grow ? 0 : positions);
        BytesRefBlock.Builder builder3 = blockFactory.newBytesRefBlockBuilder(grow ? 0 : positions);
        for (int pos = 0; pos < positions; pos++) {
            builder1.beginPositionEntry();
            builder2.beginPositionEntry();
            builder3.beginPositionEntry();
            int values = randomIntBetween(1, 16);
            for (int i = 0; i < values; i++) {
                BytesRef value = new BytesRef(Integer.toHexString(randomInt()));
                builder1.appendBytesRef(value);
                builder2.appendBytesRef(value);
                builder3.appendBytesRef(value);
            }
            builder1.endPositionEntry();
            builder2.endPositionEntry();
            builder3.endPositionEntry();
        }
        BytesRefBlock block1 = builder1.build();
        BytesRefBlock block2 = builder2.build();
        BytesRefBlock block3 = builder3.build();

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
