/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.RamUsageTester.Accumulator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;
import org.hamcrest.Matcher;

import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BlockAccountingTests extends ComputeTestCase {

    static final Accumulator RAM_USAGE_ACCUMULATOR = new TestRamUsageAccumulator();

    // A large(ish) upperbound simply so that effective greaterThan assertions are not unbounded
    static final long UPPER_BOUND = 10_000;

    // Array Vectors
    public void testBooleanVector() {
        BlockFactory blockFactory = blockFactory();
        Vector empty = blockFactory.newBooleanArrayVector(new boolean[] {}, 0);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(BooleanVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Vector emptyPlusOne = blockFactory.newBooleanArrayVector(new boolean[] { randomBoolean() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + 1)));

        boolean[] randomData = new boolean[randomIntBetween(2, 1024)];
        Vector emptyPlusSome = blockFactory.newBooleanArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + randomData.length)));

        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusSome.ramBytesUsed()));
        Releasables.close(empty, emptyPlusOne, emptyPlusSome, filterVector);
    }

    public void testIntVector() {
        BlockFactory blockFactory = blockFactory();
        Vector empty = blockFactory.newIntArrayVector(new int[] {}, 0);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(IntVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Vector emptyPlusOne = blockFactory.newIntArrayVector(new int[] { randomInt() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + Integer.BYTES)));

        int[] randomData = new int[randomIntBetween(2, 1024)];
        Vector emptyPlusSome = blockFactory.newIntArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + (long) Integer.BYTES * randomData.length)));

        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusSome.ramBytesUsed()));
        Releasables.close(empty, emptyPlusOne, emptyPlusSome, filterVector);
    }

    public void testLongVector() {
        BlockFactory blockFactory = blockFactory();
        Vector empty = blockFactory.newLongArrayVector(new long[] {}, 0);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(LongVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Vector emptyPlusOne = blockFactory.newLongArrayVector(new long[] { randomLong() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(empty.ramBytesUsed() + Long.BYTES));

        long[] randomData = new long[randomIntBetween(2, 1024)];
        Vector emptyPlusSome = blockFactory.newLongArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(empty.ramBytesUsed() + (long) Long.BYTES * randomData.length));

        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusSome.ramBytesUsed()));

        Releasables.close(empty, emptyPlusOne, emptyPlusSome, filterVector);
    }

    public void testDoubleVector() {
        BlockFactory blockFactory = blockFactory();
        Vector empty = blockFactory.newDoubleArrayVector(new double[] {}, 0);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(DoubleVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Vector emptyPlusOne = blockFactory.newDoubleArrayVector(new double[] { randomDouble() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(empty.ramBytesUsed() + Double.BYTES));

        double[] randomData = new double[randomIntBetween(2, 1024)];
        Vector emptyPlusSome = blockFactory.newDoubleArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(empty.ramBytesUsed() + (long) Double.BYTES * randomData.length));

        // a filter becomes responsible for it's enclosing data, both in terms of accountancy and releasability
        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusSome.ramBytesUsed()));

        Releasables.close(empty, emptyPlusOne, emptyPlusSome, filterVector);
    }

    public void testBytesRefVector() {
        BlockFactory blockFactory = blockFactory();
        var emptyArray = new BytesRefArray(0, blockFactory.bigArrays());
        var arrayWithOne = new BytesRefArray(0, blockFactory.bigArrays());
        Vector emptyVector = blockFactory.newBytesRefArrayVector(emptyArray, 0);
        long expectedEmptyVectorUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(emptyVector, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(BytesRefVectorBlock.class);
        assertThat(emptyVector.ramBytesUsed(), is(expectedEmptyVectorUsed));

        var bytesRef = new BytesRef(randomAlphaOfLengthBetween(1, 16));
        arrayWithOne.append(bytesRef);
        Vector emptyPlusOne = blockFactory.newBytesRefArrayVector(arrayWithOne, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), between(emptyVector.ramBytesUsed() + bytesRef.length, UPPER_BOUND));

        Vector filterVector = emptyPlusOne.filter(0);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
        Releasables.close(emptyVector, emptyPlusOne, filterVector);
    }

    // Array Blocks
    public void testBooleanBlock() {
        BlockFactory blockFactory = blockFactory();
        Block empty = new BooleanArrayBlock(new boolean[] {}, 0, new int[] { 0 }, null, Block.MvOrdering.UNORDERED, blockFactory);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(BooleanVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Block emptyPlusOne = new BooleanArrayBlock(
            new boolean[] { randomBoolean() },
            1,
            new int[] { 0, 1 },
            null,
            Block.MvOrdering.UNORDERED,
            blockFactory
        );
        assertThat(
            emptyPlusOne.ramBytesUsed(),
            is(empty.ramBytesUsed() + ramBytesDiffForBooleanArrays(1, 0) + ramBytesDiffForIntArrays(2, 1))
        );

        boolean[] randomData = new boolean[randomIntBetween(2, 1024)];
        int[] valueIndices = IntStream.range(0, randomData.length + 1).toArray();
        Block emptyPlusSome = new BooleanArrayBlock(
            randomData,
            randomData.length,
            valueIndices,
            null,
            Block.MvOrdering.UNORDERED,
            blockFactory
        );
        long expected = empty.ramBytesUsed() + ramBytesDiffForBooleanArrays(randomData.length, 0) + ramBytesDiffForIntArrays(
            valueIndices.length,
            1
        );
        assertThat(emptyPlusSome.ramBytesUsed(), is(expected));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
        Releasables.close(filterBlock);
    }

    public void testBooleanBlockWithNullFirstValues() {
        Block empty = new BooleanArrayBlock(
            new boolean[] {},
            0,
            null,
            BitSet.valueOf(new byte[] { 1 }),
            Block.MvOrdering.UNORDERED,
            blockFactory()
        );
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(BooleanVectorBlock.class);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(expectedEmptyUsed));
    }

    public void testIntBlock() {
        BlockFactory blockFactory = blockFactory();
        Block empty = new IntArrayBlock(new int[] {}, 0, new int[] { 0 }, null, Block.MvOrdering.UNORDERED, blockFactory);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(IntVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Block emptyPlusOne = new IntArrayBlock(
            new int[] { randomInt() },
            1,
            new int[] { 0, 1 },
            null,
            Block.MvOrdering.UNORDERED,
            blockFactory
        );
        assertThat(emptyPlusOne.ramBytesUsed(), is(empty.ramBytesUsed() + ramBytesDiffForIntArrays(1, 0) + ramBytesDiffForIntArrays(2, 1)));

        int[] randomData = new int[randomIntBetween(2, 1024)];
        int[] valueIndices = IntStream.range(0, randomData.length + 1).toArray();
        Block emptyPlusSome = new IntArrayBlock(
            randomData,
            randomData.length,
            valueIndices,
            null,
            Block.MvOrdering.UNORDERED,
            blockFactory
        );
        long expected = empty.ramBytesUsed() + ramBytesDiffForIntArrays(randomData.length, 0) + ramBytesDiffForIntArrays(
            valueIndices.length,
            1
        );
        assertThat(emptyPlusSome.ramBytesUsed(), is(expected));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
        Releasables.close(filterBlock);
    }

    public void testIntBlockWithNullFirstValues() {
        BlockFactory blockFactory = blockFactory();
        Block empty = new IntArrayBlock(new int[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED, blockFactory);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(IntVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));
    }

    public void testLongBlock() {
        BlockFactory blockFactory = blockFactory();
        Block empty = new LongArrayBlock(new long[] {}, 0, new int[] { 0 }, null, Block.MvOrdering.UNORDERED, blockFactory);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(LongVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Block emptyPlusOne = new LongArrayBlock(
            new long[] { randomInt() },
            1,
            new int[] { 0, 1 },
            null,
            Block.MvOrdering.UNORDERED,
            blockFactory
        );
        assertThat(
            emptyPlusOne.ramBytesUsed(),
            is(empty.ramBytesUsed() + ramBytesDiffForLongArrays(1, 0) + ramBytesDiffForIntArrays(2, 1))
        );

        long[] randomData = new long[randomIntBetween(2, 1024)];
        int[] valueIndices = IntStream.range(0, randomData.length + 1).toArray();
        Block emptyPlusSome = new LongArrayBlock(
            randomData,
            randomData.length,
            valueIndices,
            null,
            Block.MvOrdering.UNORDERED,
            blockFactory
        );
        long expected = empty.ramBytesUsed() + ramBytesDiffForLongArrays(randomData.length, 0) + ramBytesDiffForIntArrays(
            valueIndices.length,
            1
        );
        assertThat(emptyPlusSome.ramBytesUsed(), is(expected));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
        Releasables.close(filterBlock);
    }

    public void testLongBlockWithNullFirstValues() {
        Block empty = new LongArrayBlock(
            new long[] {},
            0,
            null,
            BitSet.valueOf(new byte[] { 1 }),
            Block.MvOrdering.UNORDERED,
            blockFactory()
        );
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(LongVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));
    }

    public void testDoubleBlock() {
        BlockFactory blockFactory = blockFactory();
        Block empty = new DoubleArrayBlock(new double[] {}, 0, new int[] { 0 }, null, Block.MvOrdering.UNORDERED, blockFactory);
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(DoubleVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Block emptyPlusOne = new DoubleArrayBlock(
            new double[] { randomInt() },
            1,
            new int[] { 0, 1 },
            null,
            Block.MvOrdering.UNORDERED,
            blockFactory
        );
        assertThat(
            emptyPlusOne.ramBytesUsed(),
            is(empty.ramBytesUsed() + ramBytesDiffForDoubleArrays(1, 0) + ramBytesDiffForIntArrays(2, 1))
        );

        double[] randomData = new double[randomIntBetween(2, 1024)];
        int[] valueIndices = IntStream.range(0, randomData.length + 1).toArray();
        Block emptyPlusSome = new DoubleArrayBlock(
            randomData,
            randomData.length,
            valueIndices,
            null,
            Block.MvOrdering.UNORDERED,
            blockFactory
        );
        long expected = empty.ramBytesUsed() + ramBytesDiffForDoubleArrays(randomData.length, 0) + ramBytesDiffForIntArrays(
            valueIndices.length,
            1
        );
        assertThat(emptyPlusSome.ramBytesUsed(), is(expected));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
        Releasables.close(filterBlock);
    }

    public void testDoubleBlockWithNullFirstValues() {
        Block empty = new DoubleArrayBlock(
            new double[] {},
            0,
            null,
            BitSet.valueOf(new byte[] { 1 }),
            Block.MvOrdering.UNORDERED,
            blockFactory()
        );
        long expectedEmptyUsed = Block.PAGE_MEM_OVERHEAD_PER_BLOCK + RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR)
            + RamUsageEstimator.shallowSizeOfInstance(DoubleVectorBlock.class);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));
    }

    static Matcher<Long> between(long minInclusive, long maxInclusive) {
        return allOf(greaterThanOrEqualTo(minInclusive), lessThanOrEqualTo(maxInclusive));
    }

    /** An accumulator that stops at BigArrays or BlockFactory. And calls ramBytesUsed on BigArray instances. */
    static class TestRamUsageAccumulator extends Accumulator {
        @Override
        public long accumulateObject(Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
            for (var entry : fieldValues.entrySet()) {
                if (entry.getKey().getType().equals(BigArrays.class) || entry.getKey().getType().equals(BlockFactory.class)) {
                    // skip BigArrays, as it is (correctly) not part of the ramBytesUsed for BytesRefArray
                } else if (o instanceof BigArray bigArray) {
                    return bigArray.ramBytesUsed();
                } else if (o instanceof Vector vector) {
                    if (Block.class.isAssignableFrom(entry.getKey().getType())) {
                        assert entry.getValue() instanceof AbstractVectorBlock;
                        // skip block views of vectors, which amount to a circular reference
                    } else {
                        queue.add(entry.getValue());
                    }
                } else if (o instanceof AbstractArrayBlock && entry.getValue() instanceof Block.MvOrdering) {
                    // skip; MvOrdering is an enum, so instances are shared
                } else {
                    queue.add(entry.getValue());
                }
            }
            return shallowSize;
        }
    }

    static long ramBytesDiffForBooleanArrays(int length1, int lenght2) {
        return ramBytesForBooleanArray(length1) - ramBytesForBooleanArray(lenght2);
    }

    static long ramBytesDiffForIntArrays(int length1, int lenght2) {
        return ramBytesForIntArray(length1) - ramBytesForIntArray(lenght2);
    }

    static long ramBytesDiffForLongArrays(int length1, int lenght2) {
        return ramBytesForLongArray(length1) - ramBytesForLongArray(lenght2);
    }

    static long ramBytesDiffForDoubleArrays(int length1, int lenght2) {
        return ramBytesForDoubleArray(length1) - ramBytesForDoubleArray(lenght2);
    }

    static long ramBytesForBooleanArray(int length) {
        return alignObjectSize((long) Byte.BYTES * length);
    }

    static long ramBytesForIntArray(int length) {
        return alignObjectSize((long) Integer.BYTES * length);
    }

    static long ramBytesForLongArray(int length) {
        return alignObjectSize((long) Long.BYTES * length);
    }

    static long ramBytesForDoubleArray(int length) {
        return alignObjectSize((long) Long.BYTES * length);
    }
}
