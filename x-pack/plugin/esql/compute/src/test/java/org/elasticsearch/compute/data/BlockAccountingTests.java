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
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.test.ESTestCase;
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

public class BlockAccountingTests extends ESTestCase {

    static final Accumulator RAM_USAGE_ACCUMULATOR = new TestRamUsageAccumulator();

    // A large(ish) upperbound simply so that effective greaterThan assertions are not unbounded
    static final long UPPER_BOUND = 10_000;

    // Array Vectors
    public void testBooleanVector() {
        Vector empty = new BooleanArrayVector(new boolean[] {}, 0);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Vector emptyPlusOne = new BooleanArrayVector(new boolean[] { randomBoolean() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + 1)));

        boolean[] randomData = new boolean[randomIntBetween(2, 1024)];
        Vector emptyPlusSome = new BooleanArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + randomData.length)));

        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusSome.ramBytesUsed()));
    }

    public void testIntVector() {
        Vector empty = new IntArrayVector(new int[] {}, 0);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Vector emptyPlusOne = new IntArrayVector(new int[] { randomInt() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + Integer.BYTES)));

        int[] randomData = new int[randomIntBetween(2, 1024)];
        Vector emptyPlusSome = new IntArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + (long) Integer.BYTES * randomData.length)));

        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusSome.ramBytesUsed()));
    }

    public void testLongVector() {
        Vector empty = new LongArrayVector(new long[] {}, 0);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Vector emptyPlusOne = new LongArrayVector(new long[] { randomLong() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(empty.ramBytesUsed() + Long.BYTES));

        long[] randomData = new long[randomIntBetween(2, 1024)];
        Vector emptyPlusSome = new LongArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(empty.ramBytesUsed() + (long) Long.BYTES * randomData.length));

        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusSome.ramBytesUsed()));
    }

    public void testDoubleVector() {
        Vector empty = new DoubleArrayVector(new double[] {}, 0);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Vector emptyPlusOne = new DoubleArrayVector(new double[] { randomDouble() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(empty.ramBytesUsed() + Double.BYTES));

        double[] randomData = new double[randomIntBetween(2, 1024)];
        Vector emptyPlusSome = new DoubleArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(empty.ramBytesUsed() + (long) Double.BYTES * randomData.length));

        // a filter becomes responsible for it's enclosing data, both in terms of accountancy and releasability
        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusSome.ramBytesUsed()));
    }

    public void testBytesRefVector() {
        try (
            var emptyArray = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
            var arrayWithOne = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE)
        ) {
            Vector emptyVector = new BytesRefArrayVector(emptyArray, 0);
            long expectedEmptyVectorUsed = RamUsageTester.ramUsed(emptyVector, RAM_USAGE_ACCUMULATOR);
            assertThat(emptyVector.ramBytesUsed(), is(expectedEmptyVectorUsed));

            var bytesRef = new BytesRef(randomAlphaOfLengthBetween(1, 16));
            arrayWithOne.append(bytesRef);
            Vector emptyPlusOne = new BytesRefArrayVector(arrayWithOne, 1);
            assertThat(emptyPlusOne.ramBytesUsed(), between(emptyVector.ramBytesUsed() + bytesRef.length, UPPER_BOUND));

            Vector filterVector = emptyPlusOne.filter(0);
            assertThat(filterVector.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
        }
    }

    // Array Blocks
    public void testBooleanBlock() {
        Block empty = new BooleanArrayBlock(new boolean[] {}, 0, new int[0], null, Block.MvOrdering.UNORDERED);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Block emptyPlusOne = new BooleanArrayBlock(new boolean[] { randomBoolean() }, 1, new int[] { 0 }, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + 1) + alignObjectSize(Integer.BYTES)));

        boolean[] randomData = new boolean[randomIntBetween(2, 1024)];
        int[] valueIndices = IntStream.range(0, randomData.length + 1).toArray();
        Block emptyPlusSome = new BooleanArrayBlock(randomData, randomData.length, valueIndices, null, Block.MvOrdering.UNORDERED);
        long expected = empty.ramBytesUsed() + ramBytesForBooleanArray(randomData) + ramBytesForIntArray(valueIndices);
        assertThat(emptyPlusSome.ramBytesUsed(), is(expected));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
    }

    public void testBooleanBlockWithNullFirstValues() {
        Block empty = new BooleanArrayBlock(new boolean[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(expectedEmptyUsed));
    }

    public void testIntBlock() {
        Block empty = new IntArrayBlock(new int[] {}, 0, new int[] {}, null, Block.MvOrdering.UNORDERED);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Block emptyPlusOne = new IntArrayBlock(new int[] { randomInt() }, 1, new int[] { 0 }, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusOne.ramBytesUsed(), is(empty.ramBytesUsed() + alignObjectSize(Integer.BYTES) + alignObjectSize(Integer.BYTES)));

        int[] randomData = new int[randomIntBetween(2, 1024)];
        int[] valueIndices = IntStream.range(0, randomData.length + 1).toArray();
        Block emptyPlusSome = new IntArrayBlock(randomData, randomData.length, valueIndices, null, Block.MvOrdering.UNORDERED);
        long expected = empty.ramBytesUsed() + ramBytesForIntArray(randomData) + ramBytesForIntArray(valueIndices);
        assertThat(emptyPlusSome.ramBytesUsed(), is(expected));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
    }

    public void testIntBlockWithNullFirstValues() {
        Block empty = new IntArrayBlock(new int[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));
    }

    public void testLongBlock() {
        Block empty = new LongArrayBlock(new long[] {}, 0, new int[0], null, Block.MvOrdering.UNORDERED);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Block emptyPlusOne = new LongArrayBlock(new long[] { randomInt() }, 1, new int[] { 0 }, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + Long.BYTES) + alignObjectSize(Integer.BYTES)));

        long[] randomData = new long[randomIntBetween(2, 1024)];
        int[] valueIndices = IntStream.range(0, randomData.length + 1).toArray();
        Block emptyPlusSome = new LongArrayBlock(randomData, randomData.length, valueIndices, null, Block.MvOrdering.UNORDERED);
        long expected = empty.ramBytesUsed() + ramBytesForLongArray(randomData) + ramBytesForIntArray(valueIndices);
        assertThat(emptyPlusSome.ramBytesUsed(), is(expected));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
    }

    public void testLongBlockWithNullFirstValues() {
        Block empty = new LongArrayBlock(new long[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));
    }

    public void testDoubleBlock() {
        Block empty = new DoubleArrayBlock(new double[] {}, 0, new int[0], null, Block.MvOrdering.UNORDERED);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
        assertThat(empty.ramBytesUsed(), is(expectedEmptyUsed));

        Block emptyPlusOne = new DoubleArrayBlock(new double[] { randomInt() }, 1, new int[] { 0 }, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + Double.BYTES) + alignObjectSize(Integer.BYTES)));

        double[] randomData = new double[randomIntBetween(2, 1024)];
        int[] valueIndices = IntStream.range(0, randomData.length + 1).toArray();
        Block emptyPlusSome = new DoubleArrayBlock(randomData, randomData.length, valueIndices, null, Block.MvOrdering.UNORDERED);
        long expected = empty.ramBytesUsed() + ramBytesForDoubleArray(randomData) + ramBytesForIntArray(valueIndices);
        assertThat(emptyPlusSome.ramBytesUsed(), is(expected));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), lessThan(emptyPlusOne.ramBytesUsed()));
    }

    public void testDoubleBlockWithNullFirstValues() {
        Block empty = new DoubleArrayBlock(new double[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED);
        long expectedEmptyUsed = RamUsageTester.ramUsed(empty, RAM_USAGE_ACCUMULATOR);
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

    static long ramBytesForBooleanArray(boolean[] arr) {
        return alignObjectSize((long) Byte.BYTES * arr.length);
    }

    static long ramBytesForIntArray(int[] arr) {
        return alignObjectSize((long) Integer.BYTES * arr.length);
    }

    static long ramBytesForLongArray(long[] arr) {
        return alignObjectSize((long) Long.BYTES * arr.length);
    }

    static long ramBytesForDoubleArray(double[] arr) {
        return alignObjectSize((long) Long.BYTES * arr.length);
    }
}
