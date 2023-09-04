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
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.util.BitSet;

import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BlockAccountingTests extends ESTestCase {

    // These value may differ on different platform. If so, simply increase to their maximum observed value.
    // The point is to simply ensure that the bass size is "reasonably" small.
    static final long BASE_ARRAY_VECTOR_SIZE = 40L;
    static final long BASE_ARRAY_BLOCK_SIZE = 64L;

    // A large(ish) upperbound simply so that effective greaterThan assertions are not unbounded
    static final long UPPER_BOUND = 10_000;

    // Array Vectors
    public void testBooleanVector() {
        Vector empty = new BooleanArrayVector(new boolean[] {}, 0);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_VECTOR_SIZE));

        Vector emptyPlusOne = new BooleanArrayVector(new boolean[] { randomBoolean() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + 1)));

        boolean[] randomData = new boolean[randomIntBetween(1, 1024)];
        Vector emptyPlusSome = new BooleanArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + randomData.length)));

        // a filter becomes responsible for it's enclosing data, both in terms of accountancy and releasability
        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), between(emptyPlusSome.ramBytesUsed(), UPPER_BOUND));
    }

    public void testIntVector() {
        Vector empty = new IntArrayVector(new int[] {}, 0);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_VECTOR_SIZE));

        Vector emptyPlusOne = new IntArrayVector(new int[] { randomInt() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + Integer.BYTES)));

        int[] randomData = new int[randomIntBetween(1, 1024)];
        Vector emptyPlusSome = new IntArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + (long) Integer.BYTES * randomData.length)));

        // a filter becomes responsible for it's enclosing data, both in terms of accountancy and releasability
        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), between(emptyPlusSome.ramBytesUsed(), UPPER_BOUND));
    }

    public void testLongVector() {
        Vector empty = new LongArrayVector(new long[] {}, 0);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_VECTOR_SIZE));

        Vector emptyPlusOne = new LongArrayVector(new long[] { randomLong() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(empty.ramBytesUsed() + Long.BYTES));

        long[] randomData = new long[randomIntBetween(1, 1024)];
        Vector emptyPlusSome = new LongArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(empty.ramBytesUsed() + (long) Long.BYTES * randomData.length));

        // a filter becomes responsible for it's enclosing data, both in terms of accountancy and releasability
        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), between(emptyPlusSome.ramBytesUsed(), UPPER_BOUND));
    }

    public void testDoubleVector() {
        Vector empty = new DoubleArrayVector(new double[] {}, 0);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_VECTOR_SIZE));

        Vector emptyPlusOne = new DoubleArrayVector(new double[] { randomDouble() }, 1);
        assertThat(emptyPlusOne.ramBytesUsed(), is(empty.ramBytesUsed() + Double.BYTES));

        double[] randomData = new double[randomIntBetween(1, 1024)];
        Vector emptyPlusSome = new DoubleArrayVector(randomData, randomData.length);
        assertThat(emptyPlusSome.ramBytesUsed(), is(empty.ramBytesUsed() + (long) Double.BYTES * randomData.length));

        // a filter becomes responsible for it's enclosing data, both in terms of accountancy and releasability
        Vector filterVector = emptyPlusSome.filter(1);
        assertThat(filterVector.ramBytesUsed(), between(emptyPlusSome.ramBytesUsed(), UPPER_BOUND));
    }

    public void testBytesRefVector() {
        try (
            var emptyArray = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
            var arrayWithOne = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE)
        ) {

            var bytesRef = new BytesRef(randomAlphaOfLengthBetween(1, 16));
            arrayWithOne.append(bytesRef);

            Vector empty = new BytesRefArrayVector(emptyArray, 0);
            assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_VECTOR_SIZE + emptyArray.ramBytesUsed()));

            Vector emptyPlusOne = new BytesRefArrayVector(arrayWithOne, 1);
            assertThat(emptyPlusOne.ramBytesUsed(), between(empty.ramBytesUsed() + bytesRef.length, UPPER_BOUND)); // just an upper bound

            // a filter becomes responsible for it's enclosing data, both in terms of accountancy and releasability
            Vector filterVector = emptyPlusOne.filter(1);
            assertThat(filterVector.ramBytesUsed(), between(emptyPlusOne.ramBytesUsed(), UPPER_BOUND));
        }
    }

    // Array Blocks
    public void testBooleanBlock() {
        Block empty = new BooleanArrayBlock(new boolean[] {}, 0, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_BLOCK_SIZE));

        Block emptyPlusOne = new BooleanArrayBlock(new boolean[] { randomBoolean() }, 1, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + 1)));

        boolean[] randomData = new boolean[randomIntBetween(1, 1024)];
        Block emptyPlusSome = new BooleanArrayBlock(randomData, randomData.length, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + randomData.length)));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), between(emptyPlusSome.ramBytesUsed(), UPPER_BOUND));
    }

    public void testBooleanBlockWithNullFirstValues() {
        Block empty = new BooleanArrayBlock(new boolean[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_BLOCK_SIZE));
    }

    public void testIntBlock() {
        Block empty = new IntArrayBlock(new int[] {}, 0, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_BLOCK_SIZE));

        Block emptyPlusOne = new IntArrayBlock(new int[] { randomInt() }, 1, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + Integer.BYTES)));

        int[] randomData = new int[randomIntBetween(1, 1024)];
        Block emptyPlusSome = new IntArrayBlock(randomData, randomData.length, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + (long) Integer.BYTES * randomData.length)));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), between(emptyPlusSome.ramBytesUsed(), UPPER_BOUND));
    }

    public void testIntBlockWithNullFirstValues() {
        Block empty = new IntArrayBlock(new int[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_BLOCK_SIZE));
    }

    public void testLongBlock() {
        Block empty = new LongArrayBlock(new long[] {}, 0, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_BLOCK_SIZE));

        Block emptyPlusOne = new LongArrayBlock(new long[] { randomInt() }, 1, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + Long.BYTES)));

        long[] randomData = new long[randomIntBetween(1, 1024)];
        Block emptyPlusSome = new LongArrayBlock(randomData, randomData.length, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + (long) Long.BYTES * randomData.length)));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), between(emptyPlusSome.ramBytesUsed(), UPPER_BOUND));
    }

    public void testLongBlockWithNullFirstValues() {
        Block empty = new LongArrayBlock(new long[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_BLOCK_SIZE));
    }

    public void testDoubleBlock() {
        Block empty = new DoubleArrayBlock(new double[] {}, 0, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_BLOCK_SIZE));

        Block emptyPlusOne = new DoubleArrayBlock(new double[] { randomInt() }, 1, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusOne.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + Double.BYTES)));

        double[] randomData = new double[randomIntBetween(1, 1024)];
        Block emptyPlusSome = new DoubleArrayBlock(randomData, randomData.length, new int[] {}, null, Block.MvOrdering.UNORDERED);
        assertThat(emptyPlusSome.ramBytesUsed(), is(alignObjectSize(empty.ramBytesUsed() + (long) Double.BYTES * randomData.length)));

        Block filterBlock = emptyPlusSome.filter(1);
        assertThat(filterBlock.ramBytesUsed(), between(emptyPlusSome.ramBytesUsed(), UPPER_BOUND));
    }

    public void testDoubleBlockWithNullFirstValues() {
        Block empty = new DoubleArrayBlock(new double[] {}, 0, null, BitSet.valueOf(new byte[] { 1 }), Block.MvOrdering.UNORDERED);
        assertThat(empty.ramBytesUsed(), lessThanOrEqualTo(BASE_ARRAY_BLOCK_SIZE));
    }

    static Matcher<Long> between(long minInclusive, long maxInclusive) {
        return allOf(greaterThanOrEqualTo(minInclusive), lessThanOrEqualTo(maxInclusive));
    }
}
