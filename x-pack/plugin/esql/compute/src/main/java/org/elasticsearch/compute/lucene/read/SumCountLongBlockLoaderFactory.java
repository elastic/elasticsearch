/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.index.mapper.BlockLoader;

import java.math.BigInteger;

/**
 * A {@link BlockLoader.BlockFactory} that never builds a block. The {@code long} values a {@link BlockLoader} loads are added, as
 * they are loaded, to an exact signed 128 bit sum and counted instead. That saves the {@code long[]} the values would be copied
 * into, the block that wraps it, and the pass over it that adds the values up.
 * <p>
 *     This works for every reader: the dense TSDB doc values hand over the {@code long[]} they just decoded to
 *     {@link BlockLoader.SingletonLongBuilder#appendLongs}, which is where the fused sum comes from, and the other readers go
 *     through the {@link BlockLoader.LongBuilder} one value at a time. Nulls are skipped and every value of a multivalued
 *     position is added, as the {@code SUM} and {@code COUNT} aggregations do.
 * </p>
 * <p>
 *     The runs the dense TSDB doc values hand over are added by the cheapest kernel that fits:
 * </p>
 * <ul>
 *     <li>A whole decoded block, {@value #BLOCK_SIZE} values from index {@code 0}, which is what nearly all of them are. A
 *     constant size, fully unrollable loop adds it into four independent 128 bit accumulators.</li>
 *     <li>Any other run of at least {@value #BLOCK_SIZE} values, such as a whole block of the larger size the time series
 *     indices use, or the middle of a slice that starts inside a block. One loop of any size adds it, and merges its four
 *     lanes into one accumulator.</li>
 *     <li>A shorter run, which is the edge of a slice, value by value.</li>
 * </ul>
 * <p>
 *     Not thread safe, and the {@link BlockLoader.Block}s it returns are placeholders to satisfy the loaders: they have nothing in
 *     them.
 * </p>
 */
public final class SumCountLongBlockLoaderFactory extends DelegatingBlockLoaderFactory {
    /** The number of values the TSDB doc values decode at a time, unless the index asks for larger blocks. */
    private static final int BLOCK_SIZE = 128;

    private static final long LOW_MASK = 0xFFFF_FFFFL;

    private static final BlockLoader.Block PLACEHOLDER_BLOCK = () -> {};

    /** Four independent accumulators, each {@code hi * 2^64 + unsigned(lo)}. */
    private long hi0, lo0, hi1, lo1, hi2, lo2, hi3, lo3;

    private long count;

    private final BlockLoader.LongBuilder longBuilder = new LongBuilder();
    private final BlockLoader.SingletonLongBuilder singletonLongBuilder = new SingletonLongBuilder();

    public SumCountLongBlockLoaderFactory(BlockFactory factory) {
        super(factory);
    }

    /** The number of values added. */
    public long count() {
        return count;
    }

    /** The exact sum of the values added. */
    public BigInteger sum() {
        return lane(hi0, lo0).add(lane(hi1, lo1)).add(lane(hi2, lo2)).add(lane(hi3, lo3));
    }

    private static BigInteger lane(long hi, long lo) {
        BigInteger unsignedLo = BigInteger.valueOf(lo & Long.MAX_VALUE).add(lo < 0 ? BigInteger.ONE.shiftLeft(63) : BigInteger.ZERO);
        return BigInteger.valueOf(hi).shiftLeft(64).add(unsignedLo);
    }

    @Override
    public BlockLoader.LongBuilder longsFromDocValues(int expectedCount) {
        return longBuilder;
    }

    @Override
    public BlockLoader.LongBuilder longs(int expectedCount) {
        return longBuilder;
    }

    @Override
    public BlockLoader.SingletonLongBuilder singletonLongs(int expectedCount) {
        return singletonLongBuilder;
    }

    @Override
    public BlockLoader.Block constantNulls(int blockCount) {
        return PLACEHOLDER_BLOCK;
    }

    /** Nothing to release, the loaders still need this to be {@link org.elasticsearch.core.Releasable}. */
    public void close() {}

    private void appendRun(long[] values, int from, int length) {
        if (length == BLOCK_SIZE && from == 0) {
            sumWholeBlock(values);
        } else if (length >= BLOCK_SIZE) {
            sumLongRun(values, from, length);
        } else {
            for (int i = 0; i < length; i++) {
                add64Lane0(values[from + i]);
            }
        }
        count += length;
    }

    /**
     * The hot loop: a constant size loop, so that it unrolls completely and the bounds are checked once. The low and the high 32
     * bits of the values are summed in separate lanes, so there is no carry to chase and no dependency between the lanes. A lane
     * adds only {@value #BLOCK_SIZE} / 4 values of at most {@code 2^32} each, so it can't overflow. The four lanes then go to four
     * independent accumulators.
     */
    private void sumWholeBlock(long[] values) {
        long low0 = 0, low1 = 0, low2 = 0, low3 = 0;
        long high0 = 0, high1 = 0, high2 = 0, high3 = 0;

        for (int i = 0; i < BLOCK_SIZE; i += 4) {
            long x0 = values[i];
            long x1 = values[i + 1];
            long x2 = values[i + 2];
            long x3 = values[i + 3];

            low0 += x0 & LOW_MASK;
            high0 += x0 >> 32;

            low1 += x1 & LOW_MASK;
            high1 += x1 >> 32;

            low2 += x2 & LOW_MASK;
            high2 += x2 >> 32;

            low3 += x3 & LOW_MASK;
            high3 += x3 >> 32;
        }

        long mid0 = high0 + (low0 >>> 32);
        long mid1 = high1 + (low1 >>> 32);
        long mid2 = high2 + (low2 >>> 32);
        long mid3 = high3 + (low3 >>> 32);

        add128Lane0(mid0 >> 32, (mid0 << 32) | (low0 & LOW_MASK));
        add128Lane1(mid1 >> 32, (mid1 << 32) | (low1 & LOW_MASK));
        add128Lane2(mid2 >> 32, (mid2 << 32) | (low2 & LOW_MASK));
        add128Lane3(mid3 >> 32, (mid3 << 32) | (low3 & LOW_MASK));
    }

    /**
     * A loop of any size, for the runs that aren't a whole block but are long enough for four lanes to pay off. Its lanes are
     * merged into one accumulator at the end of the run. A lane adds at most {@code 2^29} values of at most {@code 2^32} each,
     * so it can't overflow.
     */
    private void sumLongRun(long[] values, int from, int length) {
        long low0 = 0, low1 = 0, low2 = 0, low3 = 0;
        long high0 = 0, high1 = 0, high2 = 0, high3 = 0;
        int i = from;
        int end = from + length;
        for (; i + 3 < end; i += 4) {
            long x0 = values[i];
            long x1 = values[i + 1];
            long x2 = values[i + 2];
            long x3 = values[i + 3];

            low0 += x0 & LOW_MASK;
            high0 += x0 >> 32;
            low1 += x1 & LOW_MASK;
            high1 += x1 >> 32;
            low2 += x2 & LOW_MASK;
            high2 += x2 >> 32;
            low3 += x3 & LOW_MASK;
            high3 += x3 >> 32;
        }
        long low = low0 + low1 + low2 + low3;
        long high = high0 + high1 + high2 + high3;
        long mid = high + (low >>> 32);
        add128Lane0(mid >> 32, (mid << 32) | (low & LOW_MASK));
        for (; i < end; i++) {
            add64Lane0(values[i]);
        }
    }

    private void add128Lane0(long addHi, long addLo) {
        long nextLo = lo0 + addLo;
        long carry = Long.compareUnsigned(nextLo, lo0) < 0 ? 1L : 0L;
        hi0 += addHi + carry;
        lo0 = nextLo;
    }

    private void add128Lane1(long addHi, long addLo) {
        long nextLo = lo1 + addLo;
        long carry = Long.compareUnsigned(nextLo, lo1) < 0 ? 1L : 0L;
        hi1 += addHi + carry;
        lo1 = nextLo;
    }

    private void add128Lane2(long addHi, long addLo) {
        long nextLo = lo2 + addLo;
        long carry = Long.compareUnsigned(nextLo, lo2) < 0 ? 1L : 0L;
        hi2 += addHi + carry;
        lo2 = nextLo;
    }

    private void add128Lane3(long addHi, long addLo) {
        long nextLo = lo3 + addLo;
        long carry = Long.compareUnsigned(nextLo, lo3) < 0 ? 1L : 0L;
        hi3 += addHi + carry;
        lo3 = nextLo;
    }

    private void add64Lane0(long x) {
        long nextLo = lo0 + x;
        long carry = Long.compareUnsigned(nextLo, lo0) < 0 ? 1L : 0L;
        hi0 += (x >> 63) + carry;
        lo0 = nextLo;
    }

    private final class LongBuilder implements BlockLoader.LongBuilder {
        @Override
        public BlockLoader.LongBuilder appendLong(long value) {
            add64Lane0(value);
            count++;
            return this;
        }

        @Override
        public BlockLoader.Builder appendNull() {
            return this;
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            return this;
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            return this;
        }

        @Override
        public BlockLoader.Block build() {
            return PLACEHOLDER_BLOCK;
        }

        @Override
        public void close() {}
    }

    private final class SingletonLongBuilder implements BlockLoader.SingletonLongBuilder {
        @Override
        public BlockLoader.SingletonLongBuilder appendLong(long value) {
            add64Lane0(value);
            count++;
            return this;
        }

        @Override
        public BlockLoader.SingletonLongBuilder appendLongs(long[] values, int from, int length) {
            appendRun(values, from, length);
            return this;
        }

        @Override
        public BlockLoader.Builder appendNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Block build() {
            return PLACEHOLDER_BLOCK;
        }

        @Override
        public void close() {}
    }
}
