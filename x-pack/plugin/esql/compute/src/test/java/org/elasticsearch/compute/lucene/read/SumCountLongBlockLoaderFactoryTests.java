/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.mapper.BlockLoader;

import java.math.BigInteger;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that {@link SumCountLongBlockLoaderFactory} adds up, exactly, what the loaders hand over, whatever the shape of the runs:
 * whole blocks, the larger blocks of the time series indices, runs that start inside a block, and the short edges of a slice.
 */
public class SumCountLongBlockLoaderFactoryTests extends ComputeTestCase {
    private static final int BLOCK = 128;

    public void testWholeBlocks() {
        var factory = new SumCountLongBlockLoaderFactory(blockFactory());
        Expected expected = new Expected();
        BlockLoader.SingletonLongBuilder builder = factory.singletonLongs(0);
        for (int b = between(1, 600); b > 0; b--) {
            long[] block = randomValues(BLOCK);
            expected.add(block, 0, BLOCK);
            builder.appendLongs(block, 0, BLOCK);
        }
        expected.assertMatches(factory);
    }

    /** The shapes the loaders hand over: whole blocks, larger blocks, the ends and the middle of a slice, and nothing. */
    public void testRunShapes() {
        var factory = new SumCountLongBlockLoaderFactory(blockFactory());
        Expected expected = new Expected();
        BlockLoader.SingletonLongBuilder builder = factory.singletonLongs(0);
        long[] values = randomValues(2048);
        int[][] runs = {
            { 0, 128 },  // a whole block
            { 0, 512 },  // a whole block of the larger size
            { 0, 1024 }, // two of them
            { 128, 128 }, // the length of a block, but not the start of the array
            { 1, 127 },  // the end of a slice that starts inside a block
            { 0, 127 },  // the start of a slice that ends inside a block
            { 7, 300 },  // longer than a block, from the middle
            { 0, 129 },  // just longer than a block
            { 3, 125 },  // just shorter than a block
            { 64, 64 },
            { 5, 3 },
            { 0, 1 },
            { 0, 0 },
            { 1999, 49 } };
        for (int[] run : runs) {
            expected.add(values, run[0], run[1]);
            builder.appendLongs(values, run[0], run[1]);
        }
        expected.assertMatches(factory);
    }

    public void testSinglesAndNulls() {
        var factory = new SumCountLongBlockLoaderFactory(blockFactory());
        Expected expected = new Expected();
        BlockLoader.SingletonLongBuilder singletons = factory.singletonLongs(0);
        BlockLoader.LongBuilder longs = factory.longsFromDocValues(0);
        for (int i = between(1, 500); i > 0; i--) {
            long value = randomLong();
            expected.add(new long[] { value }, 0, 1);
            if (randomBoolean()) {
                singletons.appendLong(value);
            } else {
                longs.appendLong(value);
            }
            if (randomInt(3) == 0) {
                // Nulls and the positions of the multivalued fields add nothing.
                longs.appendNull();
                longs.beginPositionEntry();
                longs.endPositionEntry();
            }
        }
        expected.assertMatches(factory);
    }

    public void testExtremeValuesAddUpExactly() {
        var factory = new SumCountLongBlockLoaderFactory(blockFactory());
        Expected expected = new Expected();
        BlockLoader.SingletonLongBuilder builder = factory.singletonLongs(0);
        for (long extreme : new long[] { Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L, 1L }) {
            long[] values = new long[1024];
            Arrays.fill(values, extreme);
            for (int length : new int[] { BLOCK, 512, 1024, 129, 100 }) {
                for (int i = between(1, 500); i > 0; i--) {
                    expected.add(values, 0, length);
                    builder.appendLongs(values, 0, length);
                }
            }
        }
        expected.assertMatches(factory);
    }

    public void testRandomMixOfEverything() {
        var factory = new SumCountLongBlockLoaderFactory(blockFactory());
        Expected expected = new Expected();
        BlockLoader.SingletonLongBuilder builder = factory.singletonLongs(0);
        // from up to 1000 and runs of up to 512 values
        long[] values = randomValues(2048);
        for (int i = between(1, 3000); i > 0; i--) {
            int from = randomBoolean() ? 0 : between(0, 1000);
            int length = switch (between(0, 3)) {
                case 0 -> BLOCK;
                case 1 -> 512;
                case 2 -> between(0, BLOCK - 1);
                case 3 -> between(BLOCK, 500);
                default -> throw new AssertionError();
            };
            expected.add(values, from, length);
            builder.appendLongs(values, from, length);
        }
        expected.assertMatches(factory);
    }

    private static long[] randomValues(int length) {
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = randomBoolean() ? randomLong() : randomLongBetween(-1000, 1000);
        }
        return values;
    }

    private static final class Expected {
        private BigInteger sum = BigInteger.ZERO;
        private long count;

        void add(long[] values, int from, int length) {
            for (int i = 0; i < length; i++) {
                sum = sum.add(BigInteger.valueOf(values[from + i]));
            }
            count += length;
        }

        void assertMatches(SumCountLongBlockLoaderFactory factory) {
            assertThat(factory.sum(), equalTo(sum));
            assertThat(factory.count(), equalTo(count));
        }
    }
}
