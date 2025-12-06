/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// BlockFactory is used and effectively tested in many other places, but this class contains tests
// more specific to the factory implementation itself (and not necessarily tested elsewhere).
public class BlockFactoryTests extends ESTestCase {
    public static BlockFactory blockFactory(ByteSizeValue size) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, size).withCircuitBreaking();
        return new BlockFactory(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST), bigArrays);
    }

    final CircuitBreaker breaker;
    final BigArrays bigArrays;
    final BlockFactory blockFactory;

    @ParametersFactory
    public static List<Object[]> params() {
        List<Supplier<BlockFactory>> l = List.of(new Supplier<>() {
            @Override
            public BlockFactory get() {
                CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
                BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
                return BlockFactory.getInstance(breaker, bigArrays);
            }

            @Override
            public String toString() {
                return "1gb";
            }
        });
        return l.stream().map(s -> new Object[] { s }).toList();
    }

    public BlockFactoryTests(@Name("blockFactorySupplier") Supplier<BlockFactory> blockFactorySupplier) {
        this.blockFactory = blockFactorySupplier.get();
        this.breaker = blockFactory.breaker();
        this.bigArrays = blockFactory.bigArrays();
    }

    @Before
    @After
    public void checkBreaker() {
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testPreAdjusters() {
        for (int i = 0; i < 1000; i++) {
            int positions = randomIntBetween(1, 16384);
            long preAdjustBytes = blockFactory.preAdjustBreakerForBoolean(positions);
            assertThat(preAdjustBytes, is((long) positions));
            blockFactory.adjustBreaker(-preAdjustBytes);

            preAdjustBytes = blockFactory.preAdjustBreakerForInt(positions);
            assertThat(preAdjustBytes, is((long) positions * 4));
            blockFactory.adjustBreaker(-preAdjustBytes);

            preAdjustBytes = blockFactory.preAdjustBreakerForLong(positions);
            assertThat(preAdjustBytes, is((long) positions * 8));
            blockFactory.adjustBreaker(-preAdjustBytes);

            preAdjustBytes = blockFactory.preAdjustBreakerForDouble(positions);
            assertThat(preAdjustBytes, is((long) positions * 8));
            blockFactory.adjustBreaker(-preAdjustBytes);
        }
    }

    public void testIntBlockBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newIntBlockBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newIntArrayBlock(new int[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testIntBlockBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newIntBlockBuilder(randomIntBetween(0, 2048));
        builder.appendInt(randomInt());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newIntArrayBlock(new int[] { randomInt() }, 1, new int[] { 0, 1 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);

        block = blockFactory.newConstantIntBlockWith(randomInt(), randomIntBetween(1, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testIntBlockBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newIntBlockBuilder(randomIntBetween(0, 2048));

            builder.appendInt(randomInt());
            if (randomBoolean()) {  // null-ness
                builder.appendNull();
            }
            if (randomBoolean()) { // mv-ness
                builder.beginPositionEntry();
                builder.appendInt(randomInt());
                builder.appendInt(randomInt());
                builder.endPositionEntry();
            }
            builder.appendInt(randomInt());
            assertThat(breaker.getUsed(), greaterThan(0L));
            var block = builder.build();
            releaseAndAssertBreaker(block);
        }
    }

    public void testIntVectorBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newIntVectorBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newIntArrayVector(new int[] {}, 0);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testIntVectorBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newIntVectorBuilder(randomIntBetween(0, 2048));
        builder.appendInt(randomInt());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newIntArrayVector(new int[] { randomInt() }, 1);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newConstantIntBlockWith(randomInt(), randomIntBetween(1, 2048)).asVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testIntVectorBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newIntVectorBuilder(randomIntBetween(0, 2048));
            builder.appendInt(randomInt());
            if (randomBoolean()) {  // constant-ness or not
                builder.appendInt(randomInt());
            }
            assertThat(breaker.getUsed(), greaterThan(0L));
            var vector = builder.build();
            releaseAndAssertBreaker(vector);
        }
    }

    public void testLongBlockBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newLongBlockBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newLongArrayBlock(new long[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testLongBlockBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newLongBlockBuilder(randomIntBetween(0, 2048));
        builder.appendLong(randomLong());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newLongArrayBlock(new long[] { randomLong() }, 1, new int[] { 0, 1 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);

        block = blockFactory.newConstantLongBlockWith(randomLong(), randomIntBetween(1, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testLongBlockBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newLongBlockBuilder(randomIntBetween(0, 2048));

            builder.appendLong(randomLong());
            if (randomBoolean()) {  // null-ness
                builder.appendNull();
            }
            if (randomBoolean()) { // mv-ness
                builder.beginPositionEntry();
                builder.appendLong(randomInt());
                builder.appendLong(randomInt());
                builder.endPositionEntry();
            }
            builder.appendLong(randomLong());
            assertThat(breaker.getUsed(), greaterThan(0L));
            var block = builder.build();
            releaseAndAssertBreaker(block);
        }
    }

    public void testLongVectorBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newLongVectorBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newLongArrayVector(new long[] {}, 0);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testLongVectorBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newLongVectorBuilder(randomIntBetween(0, 2048));
        builder.appendLong(randomLong());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newLongArrayVector(new long[] { randomLong() }, 1);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newConstantLongBlockWith(randomLong(), randomIntBetween(1, 2048)).asVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testLongVectorBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newLongVectorBuilder(randomIntBetween(0, 2048));
            builder.appendLong(randomLong());
            if (randomBoolean()) {  // constant-ness or not
                builder.appendLong(randomLong());
            }
            assertThat(breaker.getUsed(), greaterThan(0L));
            var vector = builder.build();
            releaseAndAssertBreaker(vector);
        }
    }

    public void testDoubleBlockBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newDoubleBlockBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newDoubleArrayBlock(new double[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testDoubleBlockBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newDoubleBlockBuilder(randomIntBetween(0, 2048));
        builder.appendDouble(randomDouble());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newDoubleArrayBlock(new double[] { randomDouble() }, 1, new int[] { 0, 1 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);

        block = blockFactory.newConstantDoubleBlockWith(randomDouble(), randomIntBetween(1, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testDoubleBlockBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newDoubleBlockBuilder(randomIntBetween(0, 2048));

            builder.appendDouble(randomDouble());
            if (randomBoolean()) {  // null-ness
                builder.appendNull();
            }
            if (randomBoolean()) { // mv-ness
                builder.beginPositionEntry();
                builder.appendDouble(randomDouble());
                builder.appendDouble(randomDouble());
                builder.endPositionEntry();
            }
            builder.appendDouble(randomDouble());
            assertThat(breaker.getUsed(), greaterThan(0L));
            var block = builder.build();
            releaseAndAssertBreaker(block);
        }
    }

    public void testDoubleVectorBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newDoubleVectorBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newDoubleArrayVector(new double[] {}, 0);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testDoubleVectorBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newDoubleVectorBuilder(randomIntBetween(0, 2048));
        builder.appendDouble(randomDouble());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newDoubleArrayVector(new double[] { randomDouble() }, 1);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newConstantDoubleBlockWith(randomDouble(), randomIntBetween(1, 2048)).asVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testDoubleVectorBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newDoubleVectorBuilder(randomIntBetween(0, 2048));
            builder.appendDouble(randomDouble());
            if (randomBoolean()) {  // constant-ness or not
                builder.appendDouble(randomDouble());
            }
            assertThat(breaker.getUsed(), greaterThan(0L));
            var vector = builder.build();
            releaseAndAssertBreaker(vector);
        }
    }

    public void testFloatBlockBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newFloatBlockBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newFloatArrayBlock(new float[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testFloatBlockBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newFloatBlockBuilder(randomIntBetween(0, 2048));
        builder.appendFloat(randomFloat());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newFloatArrayBlock(new float[] { randomFloat() }, 1, new int[] { 0, 1 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);

        block = blockFactory.newConstantFloatBlockWith(randomFloat(), randomIntBetween(1, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testFloatBlockBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newFloatBlockBuilder(randomIntBetween(0, 2048));

            builder.appendFloat(randomFloat());
            if (randomBoolean()) {  // null-ness
                builder.appendNull();
            }
            if (randomBoolean()) { // mv-ness
                builder.beginPositionEntry();
                builder.appendFloat(randomFloat());
                builder.appendFloat(randomFloat());
                builder.endPositionEntry();
            }
            builder.appendFloat(randomFloat());
            assertThat(breaker.getUsed(), greaterThan(0L));
            var block = builder.build();
            releaseAndAssertBreaker(block);
        }
    }

    public void testFloatVectorBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newFloatVectorBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newFloatArrayVector(new float[] {}, 0);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testFloatVectorBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newFloatVectorBuilder(randomIntBetween(0, 2048));
        builder.appendFloat(randomFloat());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newFloatArrayVector(new float[] { randomFloat() }, 1);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newConstantFloatBlockWith(randomFloat(), randomIntBetween(1, 2048)).asVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testFloatVectorBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newFloatVectorBuilder(randomIntBetween(0, 2048));
            builder.appendFloat(randomFloat());
            if (randomBoolean()) {  // constant-ness or not
                builder.appendFloat(randomFloat());
            }
            assertThat(breaker.getUsed(), greaterThan(0L));
            var vector = builder.build();
            releaseAndAssertBreaker(vector);
        }
    }

    public void testBooleanBlockBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newBooleanBlockBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newBooleanArrayBlock(new boolean[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testBooleanBlockBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newBooleanBlockBuilder(randomIntBetween(0, 2048));
        builder.appendBoolean(randomBoolean());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        block = blockFactory.newBooleanArrayBlock(new boolean[] { randomBoolean() }, 1, new int[] { 0, 1 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);

        block = blockFactory.newConstantBooleanBlockWith(randomBoolean(), randomIntBetween(1, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testBooleanBlockBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newBooleanBlockBuilder(randomIntBetween(0, 2048));

            builder.appendBoolean(randomBoolean());
            if (randomBoolean()) {  // null-ness
                builder.appendNull();
            }
            if (randomBoolean()) { // mv-ness
                builder.beginPositionEntry();
                builder.appendBoolean(randomBoolean());
                builder.appendBoolean(randomBoolean());
                builder.endPositionEntry();
            }
            builder.appendBoolean(randomBoolean());
            assertThat(breaker.getUsed(), greaterThan(0L));
            var block = builder.build();
            releaseAndAssertBreaker(block);
        }
    }

    public void testBooleanVectorBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newBooleanVectorBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newBooleanArrayVector(new boolean[] {}, 0);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testBooleanVectorBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newBooleanVectorBuilder(randomIntBetween(0, 2048));
        builder.appendBoolean(randomBoolean());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newBooleanArrayVector(new boolean[] { randomBoolean() }, 1);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newConstantBooleanBlockWith(randomBoolean(), randomIntBetween(1, 2048)).asVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testBooleanVectorBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newBooleanVectorBuilder(randomIntBetween(0, 2048));
            builder.appendBoolean(randomBoolean());
            if (randomBoolean()) {  // constant-ness or not
                builder.appendBoolean(randomBoolean());
            }
            assertThat(breaker.getUsed(), greaterThan(0L));
            var vector = builder.build();
            releaseAndAssertBreaker(vector);
        }
    }

    public void testBytesRefBlockBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newBytesRefBlockBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        var emptyArray = new BytesRefArray(0, bigArrays);
        block = blockFactory.newBytesRefArrayBlock(emptyArray, 0, new int[] { 0 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testBytesRefBlockBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newBytesRefBlockBuilder(randomIntBetween(0, 2048));
        builder.appendBytesRef(randomBytesRef());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var block = builder.build();
        releaseAndAssertBreaker(block);

        var array = new BytesRefArray(1, bigArrays);
        array.append(randomBytesRef());
        block = blockFactory.newBytesRefArrayBlock(array, 1, new int[] { 0, 1 }, new BitSet(), randomOrdering());
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);

        block = blockFactory.newConstantBytesRefBlockWith(randomBytesRef(), randomIntBetween(1, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(block);
    }

    public void testBytesRefBlockBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newBytesRefBlockBuilder(randomIntBetween(0, 2048));

            builder.appendBytesRef(randomBytesRef());
            if (randomBoolean()) {  // null-ness
                builder.appendNull();
            }
            if (randomBoolean()) { // mv-ness
                builder.beginPositionEntry();
                builder.appendBytesRef(randomBytesRef());
                builder.appendBytesRef(randomBytesRef());
                builder.endPositionEntry();
            }
            builder.appendBytesRef(randomBytesRef());
            assertThat(breaker.getUsed(), greaterThan(0L));
            var block = builder.build();
            releaseAndAssertBreaker(block);
        }
    }

    public void testBytesRefVectorBuilderWithPossiblyLargeEstimateEmpty() {
        var builder = blockFactory.newBytesRefVectorBuilder(randomIntBetween(0, 2048));
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        var emptyArray = new BytesRefArray(0, bigArrays);
        vector = blockFactory.newBytesRefArrayVector(emptyArray, 0);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testBytesRefVectorBuilderWithPossiblyLargeEstimateSingle() {
        var builder = blockFactory.newBytesRefVectorBuilder(randomIntBetween(0, 2048));
        builder.appendBytesRef(randomBytesRef());
        assertThat(breaker.getUsed(), greaterThan(0L));
        var vector = builder.build();
        releaseAndAssertBreaker(vector);

        var array = new BytesRefArray(1, bigArrays);
        array.append(randomBytesRef());
        vector = blockFactory.newBytesRefArrayVector(array, 0);
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);

        vector = blockFactory.newConstantBytesRefBlockWith(randomBytesRef(), randomIntBetween(1, 2048)).asVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        releaseAndAssertBreaker(vector);
    }

    public void testBytesRefVectorBuilderWithPossiblyLargeEstimateRandom() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            var builder = blockFactory.newBytesRefVectorBuilder(randomIntBetween(0, 2048));
            builder.appendBytesRef(randomBytesRef());
            if (randomBoolean()) {  // constant-ness or not
                builder.appendBytesRef(randomBytesRef());
            }
            assertThat(breaker.getUsed(), greaterThan(0L));
            var vector = builder.build();
            releaseAndAssertBreaker(vector);
        }
    }

    public void testReleaseVector() {
        int positionCount = randomIntBetween(1, 10);
        IntVector vector = blockFactory.newIntArrayVector(new int[positionCount], positionCount);
        if (randomBoolean()) {
            vector.asBlock().close();
        } else {
            vector.close();
        }
        assertTrue(vector.isReleased());
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testParent() {
        long overLimit = between(1, 10);
        long maxOverLimit = randomLongBetween(overLimit, 1000);
        LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(blockFactory.breaker(), overLimit, maxOverLimit);
        BlockFactory childFactory = blockFactory.newChildFactory(localBreaker);
        assertThat(childFactory.parent(), sameInstance(blockFactory));
        assertThat(blockFactory.parent(), sameInstance(blockFactory));
        localBreaker.close();
    }

    private Block randomBlock(BlockFactory blockFactory, int positionCount) {
        return RandomBlock.randomBlock(
            blockFactory,
            randomFrom(ElementType.BYTES_REF, ElementType.LONG, ElementType.BOOLEAN),
            positionCount,
            randomBoolean(),
            between(0, 1),
            between(1, 3),
            between(0, 1),
            between(1, 3)
        ).block();
    }

    public void testAllowPassingBlockToDifferentContext() throws Exception {
        long overLimit1 = between(0, 10 * 1024);
        long maxOverLimit1 = randomLongBetween(overLimit1, 100 * 1024);
        LocalCircuitBreaker localBreaker1 = new LocalCircuitBreaker(blockFactory.breaker(), overLimit1, maxOverLimit1);
        long overLimit2 = between(0, 10 * 1024);
        long maxOverLimit2 = randomLongBetween(overLimit1, 100 * 1024);
        LocalCircuitBreaker localBreaker2 = new LocalCircuitBreaker(blockFactory.breaker(), overLimit2, maxOverLimit2);
        BlockFactory childFactory1 = blockFactory.newChildFactory(localBreaker1);
        BlockFactory childFactory2 = blockFactory.newChildFactory(localBreaker2);
        Thread[] releasingThreads = new Thread[between(1, 4)];
        Page[] passedPages = new Page[releasingThreads.length];
        for (int i = 0; i < passedPages.length; i++) {
            int positionCount = between(1, 100);
            Block[] blocks = new Block[between(1, 10)];
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = randomBlock(randomFrom(childFactory1, childFactory2), positionCount);
                blocks[b].allowPassingToDifferentDriver();
                assertThat(blocks[b].blockFactory(), equalTo(blockFactory));
            }
            passedPages[i] = new Page(blocks);
        }
        Block[] localBlocks = new Block[between(1, 100)];
        for (int i = 0; i < localBlocks.length; i++) {
            BlockFactory childFactory = randomFrom(childFactory1, childFactory2);
            localBlocks[i] = randomBlock(childFactory, between(1, 100));
            assertThat(localBlocks[i].blockFactory(), equalTo(childFactory));
        }
        CyclicBarrier barrier = new CyclicBarrier(releasingThreads.length + 1);
        for (int i = 0; i < releasingThreads.length; i++) {
            int threadIndex = i;
            releasingThreads[threadIndex] = new Thread(() -> {
                try {
                    barrier.await(30, TimeUnit.SECONDS);
                    passedPages[threadIndex].releaseBlocks();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            releasingThreads[threadIndex].start();
        }
        barrier.await(30, TimeUnit.SECONDS);
        for (Block block : localBlocks) {
            block.close();
        }
        for (Thread releasingThread : releasingThreads) {
            releasingThread.join();
        }
        assertThat(localBreaker1.getReservedBytes(), lessThanOrEqualTo(maxOverLimit1));
        assertThat(localBreaker2.getReservedBytes(), lessThanOrEqualTo(maxOverLimit2));
        localBreaker1.close();
        localBreaker2.close();
    }

    public void testOwningFactoryOfVectorBlock() {
        BlockFactory parentFactory = blockFactory(ByteSizeValue.ofBytes(between(1024, 4096)));
        LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(parentFactory.breaker(), between(0, 1024), between(0, 1024));
        BlockFactory localFactory = parentFactory.newChildFactory(localBreaker);
        int numValues = between(2, 10);
        try (var builder = localFactory.newIntVectorBuilder(numValues)) {
            for (int i = 0; i < numValues; i++) {
                builder.appendInt(randomInt());
            }
            IntBlock block = builder.build().asBlock();
            assertThat(block.blockFactory(), equalTo(localFactory));
            block.allowPassingToDifferentDriver();
            assertThat(block.blockFactory(), equalTo(parentFactory));
        }
    }

    static BytesRef randomBytesRef() {
        return new BytesRef(randomByteArrayOfLength(between(1, 20)));
    }

    static Block.MvOrdering randomOrdering() {
        return randomFrom(Block.MvOrdering.values());
    }

    <T extends Releasable & Accountable> void releaseAndAssertBreaker(T data) {
        Page page = data instanceof Block block ? new Page(block) : null;
        assertThat(breaker.getUsed(), greaterThan(0L));
        Releasables.closeExpectNoException(data);
        if (data instanceof Block block) {
            assertThat(block.isReleased(), is(true));
            Exception e = expectThrows(IllegalStateException.class, () -> page.getBlock(0));
            assertThat(e.getMessage(), containsString("can't read released block"));

            e = expectThrows(IllegalArgumentException.class, () -> new Page(block));
            assertThat(e.getMessage(), containsString("can't build page out of released blocks"));
        }
        assertThat(breaker.getUsed(), is(0L));
    }

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
    }
}
