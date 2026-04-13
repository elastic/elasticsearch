/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class BasicBlockTests extends ESTestCase {
    final CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
    final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
    final BlockFactory blockFactory = BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService))
        .build();

    @Before
    @After
    public void checkBreaker() {
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testEmpty() {
        assertZeroPositionsAndRelease(blockFactory.newIntArrayBlock(new int[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering()));
        assertZeroPositionsAndRelease(blockFactory.newIntBlockBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newIntArrayVector(new int[] {}, 0));
        assertZeroPositionsAndRelease(blockFactory.newIntVectorBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newLongArrayBlock(new long[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering()));
        assertZeroPositionsAndRelease(blockFactory.newIntRangeVector(0, 0));
        assertZeroPositionsAndRelease(blockFactory.newLongBlockBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newLongArrayVector(new long[] {}, 0));
        assertZeroPositionsAndRelease(blockFactory.newLongVectorBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newFloatArrayBlock(new float[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering()));
        assertZeroPositionsAndRelease(blockFactory.newFloatBlockBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newFloatArrayVector(new float[] {}, 0));
        assertZeroPositionsAndRelease(blockFactory.newFloatVectorBuilder(0).build());
        assertZeroPositionsAndRelease(
            blockFactory.newDoubleArrayBlock(new double[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering())
        );
        assertZeroPositionsAndRelease(blockFactory.newDoubleBlockBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newDoubleArrayVector(new double[] {}, 0));
        assertZeroPositionsAndRelease(blockFactory.newDoubleVectorBuilder(0).build());
        assertZeroPositionsAndRelease(
            blockFactory.newBytesRefArrayBlock(
                new BytesRefArray(0, blockFactory.bigArrays()),
                0,
                new int[] { 0 },
                new BitSet(),
                randomOrdering()
            )
        );
        assertZeroPositionsAndRelease(blockFactory.newBytesRefBlockBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newBytesRefArrayVector(new BytesRefArray(0, blockFactory.bigArrays()), 0));
        assertZeroPositionsAndRelease(blockFactory.newBytesRefVectorBuilder(0).build());
        assertZeroPositionsAndRelease(
            blockFactory.newBooleanArrayBlock(new boolean[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering())
        );
        assertZeroPositionsAndRelease(blockFactory.newBooleanBlockBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newBooleanArrayVector(new boolean[] {}, 0));
        assertZeroPositionsAndRelease(blockFactory.newBooleanVectorBuilder(0).build());
        assertZeroPositionsAndRelease(blockFactory.newAggregateMetricDoubleBlockBuilder(0).build());
        try (var block = blockFactory.newIntBlockBuilder(0).build()) {
            try (var sliced = block.slice(0, 0)) {
                assertThat(sliced.getPositionCount(), is(0));
            }
        }
        try (var vector = blockFactory.newIntArrayVector(new int[] {}, 0)) {
            try (var sliced = vector.slice(0, 0)) {
                assertThat(sliced.getPositionCount(), is(0));
            }
        }
        try (var block = blockFactory.newIntBlockBuilder(2).appendInt(1).appendInt(2).build()) {
            try (var sliced = block.slice(0, 0)) {
                assertThat(sliced.getPositionCount(), is(0));
            }
            try (var sliced = block.slice(1, 1)) {
                assertThat(sliced.getPositionCount(), is(0));
            }
        }
    }

    public void testSmallSingleValueDenseGrowthInt() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            try (var blockBuilder = blockFactory.newIntBlockBuilder(initialSize)) {
                IntStream.range(0, 10).forEach(blockBuilder::appendInt);
                IntBlock block = blockBuilder.build();
                assertSingleValueDenseBlock(block);
                block.close();
            }
        }
    }

    public void testSmallSingleValueDenseGrowthLong() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            try (var blockBuilder = blockFactory.newLongBlockBuilder(initialSize)) {
                IntStream.range(0, 10).forEach(blockBuilder::appendLong);
                LongBlock block = blockBuilder.build();
                assertSingleValueDenseBlock(block);
                block.close();
            }
        }
    }

    public void testSmallSingleValueDenseGrowthFloat() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            try (var blockBuilder = blockFactory.newFloatBlockBuilder(initialSize)) {
                IntStream.range(0, 10).forEach(blockBuilder::appendFloat);
                FloatBlock block = blockBuilder.build();
                assertSingleValueDenseBlock(block);
                block.close();
            }
        }
    }

    public void testSmallSingleValueDenseGrowthDouble() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            try (var blockBuilder = blockFactory.newDoubleBlockBuilder(initialSize)) {
                IntStream.range(0, 10).forEach(blockBuilder::appendDouble);
                DoubleBlock block = blockBuilder.build();
                assertSingleValueDenseBlock(block);
                block.close();
            }
        }
    }

    public void testSmallSingleValueDenseGrowthBytesRef() {
        final BytesRef NULL_VALUE = new BytesRef();
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            try (var blockBuilder = blockFactory.newBytesRefBlockBuilder(initialSize)) {
                IntStream.range(0, 10).mapToObj(i -> NULL_VALUE).forEach(blockBuilder::appendBytesRef);
                BytesRefBlock block = blockBuilder.build();
                assertSingleValueDenseBlock(block);
                block.close();
            }
        }
    }

    public void testSmallSingleValueDenseGrowthBoolean() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            try (var blockBuilder = blockFactory.newBooleanBlockBuilder(initialSize)) {
                IntStream.range(0, 10).forEach(i -> blockBuilder.appendBoolean(i % 3 == 0));
                BooleanBlock block = blockBuilder.build();
                assertSingleValueDenseBlock(block);
                block.close();
            }
        }
    }

    public void testSmallSingleValueDenseGrowthAggregateMetricDouble() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            try (var blockBuilder = blockFactory.newBooleanBlockBuilder(initialSize)) {
                IntStream.range(0, 10).forEach(i -> blockBuilder.appendBoolean(i % 3 == 0));
                BooleanBlock block = blockBuilder.build();
                assertSingleValueDenseBlock(block);
                block.close();
            }
        }
    }

    static void assertSingleValueDenseBlock(Block initialBlock) {
        final int positionCount = initialBlock.getPositionCount();
        int depth = randomIntBetween(1, 5);
        for (int d = 0; d < depth; d++) {
            Block block = initialBlock;
            assertThat(block.getTotalValueCount(), is(positionCount));
            assertThat(block.getPositionCount(), is(positionCount));
            for (int j = 0; j < 10; j++) {
                int pos = randomPosition(positionCount);
                assertThat(block.getFirstValueIndex(pos), is(pos));
                assertThat(block.getValueCount(pos), is(1));
                assertThat(block.isNull(pos), is(false));
            }
            assertThat(block.asVector().getPositionCount(), is(positionCount));
            assertThat(block.asVector().asBlock().getTotalValueCount(), is(positionCount));
            assertThat(block.asVector().asBlock().getPositionCount(), is(positionCount));
            assertThat(block.mayHaveNulls(), is(false));
            assertThat(block.areAllValuesNull(), is(false));
            assertThat(block.mayHaveMultivaluedFields(), is(false));
            assertThat(block.doesHaveMultivaluedFields(), is(false));

            initialBlock = block.asVector().asBlock();
        }
        assertKeepMask(initialBlock);
        assertKeepMask(initialBlock.asVector());
        assertFilter(initialBlock);
        assertFilter(initialBlock.asVector());
        assertSlice(initialBlock);
        assertSlice(initialBlock.asVector());
        assertDeepCopy(initialBlock);
    }

    public void testIntBlock() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = randomIntBetween(1, 16 * 1024);
            IntBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                try (IntBlock.Builder blockBuilder = blockFactory.newIntBlockBuilder(builderEstimateSize)) {
                    IntStream.range(0, positionCount).forEach(blockBuilder::appendInt);
                    block = blockBuilder.build();
                }
            } else {
                block = blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount).asBlock();
            }

            assertThat(block.getPositionCount(), equalTo(positionCount));
            assertThat(block.getInt(0), equalTo(0));
            assertThat(block.getInt(positionCount - 1), equalTo(positionCount - 1));
            int pos = block.getInt(randomPosition(positionCount));
            assertThat(pos, is(block.getInt(pos)));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(block, positions(blockFactory, 1, 2, new int[] { 1, 2 }), List.of(List.of(1), List.of(2), List.of(1, 2)));
            }
            assertLookup(block, positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, block);
            assertThat(block.asVector().min(), equalTo(0));
            assertThat(block.asVector().max(), equalTo(positionCount - 1));
            assertDeepCopy(block);

            try (IntBlock.Builder blockBuilder = blockFactory.newIntBlockBuilder(1)) {
                IntBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
                assertThat(copy, equalTo(block));
                assertInsertNulls(block);
                releaseAndAssertBreaker(block, copy);
            }

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    blockFactory::newIntBlockBuilder,
                    IntBlock.Builder::appendInt,
                    position -> position,
                    IntBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat(randomNonNullPosition, is(b.getInt(randomNonNullPosition.intValue())));
                    }
                );
            }

            try (
                IntVector.Builder vectorBuilder = blockFactory.newIntVectorBuilder(
                    randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
                )
            ) {
                IntStream.range(0, positionCount).forEach(vectorBuilder::appendInt);
                IntVector vector = vectorBuilder.build();
                assertSingleValueDenseBlock(vector.asBlock());
                assertThat(vector.min(), equalTo(0));
                assertThat(vector.max(), equalTo(positionCount - 1));
                assertInsertNulls(vector.asBlock());
                assertDeepCopy(vector.asBlock());
                releaseAndAssertBreaker(vector.asBlock());
            }
        }
    }

    public void testIntRangeVector() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = between(1, 16 * 1024);
            int start = randomBoolean() ? 0 : between(1, 16 * 1024);
            IntVector vector = blockFactory.newIntRangeVector(start, start + positionCount);
            assertThat(vector.getPositionCount(), equalTo(positionCount));
            for (int p = 0; p < positionCount; p++) {
                assertThat(vector.getInt(p), equalTo(start + p));
            }
            assertSingleValueDenseBlock(vector.asBlock());
            if (positionCount > 2) {
                assertLookup(
                    vector.asBlock(),
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    List.of(List.of(start + 1), List.of(start + 2), List.of(start + 1, start + 2))
                );
            }
            assertLookup(vector.asBlock(), positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, vector.asBlock());
            assertThat(vector.min(), equalTo(start));
            assertThat(vector.max(), equalTo(start + positionCount - 1));
            assertDeepCopy(vector.asBlock());
            assertSingleValueDenseBlock(vector.asBlock());

            try (IntBlock.Builder blockBuilder = blockFactory.newIntBlockBuilder(1)) {
                IntBlock copy = blockBuilder.copyFrom(vector.asBlock(), 0, vector.getPositionCount()).build();
                assertThat(copy, equalTo(vector.asBlock()));
                assertInsertNulls(vector.asBlock());
                releaseAndAssertBreaker(vector.asBlock(), copy);
            }
        }
    }

    public void testIntBlockEmpty() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            IntBlock block;
            if (randomBoolean()) {
                try (IntBlock.Builder blockBuilder = blockFactory.newIntBlockBuilder(0)) {
                    block = blockBuilder.build();
                }
            } else {
                block = blockFactory.newIntArrayVector(new int[] {}, 0).asBlock();
            }

            assertThat(block.getPositionCount(), equalTo(0));
            assertLookup(block, positions(blockFactory, 1000), singletonList(null));
            assertEmptyLookup(blockFactory, block);
            assertThat(block.asVector().min(), equalTo(Integer.MAX_VALUE));
            assertThat(block.asVector().max(), equalTo(Integer.MIN_VALUE));
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);

            try (IntVector.Builder vectorBuilder = blockFactory.newIntVectorBuilder(0)) {
                IntVector vector = vectorBuilder.build();
                assertThat(vector.min(), equalTo(Integer.MAX_VALUE));
                assertThat(vector.max(), equalTo(Integer.MIN_VALUE));
                assertInsertNulls(vector.asBlock());
                assertDeepCopy(vector.asBlock());
                releaseAndAssertBreaker(vector.asBlock());
            }
        }
    }

    public void testConstantIntBlock() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = randomIntBetween(1, 16 * 1024);
            int value = randomInt();
            IntBlock block = blockFactory.newConstantIntBlockWith(value, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getInt(0)));
            assertThat(value, is(block.getInt(positionCount - 1)));
            assertThat(value, is(block.getInt(randomPosition(positionCount))));
            assertThat(block.isNull(randomPosition(positionCount)), is(false));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    List.of(List.of(value), List.of(value), List.of(value, value))
                );
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2),
                    List.of(List.of(value), List.of(value)),
                    b -> assertThat(b.asVector(), instanceOf(ConstantIntVector.class))
                );
            }
            assertLookup(
                block,
                positions(blockFactory, positionCount + 1000),
                singletonList(null),
                b -> assertThat(b, instanceOf(ConstantNullBlock.class))
            );
            assertEmptyLookup(blockFactory, block);
            assertThat(block.asVector().min(), equalTo(value));
            assertThat(block.asVector().max(), equalTo(value));
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
        }
    }

    public void testLongBlock() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = randomIntBetween(1, 16 * 1024);
            LongBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                LongBlock.Builder blockBuilder = blockFactory.newLongBlockBuilder(builderEstimateSize);
                LongStream.range(0, positionCount).forEach(blockBuilder::appendLong);
                block = blockBuilder.build();
            } else {
                block = blockFactory.newLongArrayVector(LongStream.range(0, positionCount).toArray(), positionCount).asBlock();
            }

            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0L, is(block.getLong(0)));
            assertThat((long) positionCount - 1, is(block.getLong(positionCount - 1)));
            int pos = (int) block.getLong(randomPosition(positionCount));
            assertThat((long) pos, is(block.getLong(pos)));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(block, positions(blockFactory, 1, 2, new int[] { 1, 2 }), List.of(List.of(1L), List.of(2L), List.of(1L, 2L)));
            }
            assertLookup(block, positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, block);

            try (LongBlock.Builder blockBuilder = blockFactory.newLongBlockBuilder(1)) {
                LongBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
                assertThat(copy, equalTo(block));
                assertInsertNulls(block);
                assertDeepCopy(block);
                releaseAndAssertBreaker(block, copy);
            }

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    blockFactory::newLongBlockBuilder,
                    LongBlock.Builder::appendLong,
                    position -> (long) position,
                    LongBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat((long) randomNonNullPosition, is(b.getLong(randomNonNullPosition.intValue())));
                    }
                );
            }

            LongVector.Builder vectorBuilder = blockFactory.newLongVectorBuilder(
                randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
            );
            LongStream.range(0, positionCount).forEach(vectorBuilder::appendLong);
            LongVector vector = vectorBuilder.build();
            assertSingleValueDenseBlock(vector.asBlock());
            assertInsertNulls(vector.asBlock());
            assertDeepCopy(vector.asBlock());
            releaseAndAssertBreaker(vector.asBlock());
        }
    }

    public void testConstantLongBlock() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = randomIntBetween(1, 16 * 1024);
            long value = randomLong();
            LongBlock block = blockFactory.newConstantLongBlockWith(value, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getLong(0)));
            assertThat(value, is(block.getLong(positionCount - 1)));
            assertThat(value, is(block.getLong(randomPosition(positionCount))));
            assertThat(block.isNull(randomPosition(positionCount)), is(false));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    List.of(List.of(value), List.of(value), List.of(value, value))
                );
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2),
                    List.of(List.of(value), List.of(value)),
                    b -> assertThat(b.asVector(), instanceOf(ConstantLongVector.class))
                );
            }
            assertLookup(
                block,
                positions(blockFactory, positionCount + 1000),
                singletonList(null),
                b -> assertThat(b, instanceOf(ConstantNullBlock.class))
            );
            assertEmptyLookup(blockFactory, block);
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
        }
    }

    public void testDoubleBlock() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = randomIntBetween(1, 16 * 1024);
            DoubleBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                var blockBuilder = blockFactory.newDoubleBlockBuilder(builderEstimateSize);
                LongStream.range(0, positionCount).asDoubleStream().forEach(blockBuilder::appendDouble);
                block = blockBuilder.build();
            } else {
                block = blockFactory.newDoubleArrayVector(LongStream.range(0, positionCount).asDoubleStream().toArray(), positionCount)
                    .asBlock();
            }

            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0d, is(block.getDouble(0)));
            assertThat((double) positionCount - 1, is(block.getDouble(positionCount - 1)));
            int pos = (int) block.getDouble(randomPosition(positionCount));
            assertThat((double) pos, is(block.getDouble(pos)));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(block, positions(blockFactory, 1, 2, new int[] { 1, 2 }), List.of(List.of(1d), List.of(2d), List.of(1d, 2d)));
            }
            assertLookup(block, positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, block);

            try (DoubleBlock.Builder blockBuilder = blockFactory.newDoubleBlockBuilder(1)) {
                DoubleBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
                assertThat(copy, equalTo(block));
                assertInsertNulls(block);
                assertDeepCopy(block);
                releaseAndAssertBreaker(block, copy);
            }

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    blockFactory::newDoubleBlockBuilder,
                    DoubleBlock.Builder::appendDouble,
                    position -> (double) position,
                    DoubleBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat((double) randomNonNullPosition, is(b.getDouble(randomNonNullPosition.intValue())));
                    }
                );
            }

            try (
                DoubleVector.Builder vectorBuilder = blockFactory.newDoubleVectorBuilder(
                    randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
                )
            ) {
                IntStream.range(0, positionCount).mapToDouble(ii -> 1.0 / ii).forEach(vectorBuilder::appendDouble);
                DoubleVector vector = vectorBuilder.build();
                assertSingleValueDenseBlock(vector.asBlock());
                assertInsertNulls(vector.asBlock());
                assertDeepCopy(vector.asBlock());
                releaseAndAssertBreaker(vector.asBlock());
            }
        }
    }

    public void testConstantDoubleBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            double value = randomDouble();
            DoubleBlock block = blockFactory.newConstantDoubleBlockWith(value, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getDouble(0)));
            assertThat(value, is(block.getDouble(positionCount - 1)));
            assertThat(value, is(block.getDouble(randomPosition(positionCount))));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    List.of(List.of(value), List.of(value), List.of(value, value))
                );
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2),
                    List.of(List.of(value), List.of(value)),
                    b -> assertThat(b.asVector(), instanceOf(ConstantDoubleVector.class))
                );
            }
            assertLookup(
                block,
                positions(blockFactory, positionCount + 1000),
                singletonList(null),
                b -> assertThat(b, instanceOf(ConstantNullBlock.class))
            );
            assertEmptyLookup(blockFactory, block);
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
        }
    }

    public void testFloatBlock() {
        for (int i = 0; i < 1000; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = randomIntBetween(1, 16 * 1024);
            FloatBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                var blockBuilder = blockFactory.newFloatBlockBuilder(builderEstimateSize);
                IntStream.range(0, positionCount).forEach(blockBuilder::appendFloat);
                block = blockBuilder.build();
            } else {
                float[] fa = new float[positionCount];
                IntStream.range(0, positionCount).forEach(v -> fa[v] = (float) v);
                block = blockFactory.newFloatArrayVector(fa, positionCount).asBlock();
            }

            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0f, is(block.getFloat(0)));
            assertThat((float) positionCount - 1, is(block.getFloat(positionCount - 1)));
            int pos = (int) block.getFloat(randomPosition(positionCount));
            assertThat((float) pos, is(block.getFloat(pos)));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(block, positions(blockFactory, 1, 2, new int[] { 1, 2 }), List.of(List.of(1f), List.of(2f), List.of(1f, 2f)));
            }
            assertLookup(block, positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, block);

            try (FloatBlock.Builder blockBuilder = blockFactory.newFloatBlockBuilder(1)) {
                FloatBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
                assertThat(copy, equalTo(block));
                assertInsertNulls(block);
                assertDeepCopy(block);
                releaseAndAssertBreaker(block, copy);
            }

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    blockFactory::newFloatBlockBuilder,
                    FloatBlock.Builder::appendFloat,
                    position -> (float) position,
                    FloatBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat((float) randomNonNullPosition, is(b.getFloat(randomNonNullPosition.intValue())));
                    }
                );
            }

            try (
                DoubleVector.Builder vectorBuilder = blockFactory.newDoubleVectorBuilder(
                    randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
                )
            ) {
                IntStream.range(0, positionCount).mapToDouble(ii -> 1.0 / ii).forEach(vectorBuilder::appendDouble);
                DoubleVector vector = vectorBuilder.build();
                assertSingleValueDenseBlock(vector.asBlock());
                assertInsertNulls(vector.asBlock());
                assertDeepCopy(vector.asBlock());
                releaseAndAssertBreaker(vector.asBlock());
            }
        }
    }

    public void testConstantFloatBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            float value = randomFloat();
            FloatBlock block = blockFactory.newConstantFloatBlockWith(value, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getFloat(0)));
            assertThat(value, is(block.getFloat(positionCount - 1)));
            assertThat(value, is(block.getFloat(randomPosition(positionCount))));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    List.of(List.of(value), List.of(value), List.of(value, value))
                );
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2),
                    List.of(List.of(value), List.of(value)),
                    b -> assertThat(b.asVector(), instanceOf(ConstantFloatVector.class))
                );
            }
            assertLookup(
                block,
                positions(blockFactory, positionCount + 1000),
                singletonList(null),
                b -> assertThat(b, instanceOf(ConstantNullBlock.class))
            );
            assertEmptyLookup(blockFactory, block);
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
        }
    }

    private void testBytesRefBlock(Supplier<BytesRef> byteArraySupplier, boolean chomp, org.mockito.ThrowingConsumer<BytesRef> assertions) {
        int positionCount = randomIntBetween(1, 16 * 1024);
        BytesRef[] values = new BytesRef[positionCount];
        for (int i = 0; i < positionCount; i++) {
            BytesRef bytesRef = byteArraySupplier.get();
            if (chomp && bytesRef.length > 0 && randomBoolean()) {
                bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
            }
            values[i] = bytesRef;
        }

        BytesRefBlock block;
        if (randomBoolean()) {
            final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
            try (var blockBuilder = blockFactory.newBytesRefBlockBuilder(builderEstimateSize)) {
                Arrays.stream(values).map(obj -> randomBoolean() ? obj : BytesRef.deepCopyOf(obj)).forEach(blockBuilder::appendBytesRef);
                block = blockBuilder.build();
            }
        } else {
            BytesRefArray array = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
            Arrays.stream(values).forEach(array::append);
            block = blockFactory.newBytesRefArrayVector(array, positionCount).asBlock();
        }

        assertThat(positionCount, is(block.getPositionCount()));
        BytesRef bytes = new BytesRef();
        for (int i = 0; i < positionCount; i++) {
            int pos = randomIntBetween(0, positionCount - 1);
            bytes = block.getBytesRef(pos, bytes);
            assertThat(bytes, equalTo(values[pos]));
            assertions.accept(bytes);
        }
        assertSingleValueDenseBlock(block);
        if (positionCount > 2) {
            assertLookup(
                block,
                positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                List.of(List.of(values[1]), List.of(values[2]), List.of(values[1], values[2]))
            );
        }
        assertLookup(block, positions(blockFactory, positionCount + 1000), singletonList(null));
        assertEmptyLookup(blockFactory, block);

        try (BytesRefBlock.Builder blockBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            BytesRefBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
            assertThat(copy, equalTo(block));
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block, copy);
        }

        if (positionCount > 1) {
            assertNullValues(
                positionCount,
                blockFactory::newBytesRefBlockBuilder,
                BytesRefBlock.Builder::appendBytesRef,
                position -> values[position],
                BytesRefBlock.Builder::build,
                (randomNonNullPosition, b) -> assertThat(
                    values[randomNonNullPosition],
                    is(b.getBytesRef(randomNonNullPosition, new BytesRef()))
                )
            );
        }

        try (
            BytesRefVector.Builder vectorBuilder = blockFactory.newBytesRefVectorBuilder(
                randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
            )
        ) {
            IntStream.range(0, positionCount).mapToObj(ii -> new BytesRef(randomAlphaOfLength(5))).forEach(vectorBuilder::appendBytesRef);
            BytesRefVector vector = vectorBuilder.build();
            assertSingleValueDenseBlock(vector.asBlock());
            assertInsertNulls(vector.asBlock());
            assertDeepCopy(vector.asBlock());
            releaseAndAssertBreaker(vector.asBlock());
        }
    }

    public void testBytesRefBlock() {
        testBytesRefBlock(() -> new BytesRef(randomByteArrayOfLength(between(1, 20))), true, b -> {});
    }

    public void testBytesRefBlockOnGeoPoints() {
        testBytesRefBlock(
            () -> new BytesRef(WellKnownBinary.toWKB(GeometryTestUtils.randomPoint(), ByteOrder.LITTLE_ENDIAN)),
            false,
            wkb -> WellKnownText.fromWKB(wkb.bytes, wkb.offset, wkb.length)
        );
    }

    public void testBytesRefBlockOnCartesianPoints() {
        testBytesRefBlock(
            () -> new BytesRef(WellKnownBinary.toWKB(ShapeTestUtils.randomPoint(), ByteOrder.LITTLE_ENDIAN)),
            false,
            wkb -> WellKnownText.fromWKB(wkb.bytes, wkb.offset, wkb.length)
        );
    }

    public void testAppendPagedBytesCursorVector() {
        byte[] data = randomByteArrayOfLength(between(1, 100));
        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(data, 0, data.length);
        try (
            BytesRefVector.Builder cursorBuilder = blockFactory.newBytesRefVectorBuilder(1);
            BytesRefVector.Builder bytesRefBuilder = blockFactory.newBytesRefVectorBuilder(1)
        ) {
            cursorBuilder.append(cursor);
            bytesRefBuilder.appendBytesRef(new BytesRef(data));
            try (BytesRefVector v1 = cursorBuilder.build(); BytesRefVector v2 = bytesRefBuilder.build()) {
                assertThat(BytesRefVector.equals(v1, v2), is(true));
            }
        }
    }

    public void testAppendPagedBytesCursorBlock() {
        byte[] data = randomByteArrayOfLength(between(1, 100));
        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(data, 0, data.length);
        try (
            BytesRefBlock.Builder cursorBuilder = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder bytesRefBuilder = blockFactory.newBytesRefBlockBuilder(1)
        ) {
            cursorBuilder.append(cursor);
            bytesRefBuilder.appendBytesRef(new BytesRef(data));
            try (BytesRefBlock b1 = cursorBuilder.build(); BytesRefBlock b2 = bytesRefBuilder.build()) {
                assertThat(BytesRefBlock.equals(b1, b2), is(true));
            }
        }
    }

    public void testBytesRefBlockBuilderWithNulls() {
        int positionCount = randomIntBetween(0, 16 * 1024);
        final int builderEstimateSize = positionCount == 0 ? 0 : (randomBoolean() ? randomIntBetween(1, positionCount) : positionCount);
        try (var blockBuilder = blockFactory.newBytesRefBlockBuilder(builderEstimateSize)) {
            BytesRef[] values = new BytesRef[positionCount];
            for (int i = 0; i < positionCount; i++) {
                if (randomBoolean()) {
                    // Add random sparseness
                    blockBuilder.appendNull();
                    values[i] = null;
                } else {
                    BytesRef bytesRef = new BytesRef(randomByteArrayOfLength(between(1, 20)));
                    if (bytesRef.length > 0 && randomBoolean()) {
                        bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                        bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
                    }
                    values[i] = bytesRef;
                    if (randomBoolean()) {
                        bytesRef = BytesRef.deepCopyOf(bytesRef);
                    }
                    blockBuilder.appendBytesRef(bytesRef);
                }
            }
            BytesRefBlock block = blockBuilder.build();

            assertThat(positionCount, is(block.getPositionCount()));
            BytesRef bytes = new BytesRef();
            for (int i = 0; i < positionCount; i++) {
                int pos = randomIntBetween(0, positionCount - 1);
                bytes = block.getBytesRef(pos, bytes);
                if (values[pos] == null) {
                    assertThat(block.isNull(pos), equalTo(true));
                    assertThat(bytes, equalTo(new BytesRef()));
                } else {
                    assertThat(bytes, equalTo(values[pos]));
                    assertThat(block.getBytesRef(pos, bytes), equalTo(values[pos]));
                }
            }
            assertKeepMask(block);
            assertFilter(block);
            assertSlice(block);
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
        }
    }

    public void testConstantBytesRefBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            BytesRef value = new BytesRef(randomByteArrayOfLength(between(1, 20)));
            BytesRef originalValue = BytesRef.deepCopyOf(value);
            BytesRefBlock block = blockFactory.newConstantBytesRefBlockWith(value, positionCount);
            // modify the value after creating the constant block
            for (int b = 0; b < value.length; b++) {
                if (randomBoolean()) {
                    value.bytes[b] = randomByte();
                }
            }
            assertThat(block.getPositionCount(), is(positionCount));

            BytesRef bytes = new BytesRef();
            bytes = block.getBytesRef(0, bytes);
            assertThat(bytes, is(originalValue));
            bytes = block.getBytesRef(positionCount - 1, bytes);
            assertThat(bytes, is(originalValue));
            bytes = block.getBytesRef(randomPosition(positionCount), bytes);
            assertThat(bytes, is(originalValue));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    List.of(List.of(originalValue), List.of(originalValue), List.of(originalValue, originalValue))
                );
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2),
                    List.of(List.of(originalValue), List.of(originalValue)),
                    b -> assertThat(b.asVector(), instanceOf(ConstantBytesRefVector.class))
                );
            }
            assertLookup(
                block,
                positions(blockFactory, positionCount + 1000),
                singletonList(null),
                b -> assertThat(b, instanceOf(ConstantNullBlock.class))
            );
            assertEmptyLookup(blockFactory, block);
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
            // modify the offset
            var v0 = block.getBytesRef(randomInt(positionCount), new BytesRef());
            var v1 = block.getBytesRef(randomInt(positionCount), new BytesRef());
            v1.length = 0;
            var v2 = block.getBytesRef(randomInt(positionCount), new BytesRef());
            assertThat(v2, equalTo(v0));
        }
    }

    public void testBooleanBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            BooleanBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                try (var blockBuilder = blockFactory.newBooleanBlockBuilder(builderEstimateSize)) {
                    IntStream.range(0, positionCount).forEach(p -> blockBuilder.appendBoolean(p % 10 == 0));
                    block = blockBuilder.build();
                }
            } else {
                boolean[] values = new boolean[positionCount];
                for (int p = 0; p < positionCount; p++) {
                    values[p] = p % 10 == 0;
                }
                block = blockFactory.newBooleanArrayVector(values, positionCount).asBlock();
            }

            assertThat(block.getPositionCount(), is(positionCount));
            assertThat(block.getBoolean(0), is(true));
            assertThat(block.getBoolean(positionCount - 1), is((positionCount - 1) % 10 == 0));
            assertSingleValueDenseBlock(block);
            if (positionCount > 1) {
                assertLookup(
                    block,
                    positions(blockFactory, 1, 0, new int[] { 1, 0 }),
                    List.of(List.of(false), List.of(true), List.of(false, true))
                );
            }
            assertLookup(block, positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, block);
            try (ToMask mask = block.toMask()) {
                assertThat(mask.hadMultivaluedFields(), equalTo(false));
                for (int p = 0; p < positionCount; p++) {
                    assertThat(mask.mask().getBoolean(p), equalTo(p % 10 == 0));
                }
            }

            try (BooleanBlock.Builder blockBuilder = blockFactory.newBooleanBlockBuilder(1)) {
                BooleanBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
                assertThat(copy, equalTo(block));
                assertInsertNulls(block);
                assertDeepCopy(block);
                releaseAndAssertBreaker(block, copy);
            }

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    size -> blockFactory.newBooleanBlockBuilder(size),
                    (bb, value) -> bb.appendBoolean(value),
                    position -> position % 10 == 0,
                    BooleanBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat(b.getBoolean(randomNonNullPosition.intValue()), is(randomNonNullPosition % 10 == 0));
                    }
                );
            }

            BooleanVector.Builder vectorBuilder = blockFactory.newBooleanVectorBuilder(
                randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
            );
            Boolean value = randomFrom(random(), null, true, false);
            Boolean[] bools = IntStream.range(0, positionCount).mapToObj(ii -> {
                if (value == null) {
                    return randomBoolean();
                }
                return value;
            }).toArray(Boolean[]::new);
            Arrays.stream(bools).forEach(vectorBuilder::appendBoolean);
            BooleanVector vector = vectorBuilder.build();
            assertSingleValueDenseBlock(vector.asBlock());
            assertToMask(vector);
            if (value == null) {
                assertThat(vector.allTrue(), equalTo(Arrays.stream(bools).allMatch(v -> v)));
                assertThat(vector.allFalse(), equalTo(Arrays.stream(bools).allMatch(v -> v == false)));
            } else {
                if (value) {
                    assertTrue(vector.allTrue());
                    assertFalse(vector.allFalse());
                } else {
                    assertFalse(vector.allTrue());
                    assertTrue(vector.allFalse());
                }
            }
            assertInsertNulls(vector.asBlock());
            assertDeepCopy(vector.asBlock());
            releaseAndAssertBreaker(vector.asBlock());
        }
    }

    public void testConstantBooleanBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            boolean value = randomBoolean();
            BooleanBlock block = blockFactory.newConstantBooleanBlockWith(value, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(block.getBoolean(0), is(value));
            assertThat(block.getBoolean(positionCount - 1), is(value));
            assertThat(block.getBoolean(randomPosition(positionCount)), is(value));
            assertSingleValueDenseBlock(block);
            if (positionCount > 2) {
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    List.of(List.of(value), List.of(value), List.of(value, value))
                );
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2),
                    List.of(List.of(value), List.of(value)),
                    b -> assertThat(b.asVector(), instanceOf(ConstantBooleanVector.class))
                );
            }
            assertLookup(
                block,
                positions(blockFactory, positionCount + 1000),
                singletonList(null),
                b -> assertThat(b, instanceOf(ConstantNullBlock.class))
            );
            assertEmptyLookup(blockFactory, block);
            if (value) {
                assertTrue(block.asVector().allTrue());
                assertFalse(block.asVector().allFalse());
            } else {
                assertFalse(block.asVector().allTrue());
                assertTrue(block.asVector().allFalse());
            }
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
        }
    }

    public void testConstantNullBlock() {
        for (int i = 0; i < 100; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = randomIntBetween(1, 16 * 1024);
            Block block = blockFactory.newConstantNullBlock(positionCount);
            assertTrue(block.areAllValuesNull());
            assertThat(block, instanceOf(BooleanBlock.class));
            assertThat(block, instanceOf(IntBlock.class));
            assertThat(block, instanceOf(LongBlock.class));
            assertThat(block, instanceOf(DoubleBlock.class));
            assertThat(block, instanceOf(BytesRefBlock.class));
            assertNull(block.asVector());
            if (randomBoolean()) {
                Block orig = block;
                block = (new ConstantNullBlock.Builder(blockFactory)).copyFrom(block, 0, block.getPositionCount()).build();
                orig.close();
            }
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(block.getPositionCount(), is(positionCount));
            assertThat(block.isNull(randomPosition(positionCount)), is(true));
            if (positionCount > 2) {
                List<List<Object>> expected = new ArrayList<>();
                expected.add(null);
                expected.add(null);
                expected.add(null);
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    expected,
                    b -> assertThat(b, instanceOf(ConstantNullBlock.class))
                );
            }
            assertLookup(
                block,
                positions(blockFactory, positionCount + 1000),
                singletonList(null),
                b -> assertThat(b, instanceOf(ConstantNullBlock.class))
            );
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
        }
    }

    public void testSingleValueSparseInt() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        try (var blockBuilder = blockFactory.newIntBlockBuilder(builderEstimateSize)) {

            int actualValueCount = 0;
            int[] values = new int[positionCount];
            for (int i = 0; i < positionCount; i++) {
                if (randomBoolean()) {
                    values[i] = randomInt();
                    blockBuilder.appendInt(values[i]);
                    actualValueCount++;
                } else {
                    blockBuilder.appendNull();
                }
            }
            IntBlock block = blockBuilder.build();

            assertThat(block.getPositionCount(), is(positionCount));
            assertThat(block.getTotalValueCount(), is(actualValueCount));
            int nullCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (block.isNull(i)) {
                    nullCount++;
                    // assertThat(block.getInt(i), is(0)); // Q: do we wanna allow access to the default value
                } else {
                    assertThat(block.getInt(i), is(values[i]));
                }
            }
            assertThat(block.asVector(), nullCount > 0 ? is(nullValue()) : is(notNullValue()));
            block.close();
        }
    }

    public void testSingleValueSparseLong() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        try (var blockBuilder = blockFactory.newLongBlockBuilder(builderEstimateSize)) {

            int actualValueCount = 0;
            long[] values = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                if (randomBoolean()) {
                    values[i] = randomLong();
                    blockBuilder.appendLong(values[i]);
                    actualValueCount++;
                } else {
                    blockBuilder.appendNull();
                }
            }
            LongBlock block = blockBuilder.build();

            assertThat(block.getPositionCount(), is(positionCount));
            assertThat(block.getTotalValueCount(), is(actualValueCount));
            int nullCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (block.isNull(i)) {
                    nullCount++;
                } else {
                    assertThat(block.getLong(i), is(values[i]));
                }
            }
            assertThat(block.asVector(), nullCount > 0 ? is(nullValue()) : is(notNullValue()));
            block.close();
        }
    }

    public void testSingleValueSparseDouble() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        try (var blockBuilder = blockFactory.newDoubleBlockBuilder(builderEstimateSize)) {

            int actualValueCount = 0;
            double[] values = new double[positionCount];
            for (int i = 0; i < positionCount; i++) {
                if (randomBoolean()) {
                    values[i] = randomDouble();
                    blockBuilder.appendDouble(values[i]);
                    actualValueCount++;
                } else {
                    blockBuilder.appendNull();
                }
            }
            DoubleBlock block = blockBuilder.build();

            assertThat(block.getPositionCount(), is(positionCount));
            assertThat(block.getTotalValueCount(), is(actualValueCount));
            int nullCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (block.isNull(i)) {
                    nullCount++;
                } else {
                    assertThat(block.getDouble(i), is(values[i]));
                }
            }
            assertThat(block.asVector(), nullCount > 0 ? is(nullValue()) : is(notNullValue()));
            block.close();
        }
    }

    public void testSingleValueSparseBoolean() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        try (var blockBuilder = blockFactory.newBooleanBlockBuilder(builderEstimateSize)) {

            boolean[] values = new boolean[positionCount];
            int actualValueCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (randomBoolean()) {
                    values[i] = randomBoolean();
                    blockBuilder.appendBoolean(values[i]);
                    actualValueCount++;
                } else {
                    blockBuilder.appendNull();
                }
            }
            BooleanBlock block = blockBuilder.build();

            assertThat(block.getPositionCount(), is(positionCount));
            assertThat(block.getTotalValueCount(), is(actualValueCount));
            int nullCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (block.isNull(i)) {
                    nullCount++;
                } else {
                    assertThat(block.getBoolean(i), is(values[i]));
                }
            }
            assertThat(block.asVector(), nullCount > 0 ? is(nullValue()) : is(notNullValue()));
            block.close();
        }
    }

    public void testToStringSmall() {
        final int estimatedSize = randomIntBetween(1024, 4096);

        try (
            var boolBlock = blockFactory.newBooleanBlockBuilder(estimatedSize).appendBoolean(true).appendBoolean(false).build();
            var boolVector = blockFactory.newBooleanVectorBuilder(estimatedSize).appendBoolean(true).appendBoolean(false).build()
        ) {
            for (Object obj : List.of(boolVector, boolBlock, boolBlock.asVector())) {
                String s = obj.toString();
                assertThat(s, containsString("[true, false]"));
                assertThat(s, containsString("positions=2"));
            }
        }

        try (
            var intBlock = blockFactory.newIntBlockBuilder(estimatedSize).appendInt(1).appendInt(2).build();
            var intVector = blockFactory.newIntVectorBuilder(estimatedSize).appendInt(1).appendInt(2).build()
        ) {
            for (Object obj : List.of(intVector, intBlock, intBlock.asVector())) {
                String s = obj.toString();
                assertThat(s, containsString("[1, 2]"));
                assertThat(s, containsString("positions=2"));
            }
            for (IntBlock block : List.of(intBlock, intVector.asBlock())) {
                try (var filter = block.filter(false, 0)) {
                    assertThat(filter.toString(), containsString("IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]"));
                }
                try (var filter = block.filter(false, 1)) {
                    assertThat(filter.toString(), containsString("IntVectorBlock[vector=ConstantIntVector[positions=1, value=2]]"));
                }
                try (var filter = block.filter(false, 0, 1)) {
                    assertThat(filter.toString(), containsString("IntVectorBlock[vector=IntArrayVector[positions=2, values=[1, 2]]]"));
                }
                try (var filter = block.filter(false)) {
                    assertThat(filter.toString(), containsString("IntVectorBlock[vector=IntArrayVector[positions=0, values=[]]]"));
                }
            }
            for (IntVector vector : List.of(intVector, intBlock.asVector())) {
                try (var filter = vector.filter(false, 0)) {
                    assertThat(filter.toString(), containsString("ConstantIntVector[positions=1, value=1]"));
                }
                try (IntVector filter = vector.filter(false, 1)) {
                    assertThat(filter.toString(), containsString("ConstantIntVector[positions=1, value=2]"));
                }
                try (IntVector filter = vector.filter(false, 0, 1)) {
                    assertThat(filter.toString(), containsString("IntArrayVector[positions=2, values=[1, 2]]"));
                }
                try (IntVector filter = vector.filter(false)) {
                    assertThat(filter.toString(), containsString("IntArrayVector[positions=0, values=[]]"));
                }
            }
            for (IntBlock block : List.of(intBlock, intVector.asBlock())) {
                try (var sliced = block.slice(0, 1)) {
                    assertThat(sliced.toString(), containsString("IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]"));
                }
                try (var sliced = block.slice(1, 2)) {
                    assertThat(sliced.toString(), containsString("IntVectorBlock[vector=ConstantIntVector[positions=1, value=2]]"));
                }
                try (var sliced = block.slice(0, 2)) {
                    assertThat(sliced.toString(), containsString("IntVectorBlock[vector=IntArrayVector[positions=2, values=[1, 2]]]"));
                }
                try (var sliced = block.slice(0, 0)) {
                    assertThat(sliced.toString(), containsString("IntVectorBlock[vector=IntArrayVector[positions=0, values=[]]]"));
                }
            }
            for (IntVector vector : List.of(intVector, intBlock.asVector())) {
                try (IntVector sliced = vector.slice(0, 1)) {
                    assertThat(sliced.toString(), containsString("ConstantIntVector[positions=1, value=1]"));
                }
                try (IntVector sliced = vector.slice(1, 2)) {
                    assertThat(sliced.toString(), containsString("ConstantIntVector[positions=1, value=2]"));
                }
                try (IntVector sliced = vector.slice(0, 2)) {
                    assertThat(sliced.toString(), containsString("IntArrayVector[positions=2, values=[1, 2]]"));
                }
                try (IntVector sliced = vector.slice(0, 0)) {
                    assertThat(sliced.toString(), containsString("IntArrayVector[positions=0, values=[]]"));
                }
            }
        }

        try (
            var longBlock = blockFactory.newLongBlockBuilder(estimatedSize).appendLong(10L).appendLong(20L).build();
            var longVector = blockFactory.newLongVectorBuilder(estimatedSize).appendLong(10L).appendLong(20L).build()
        ) {
            for (Object obj : List.of(longVector, longBlock, longBlock.asVector())) {
                String s = obj.toString();
                assertThat(s, containsString("[10, 20]"));
                assertThat(s, containsString("positions=2"));
            }
        }

        try (
            var doubleBlock = blockFactory.newDoubleBlockBuilder(estimatedSize).appendDouble(3.3).appendDouble(4.4).build();
            var doubleVector = blockFactory.newDoubleVectorBuilder(estimatedSize).appendDouble(3.3).appendDouble(4.4).build()
        ) {
            for (Object obj : List.of(doubleVector, doubleBlock, doubleBlock.asVector())) {
                String s = obj.toString();
                assertThat(s, containsString("[3.3, 4.4]"));
                assertThat(s, containsString("positions=2"));
            }
        }

        assert new BytesRef("1a").toString().equals("[31 61]") && new BytesRef("2b").toString().equals("[32 62]");
        try (
            var blockBuilder = blockFactory.newBytesRefBlockBuilder(estimatedSize);
            var vectorBuilder = blockFactory.newBytesRefVectorBuilder(estimatedSize)
        ) {
            var bytesRefBlock = blockBuilder.appendBytesRef(new BytesRef("1a")).appendBytesRef(new BytesRef("2b")).build();
            var bytesRefVector = vectorBuilder.appendBytesRef(new BytesRef("1a")).appendBytesRef(new BytesRef("2b")).build();
            for (Object obj : List.of(bytesRefVector, bytesRefVector, bytesRefBlock.asVector())) {
                String s = obj.toString();
                assertThat(s, containsString("positions=2"));
            }
            Releasables.close(bytesRefBlock, bytesRefVector);
        }
    }

    interface BlockBuilderFactory<B extends Block.Builder> {
        B create(int estimatedSize);
    }

    interface BlockProducer<B extends Block, BB extends Block.Builder> {
        B build(BB blockBuilder);
    }

    interface ValueAppender<BB extends Block.Builder, T> {
        void appendValue(BB blockBuilder, T value);
    }

    interface ValueSupplier<T> {
        T getValue(int position);
    }

    <B extends Block, BB extends Block.Builder, T> void assertNullValues(
        int positionCount,
        BlockBuilderFactory<BB> blockBuilderFactory,
        ValueAppender<BB, T> valueAppender,
        ValueSupplier<T> valueSupplier,
        BlockProducer<B, BB> blockProducer,
        BiConsumer<Integer, B> asserter
    ) {
        assertThat("test needs at least two positions", positionCount, greaterThan(1));
        int randomNullPosition = randomIntBetween(0, positionCount - 1);
        int randomNonNullPosition = randomValueOtherThan(randomNullPosition, () -> randomIntBetween(0, positionCount - 1));
        BitSet nullsMask = new BitSet(positionCount);
        nullsMask.set(randomNullPosition);

        var blockBuilder = blockBuilderFactory.create(positionCount);
        IntStream.range(0, positionCount).forEach(position -> {
            if (nullsMask.get(position)) {
                blockBuilder.appendNull();
            } else {
                valueAppender.appendValue(blockBuilder, valueSupplier.getValue(position));
            }
        });
        var block = blockProducer.build(blockBuilder);

        assertThat(block.getPositionCount(), equalTo(positionCount));
        assertThat(block.getTotalValueCount(), equalTo(positionCount - 1));
        asserter.accept(randomNonNullPosition, block);
        assertTrue(block.isNull(randomNullPosition));
        assertFalse(block.isNull(randomNonNullPosition));
        assertInsertNulls(block);
        releaseAndAssertBreaker(block);
        if (block instanceof BooleanBlock bb) {
            try (ToMask mask = bb.toMask()) {
                assertThat(mask.hadMultivaluedFields(), equalTo(false));
                for (int p = 0; p < positionCount; p++) {
                    assertThat(mask.mask().getBoolean(p), equalTo(nullsMask.get(p) == false && p % 10 == 0));
                }
            }
        }
    }

    void assertZeroPositionsAndRelease(BooleanBlock block) {
        assertToMaskZeroPositions(block);
        assertZeroPositionsAndRelease((Block) block);
    }

    void assertZeroPositionsAndRelease(Block block) {
        assertThat(block.getPositionCount(), is(0));
        assertKeepMaskEmpty(block);
        assertInsertNulls(block);
        releaseAndAssertBreaker(block);
    }

    void assertZeroPositionsAndRelease(BooleanVector vector) {
        assertToMask(vector);
        assertZeroPositionsAndRelease((Vector) vector);
    }

    void assertZeroPositionsAndRelease(Vector vector) {
        assertThat(vector.getPositionCount(), is(0));
        assertKeepMaskEmpty(vector);
        releaseAndAssertBreaker(vector);
    }

    static void assertKeepMaskEmpty(Block block) {
        try (BooleanVector mask = randomMask(between(0, 1000)); Block masked = block.keepMask(mask)) {
            if (false == (masked == block || masked.asVector() == block.asVector())) {
                fail("should return original block or vector");
            }
        }
    }

    static void assertKeepMaskEmpty(Vector vector) {
        try (BooleanVector mask = randomMask(between(0, 1000)); Block masked = vector.keepMask(mask)) {
            assertThat(masked.asVector(), sameInstance(vector));
        }
    }

    static void assertToMaskZeroPositions(BooleanBlock block) {
        try (ToMask mask = block.toMask()) {
            assertThat(mask.mask().getPositionCount(), equalTo(0));
            assertThat(mask.hadMultivaluedFields(), equalTo(false));
        }
    }

    static void assertToMask(BooleanVector vector) {
        try (ToMask mask = vector.asBlock().toMask()) {
            assertThat(mask.mask(), sameInstance(vector));
            assertThat(mask.hadMultivaluedFields(), equalTo(false));
        }
    }

    static void assertInsertNulls(Block block) {
        int maxNulls = Math.min(1000, block.getPositionCount() * 5);
        List<Object> orig = new ArrayList<>(block.getPositionCount());
        BlockTestUtils.readInto(orig, block);

        int nullCount = 0;
        try (IntVector.Builder beforeBuilder = block.blockFactory().newIntVectorBuilder(block.getPositionCount())) {
            List<Object> expected = new ArrayList<>(block.getPositionCount());
            for (int p = 0; p < block.getPositionCount(); p++) {
                while (nullCount < maxNulls && randomBoolean()) {
                    expected.add(null);
                    beforeBuilder.appendInt(p);
                    nullCount++;
                }
                expected.add(orig.get(p));
            }
            while (nullCount == 0 || (nullCount < maxNulls && randomBoolean())) {
                expected.add(null);
                beforeBuilder.appendInt(block.getPositionCount());
                nullCount++;
            }

            try (IntVector before = beforeBuilder.build(); Block withNulls = block.insertNulls(before)) {
                List<Object> actual = new ArrayList<>(block.getPositionCount());
                BlockTestUtils.readInto(actual, withNulls);
                assertThat(actual, equalTo(expected));
            }
        }
    }

    void releaseAndAssertBreaker(Block... blocks) {
        assertThat(breaker.getUsed(), greaterThan(0L));
        Page[] pages = Arrays.stream(blocks).map(Page::new).toArray(Page[]::new);

        /*
         * Deep copy the block into the non-breaking instance to make
         * sure that works and that we can read from the deep copy after
         * this has been released.
         */
        Block[] deepCopies = new Block[blocks.length];
        for (int b = 0; b < blocks.length; b++) {
            Block copiedOutOfBreaker = blocks[b].deepCopy(TestBlockFactory.getNonBreakingInstance());
            assertThat(copiedOutOfBreaker, equalTo(blocks[b]));
            deepCopies[b] = copiedOutOfBreaker;
        }

        Releasables.closeExpectNoException(blocks);
        Arrays.stream(blocks).forEach(block -> assertThat(block.isReleased(), is(true)));
        Arrays.stream(blocks).forEach(BasicBlockTests::assertCannotDoubleRelease);
        Arrays.stream(pages).forEach(BasicBlockTests::assertCannotReadFromPage);
        Arrays.stream(blocks).forEach(BasicBlockTests::assertCannotAddToPage);
        assertThat(breaker.getUsed(), is(0L));

        for (int b = 0; b < deepCopies.length; b++) {
            BlockTestUtils.readInto(new ArrayList<>(), deepCopies[b]);
        }
    }

    void releaseAndAssertBreaker(Vector vector) {
        assertThat(breaker.getUsed(), greaterThan(0L));

        /*
         * Deep copy the vector into the non-breaking instance to make
         * sure that works and that we can read from the deep copy after
         * this has been released.
         */
        Vector copiedOutOfBreaker = vector.deepCopy(TestBlockFactory.getNonBreakingInstance());
        assertThat(copiedOutOfBreaker, equalTo(vector));

        Releasables.closeExpectNoException(vector);
        assertThat(breaker.getUsed(), is(0L));

        BlockTestUtils.readInto(new ArrayList<>(), copiedOutOfBreaker.asBlock());
    }

    static void assertCannotDoubleRelease(Block block) {
        var ex = expectThrows(IllegalStateException.class, () -> block.close());
        assertThat(ex.getMessage(), containsString("can't release already released object"));
    }

    static void assertCannotReadFromPage(Page page) {
        var e = expectThrows(IllegalStateException.class, () -> page.getBlock(0));
        assertThat(e.getMessage(), containsString("can't read released block"));
    }

    static void assertCannotAddToPage(Block block) {
        var e = expectThrows(IllegalArgumentException.class, () -> new Page(block));
        assertThat(e.getMessage(), containsString("can't build page out of released blocks but"));
    }

    static int randomPosition(int positionCount) {
        return positionCount == 1 ? 0 : randomIntBetween(0, positionCount - 1);
    }

    static Block.MvOrdering randomOrdering() {
        return randomFrom(Block.MvOrdering.values());
    }

    public void testRefCountingArrayBlock() {
        Block block = randomArrayBlock();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingBigArrayBlock() {
        Block block = randomBigArrayBlock();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingConstantNullBlock() {
        Block block = blockFactory.newConstantNullBlock(10);
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingDocBlock() {
        int positionCount = randomIntBetween(0, 100);
        DocBlock block = new DocVector(
            AlwaysReferencedIndexedByShardId.INSTANCE,
            intVector(positionCount),
            intVector(positionCount),
            intVector(positionCount),
            DocVector.config().singleSegmentNonDecreasing(true)
        ).asBlock();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingVectorBlock() {
        Block block = randomConstantVector().asBlock();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingArrayVector() {
        Vector vector = randomArrayVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingBigArrayVector() {
        Vector vector = randomBigArrayVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingConstantVector() {
        Vector vector = randomConstantVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingDocVector() {
        int positionCount = randomIntBetween(0, 100);
        DocVector vector = new DocVector(
            AlwaysReferencedIndexedByShardId.INSTANCE,
            intVector(positionCount),
            intVector(positionCount),
            intVector(positionCount),
            DocVector.config().singleSegmentNonDecreasing(true)
        );
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testFilterOrdinalBytesRefBlock() {
        int dictSize = between(1, 10);
        int positionCount = between(1, 100);
        try (
            var dictBuilder = blockFactory.newBytesRefVectorBuilder(dictSize);
            var ordinalBuilder = blockFactory.newIntBlockBuilder(positionCount)
        ) {
            for (int i = 0; i < dictSize; i++) {
                dictBuilder.appendBytesRef(new BytesRef("value" + i));
            }
            for (int i = 0; i < positionCount; i++) {
                int valueCount = randomIntBetween(0, 2);
                switch (valueCount) {
                    case 0 -> ordinalBuilder.appendNull();
                    case 1 -> ordinalBuilder.appendInt(randomIntBetween(0, dictSize - 1));
                    default -> {
                        ordinalBuilder.beginPositionEntry();
                        for (int v = 0; v < valueCount; v++) {
                            ordinalBuilder.appendInt(randomIntBetween(0, dictSize - 1));
                        }
                        ordinalBuilder.endPositionEntry();
                    }
                }
            }
            try (var block = new OrdinalBytesRefBlock(ordinalBuilder.build(), dictBuilder.build())) {
                int[] masks = new int[between(1, 100)];
                for (int i = 0; i < masks.length; i++) {
                    masks[i] = randomIntBetween(0, positionCount - 1);
                }
                try (var filtered = block.filter(true, masks)) {
                    assertThat(filtered, not(instanceOf(OrdinalBytesRefBlock.class)));
                }
                assertSliceFullRange(block);
                assertSliceOrdinalBytesRefBlock(block, 0, 0);
                if (positionCount > 1) {
                    assertSliceOrdinalBytesRefBlock(block, 0, 1);
                    assertSliceOrdinalBytesRefBlock(block, 1, positionCount);
                }
            }
        }
    }

    private void assertSliceOrdinalBytesRefBlock(OrdinalBytesRefBlock block, int beginInclusive, int endExclusive) {
        try (var sliced = block.slice(beginInclusive, endExclusive)) {
            assertThat(sliced, instanceOf(OrdinalBytesRefBlock.class));
            assertThat(sliced.getPositionCount(), equalTo(endExclusive - beginInclusive));
        }
    }

    public void testFilterOrdinalBytesRefVector() {
        int dictSize = between(1, 10);
        int positionCount = between(1, 100);
        try (
            var dictBuilder = blockFactory.newBytesRefVectorBuilder(dictSize);
            var ordinalBuilder = blockFactory.newIntVectorBuilder(positionCount)
        ) {
            for (int i = 0; i < dictSize; i++) {
                dictBuilder.appendBytesRef(new BytesRef("value" + i));
            }
            for (int i = 0; i < positionCount; i++) {
                ordinalBuilder.appendInt(randomIntBetween(0, dictSize - 1));
            }
            try (var vector = new OrdinalBytesRefVector(ordinalBuilder.build(), dictBuilder.build())) {
                int[] masks = new int[between(1, 100)];
                for (int i = 0; i < masks.length; i++) {
                    masks[i] = randomIntBetween(0, positionCount - 1);
                }
                try (var filtered = vector.filter(true, masks)) {
                    assertThat(filtered, not(instanceOf(OrdinalBytesRefVector.class)));
                }
                assertSliceFullRange(vector);
                assertSliceOrdinalBytesRefVector(vector, 0, 0);
                if (positionCount > 1) {
                    assertSliceOrdinalBytesRefVector(vector, 0, 1);
                    assertSliceOrdinalBytesRefVector(vector, 1, positionCount);
                }
            }
        }
    }

    private void assertSliceOrdinalBytesRefVector(OrdinalBytesRefVector vector, int beginInclusive, int endExclusive) {
        try (var sliced = vector.slice(beginInclusive, endExclusive)) {
            assertThat(sliced, instanceOf(OrdinalBytesRefVector.class));
            assertThat(sliced.getPositionCount(), equalTo(endExclusive - beginInclusive));
        }
    }

    /**
     * Take an object with exactly 1 reference and assert that ref counting works fine.
     * Assumes that {@link Releasable#close()} and {@link RefCounted#decRef()} are equivalent.
     */
    static <T extends RefCounted & Releasable> void assertRefCountingBehavior(T object) {
        assertTrue(object.hasReferences());
        int numShallowCopies = randomIntBetween(0, 15);
        for (int i = 0; i < numShallowCopies; i++) {
            if (randomBoolean()) {
                object.incRef();
            } else {
                assertTrue(object.tryIncRef());
            }
        }

        for (int i = 0; i < numShallowCopies; i++) {
            if (randomBoolean()) {
                object.close();
            } else {
                // closing and decRef'ing must be equivalent
                assertFalse(object.decRef());
            }
            assertTrue(object.hasReferences());
        }

        if (randomBoolean()) {
            object.close();
        } else {
            assertTrue(object.decRef());
        }

        assertFalse(object.hasReferences());
        assertFalse(object.tryIncRef());

        expectThrows(IllegalStateException.class, object::close);
        expectThrows(IllegalStateException.class, object::incRef);
    }

    private IntVector intVector(int positionCount) {
        return blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
    }

    private Vector randomArrayVector() {
        int positionCount = randomIntBetween(0, 100);
        int vectorType = randomIntBetween(0, 4);

        return switch (vectorType) {
            case 0 -> {
                boolean[] values = new boolean[positionCount];
                Arrays.fill(values, randomBoolean());
                yield blockFactory.newBooleanArrayVector(values, positionCount);
            }
            case 1 -> {
                BytesRefArray values = new BytesRefArray(positionCount, BigArrays.NON_RECYCLING_INSTANCE);
                for (int i = 0; i < positionCount; i++) {
                    values.append(new BytesRef(randomByteArrayOfLength(between(1, 20))));
                }

                yield blockFactory.newBytesRefArrayVector(values, positionCount);
            }
            case 2 -> {
                double[] values = new double[positionCount];
                Arrays.fill(values, 1.0);

                yield blockFactory.newDoubleArrayVector(values, positionCount);
            }
            case 3 -> {
                int[] values = new int[positionCount];
                Arrays.fill(values, 1);

                yield blockFactory.newIntArrayVector(values, positionCount);
            }
            default -> {
                long[] values = new long[positionCount];
                Arrays.fill(values, 1L);

                yield blockFactory.newLongArrayVector(values, positionCount);
            }
        };
    }

    private Vector randomBigArrayVector() {
        int positionCount = randomIntBetween(0, 10000);
        int arrayType = randomIntBetween(0, 3);

        return switch (arrayType) {
            case 0 -> {
                BitArray values = new BitArray(positionCount, blockFactory.bigArrays());
                for (int i = 0; i < positionCount; i++) {
                    if (randomBoolean()) {
                        values.set(positionCount);
                    }
                }

                yield new BooleanBigArrayVector(values, positionCount, blockFactory);
            }
            case 1 -> {
                DoubleArray values = blockFactory.bigArrays().newDoubleArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomDouble());
                }

                yield new DoubleBigArrayVector(values, positionCount, blockFactory);
            }
            case 2 -> {
                IntArray values = blockFactory.bigArrays().newIntArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomInt());
                }

                yield new IntBigArrayVector(values, positionCount, blockFactory);
            }
            default -> {
                LongArray values = blockFactory.bigArrays().newLongArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomLong());
                }

                yield new LongBigArrayVector(values, positionCount, blockFactory);
            }
        };
    }

    private Vector randomConstantVector() {
        int positionCount = randomIntBetween(0, 100);
        int vectorType = randomIntBetween(0, 4);

        return switch (vectorType) {
            case 0 -> blockFactory.newConstantBooleanVector(true, positionCount);
            case 1 -> blockFactory.newConstantBytesRefVector(new BytesRef(), positionCount);
            case 2 -> blockFactory.newConstantDoubleVector(1.0, positionCount);
            case 3 -> blockFactory.newConstantIntVector(1, positionCount);
            default -> blockFactory.newConstantLongVector(1L, positionCount);
        };
    }

    private Block randomArrayBlock() {
        int positionCount = randomIntBetween(0, 100);
        int arrayType = randomIntBetween(0, 4);
        int[] firstValueIndexes = IntStream.range(0, positionCount + 1).toArray();

        return switch (arrayType) {
            case 0 -> {
                boolean[] values = new boolean[positionCount];
                Arrays.fill(values, randomBoolean());

                yield blockFactory.newBooleanArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
            case 1 -> {
                BytesRefArray values = new BytesRefArray(positionCount, BigArrays.NON_RECYCLING_INSTANCE);
                for (int i = 0; i < positionCount; i++) {
                    values.append(new BytesRef(randomByteArrayOfLength(between(1, 20))));
                }

                yield blockFactory.newBytesRefArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
            case 2 -> {
                double[] values = new double[positionCount];
                Arrays.fill(values, 1.0);

                yield blockFactory.newDoubleArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
            case 3 -> {
                int[] values = new int[positionCount];
                Arrays.fill(values, 1);

                yield blockFactory.newIntArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
            default -> {
                long[] values = new long[positionCount];
                Arrays.fill(values, 1L);

                yield blockFactory.newLongArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
        };
    }

    private Block randomBigArrayBlock() {
        int positionCount = randomIntBetween(0, 10000);
        int arrayType = randomIntBetween(0, 3);

        return switch (arrayType) {
            case 0 -> {
                BitArray values = new BitArray(positionCount, blockFactory.bigArrays());
                for (int i = 0; i < positionCount; i++) {
                    if (randomBoolean()) {
                        values.set(positionCount);
                    }
                }

                yield new BooleanBigArrayBlock(values, positionCount, null, new BitSet(), Block.MvOrdering.UNORDERED, blockFactory);
            }
            case 1 -> {
                DoubleArray values = blockFactory.bigArrays().newDoubleArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomDouble());
                }

                yield new DoubleBigArrayBlock(values, positionCount, null, new BitSet(), Block.MvOrdering.UNORDERED, blockFactory);
            }
            case 2 -> {
                IntArray values = blockFactory.bigArrays().newIntArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomInt());
                }

                yield new IntBigArrayBlock(values, positionCount, null, new BitSet(), Block.MvOrdering.UNORDERED, blockFactory);
            }
            default -> {
                LongArray values = blockFactory.bigArrays().newLongArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomLong());
                }

                yield new LongBigArrayBlock(values, positionCount, null, new BitSet(), Block.MvOrdering.UNORDERED, blockFactory);
            }
        };
    }

    static IntBlock positions(BlockFactory blockFactory, Object... positions) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions.length)) {
            for (Object p : positions) {
                if (p instanceof int[] mv) {
                    builder.beginPositionEntry();
                    for (int v : mv) {
                        builder.appendInt(v);
                    }
                    builder.endPositionEntry();
                    continue;
                }
                if (p instanceof Integer v) {
                    builder.appendInt(v);
                    continue;
                }
                throw new IllegalArgumentException("invalid position: " + p + "(" + p.getClass().getName() + ")");
            }
            return builder.build();
        }
    }

    static void assertEmptyLookup(BlockFactory blockFactory, Block block) {
        try (
            IntBlock positions = positions(blockFactory);
            ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))
        ) {
            assertThat(lookup.hasNext(), equalTo(false));
        }
    }

    static void assertLookup(Block block, IntBlock positions, List<List<Object>> expected) {
        assertLookup(block, positions, expected, l -> {});
    }

    static void assertLookup(Block block, IntBlock positions, List<List<Object>> expected, Consumer<Block> extra) {
        try (positions; ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))) {
            assertThat(lookup.hasNext(), equalTo(true));
            try (Block b = lookup.next()) {
                assertThat(valuesAtPositions(b, 0, b.getPositionCount()), equalTo(expected));
                assertThat(b.blockFactory(), sameInstance(positions.blockFactory()));
                extra.accept(b);
            }
            assertThat(lookup.hasNext(), equalTo(false));
        }
    }

    public static Block assertDeepCopy(Block block) {
        CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService);
        BlockFactory into = BlockFactory.builder(bigArrays).build();
        try (Block deepCopy = block.deepCopy(into)) {
            assertThat(deepCopy, equalTo(block));

            if (block.asVector() != null && block.asVector().isConstant()) {
                /*
                 * If we were a constant, we will still be one. If we were not a constant,
                 * deepCopy might make a constant in the rare case that we have a single element array.
                 */
                assertThat(deepCopy.asVector() != null && deepCopy.asVector().isConstant(), equalTo(true));
            }
        }
        Block untracked = block.deepCopy(TestBlockFactory.getNonBreakingInstance());
        assertThat(untracked, equalTo(block));
        // untracked doesn't need to be released
        return untracked;
    }

    public static void assertKeepMask(Vector vector) {
        int maskPositions = vector.getPositionCount();
        if (randomBoolean()) {
            maskPositions += between(1, 1000);
        }
        try (
            BooleanVector mask = TestBlockFactory.getNonBreakingInstance().newConstantBooleanVector(true, maskPositions);
            Block masked = vector.keepMask(mask)
        ) {
            assertThat(masked.asVector(), sameInstance(vector));
        }
        try (
            BooleanVector mask = TestBlockFactory.getNonBreakingInstance().newConstantBooleanVector(false, maskPositions);
            Block masked = vector.keepMask(mask)
        ) {
            assertThat(masked.getPositionCount(), equalTo(vector.getPositionCount()));
            for (int p = 0; p < vector.getPositionCount(); p++) {
                assertTrue(masked.isNull(p));
            }
        }
        try (BooleanVector mask = randomMask(maskPositions); Block masked = vector.keepMask(mask)) {
            assertThat(masked.getPositionCount(), equalTo(vector.getPositionCount()));
            for (int p = 0; p < vector.getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    assertFalse(masked.isNull(p));
                    assertEquals(1, masked.getValueCount(p));
                    assertEquals(BlockUtils.toJavaObject(vector.asBlock(), p), BlockUtils.toJavaObject(masked, p));
                } else {
                    assertTrue(masked.isNull(p));
                }
            }
        }
    }

    public static void assertKeepMask(Block block) {
        int maskPositions = block.getPositionCount();
        if (randomBoolean()) {
            maskPositions += between(1, 1000);
        }
        try (
            BooleanVector mask = TestBlockFactory.getNonBreakingInstance().newConstantBooleanVector(true, maskPositions);
            Block masked = block.keepMask(mask)
        ) {
            if (false == (masked == block || masked.asVector() == block.asVector())) {
                fail("should return original block or vector");
            }
        }
        try (
            BooleanVector mask = TestBlockFactory.getNonBreakingInstance().newConstantBooleanVector(false, maskPositions);
            Block masked = block.keepMask(mask)
        ) {
            assertThat(masked.getPositionCount(), equalTo(block.getPositionCount()));
            for (int p = 0; p < block.getPositionCount(); p++) {
                assertTrue(masked.isNull(p));
            }
        }
        try (BooleanVector mask = randomMask(maskPositions); Block masked = block.keepMask(mask)) {
            assertThat(masked.getPositionCount(), equalTo(block.getPositionCount()));
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (mask.getBoolean(p) && false == block.isNull(p)) {
                    assertFalse(masked.isNull(p));
                    assertEquals(block.getValueCount(p), masked.getValueCount(p));
                    assertEquals(BlockUtils.toJavaObject(block, p), BlockUtils.toJavaObject(masked, p));
                } else {
                    assertTrue(masked.isNull(p));
                }
            }
        }
    }

    /**
     * Asserts the behavior of {@link Block#filter} with random filters.
     */
    public static void assertFilter(Block block) {
        int positionCount = block.getPositionCount();
        try (Block filtered = block.filter(false)) {
            assertThat(filtered.getPositionCount(), equalTo(0));
        }
        if (positionCount == 0) {
            return;
        }
        int[] allPositions = IntStream.range(0, positionCount).toArray();
        try (Block filtered = block.filter(false, allPositions)) {
            assertThat(filtered.getPositionCount(), equalTo(positionCount));
            for (int p = 0; p < positionCount; p++) {
                assertEquals(BlockUtils.toJavaObject(block, p), BlockUtils.toJavaObject(filtered, p));
            }
        }
        int[] subsetPositions = randomSubsetOf(between(1, positionCount), IntStream.range(0, positionCount).boxed().toList()).stream()
            .mapToInt(Integer::intValue)
            .toArray();
        try (Block filtered = block.filter(false, subsetPositions)) {
            assertThat(filtered.getPositionCount(), equalTo(subsetPositions.length));
            for (int p = 0; p < subsetPositions.length; p++) {
                assertEquals(BlockUtils.toJavaObject(block, subsetPositions[p]), BlockUtils.toJavaObject(filtered, p));
            }
        }
        int[] subsetWithRepeats = randomList(1, 1000, () -> between(0, positionCount - 1)).stream().mapToInt(i -> i).toArray();
        try (Block filtered = block.filter(true, subsetWithRepeats)) {
            assertThat(filtered.getPositionCount(), equalTo(subsetWithRepeats.length));
            for (int p = 0; p < subsetWithRepeats.length; p++) {
                assertEquals(BlockUtils.toJavaObject(block, subsetWithRepeats[p]), BlockUtils.toJavaObject(filtered, p));
            }
        }
    }

    /**
     * Asserts the behavior of {@link Vector#filter} with random filter.
     */
    public static void assertFilter(Vector vector) {
        int positionCount = vector.getPositionCount();
        try (Vector filtered = vector.filter(false)) {
            assertThat(filtered.getPositionCount(), equalTo(0));
        }
        if (positionCount == 0) {
            return;
        }
        int[] allPositions = IntStream.range(0, positionCount).toArray();
        try (Vector filtered = vector.filter(false, allPositions)) {
            assertThat(filtered.getPositionCount(), equalTo(positionCount));
            for (int p = 0; p < positionCount; p++) {
                assertEquals(BlockUtils.toJavaObject(vector.asBlock(), p), BlockUtils.toJavaObject(filtered.asBlock(), p));
            }
        }
        int[] subsetPositions = randomSubsetOf(between(1, positionCount), IntStream.range(0, positionCount).boxed().toList()).stream()
            .mapToInt(Integer::intValue)
            .toArray();
        Arrays.sort(subsetPositions);
        try (Vector filtered = vector.filter(false, subsetPositions)) {
            assertThat(filtered.getPositionCount(), equalTo(subsetPositions.length));
            for (int p = 0; p < subsetPositions.length; p++) {
                assertEquals(BlockUtils.toJavaObject(vector.asBlock(), subsetPositions[p]), BlockUtils.toJavaObject(filtered.asBlock(), p));
            }
        }
        int[] subsetWithRepeats = randomList(1, 1000, () -> between(0, positionCount - 1)).stream().mapToInt(i -> i).toArray();
        try (Vector filtered = vector.filter(true, subsetWithRepeats)) {
            assertThat(filtered.getPositionCount(), equalTo(subsetWithRepeats.length));
            for (int p = 0; p < subsetWithRepeats.length; p++) {
                assertEquals(
                    BlockUtils.toJavaObject(vector.asBlock(), subsetWithRepeats[p]),
                    BlockUtils.toJavaObject(filtered.asBlock(), p)
                );
            }
        }
    }

    /**
     * Asserts the behavior of {@link Block#slice} with random slices.
     */
    public static void assertSlice(Block block) {
        int positionCount = block.getPositionCount();
        assertSliceFullRange(block);
        if (positionCount == 0) {
            return;
        }
        try (Block sliced = block.slice(0, positionCount)) {
            assertThat(sliced.getPositionCount(), equalTo(positionCount));
            for (int p = 0; p < positionCount; p++) {
                assertEquals(BlockUtils.toJavaObject(block, p), BlockUtils.toJavaObject(sliced, p));
            }
        }
        int begin = between(0, positionCount);
        int end = between(begin, positionCount);
        try (Block sliced = block.slice(begin, end)) {
            assertThat(sliced.getPositionCount(), equalTo(end - begin));
            for (int p = 0; p < end - begin; p++) {
                assertEquals(BlockUtils.toJavaObject(block, begin + p), BlockUtils.toJavaObject(sliced, p));
            }
        }
    }

    /**
     * Asserts the behavior of {@link Vector#slice} with random slices.
     */
    public static void assertSlice(Vector vector) {
        int positionCount = vector.getPositionCount();
        assertSliceFullRange(vector);
        if (positionCount == 0) {
            return;
        }
        try (Vector sliced = vector.slice(0, positionCount)) {
            assertThat(sliced.getPositionCount(), equalTo(positionCount));
            for (int p = 0; p < positionCount; p++) {
                assertEquals(BlockUtils.toJavaObject(vector.asBlock(), p), BlockUtils.toJavaObject(sliced.asBlock(), p));
            }
        }
        int begin = between(0, positionCount);
        int end = between(begin, positionCount);
        try (Vector sliced = vector.slice(begin, end)) {
            assertThat(sliced.getPositionCount(), equalTo(end - begin));
            for (int p = 0; p < end - begin; p++) {
                assertEquals(BlockUtils.toJavaObject(vector.asBlock(), begin + p), BlockUtils.toJavaObject(sliced.asBlock(), p));
            }
        }
    }

    private static void assertSliceFullRange(Block block) {
        try (var sliced = block.slice(0, block.getPositionCount())) {
            assertThat(sliced, sameInstance(sliced));
        }
    }

    private static void assertSliceFullRange(Vector vector) {
        try (var sliced = vector.slice(0, vector.getPositionCount())) {
            assertThat(sliced, sameInstance(vector));
        }
    }

    public void testCopyToLong() {
        int positionCount = randomIntBetween(1, 1000);
        try (LongVector.Builder builder = blockFactory.newLongVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendLong(randomLong());
            }
            try (LongVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                long sentinel = randomLong();
                long[] dst = new long[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getLong(srcPosition + i)));
                }
                for (int i = 0; i < dstPosition; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
                for (int i = dstPosition + length; i < dst.length; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
            }
        }
    }

    public void testCopyToInt() {
        int positionCount = randomIntBetween(1, 1000);
        try (IntVector.Builder builder = blockFactory.newIntVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendInt(randomInt());
            }
            try (IntVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                int sentinel = randomInt();
                int[] dst = new int[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getInt(srcPosition + i)));
                }
                for (int i = 0; i < dstPosition; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
                for (int i = dstPosition + length; i < dst.length; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
            }
        }
    }

    public void testCopyToDouble() {
        int positionCount = randomIntBetween(1, 1000);
        try (DoubleVector.Builder builder = blockFactory.newDoubleVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendDouble(randomDouble());
            }
            try (DoubleVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                double sentinel = randomDouble();
                double[] dst = new double[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getDouble(srcPosition + i)));
                }
                for (int i = 0; i < dstPosition; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
                for (int i = dstPosition + length; i < dst.length; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
            }
        }
    }

    public void testCopyToFloat() {
        int positionCount = randomIntBetween(1, 1000);
        try (FloatVector.Builder builder = blockFactory.newFloatVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendFloat(randomFloat());
            }
            try (FloatVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                float sentinel = randomFloat();
                float[] dst = new float[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getFloat(srcPosition + i)));
                }
                for (int i = 0; i < dstPosition; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
                for (int i = dstPosition + length; i < dst.length; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
            }
        }
    }

    public void testCopyToBoolean() {
        int positionCount = randomIntBetween(1, 1000);
        try (BooleanVector.Builder builder = blockFactory.newBooleanVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendBoolean(randomBoolean());
            }
            try (BooleanVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                boolean sentinel = randomBoolean();
                boolean[] dst = new boolean[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getBoolean(srcPosition + i)));
                }
                for (int i = 0; i < dstPosition; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
                for (int i = dstPosition + length; i < dst.length; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
            }
        }
    }

    /**
     * Build a random valid "mask" of single valued boolean fields that.
     */
    static BooleanVector randomMask(int positions) {
        try (BooleanVector.Builder builder = TestBlockFactory.getNonBreakingInstance().newBooleanVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendBoolean(randomBoolean());
            }
            return builder.build();
        }
    }
}
