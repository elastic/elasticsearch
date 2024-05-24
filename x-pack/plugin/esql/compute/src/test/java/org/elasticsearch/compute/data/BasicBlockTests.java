/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

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
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasicBlockTests extends ESTestCase {
    final CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
    final BlockFactory blockFactory = BlockFactory.getInstance(breaker, bigArrays);

    @Before
    @After
    public void checkBreaker() {
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testEmpty() {
        testEmpty(blockFactory);
    }

    void testEmpty(BlockFactory bf) {
        assertZeroPositionsAndRelease(bf.newIntArrayBlock(new int[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering()));
        assertZeroPositionsAndRelease(bf.newIntBlockBuilder(0).build());
        assertZeroPositionsAndRelease(bf.newIntArrayVector(new int[] {}, 0));
        assertZeroPositionsAndRelease(bf.newIntVectorBuilder(0).build());
        assertZeroPositionsAndRelease(bf.newLongArrayBlock(new long[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering()));
        assertZeroPositionsAndRelease(bf.newLongBlockBuilder(0).build());
        assertZeroPositionsAndRelease(bf.newLongArrayVector(new long[] {}, 0));
        assertZeroPositionsAndRelease(bf.newLongVectorBuilder(0).build());
        assertZeroPositionsAndRelease(bf.newDoubleArrayBlock(new double[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering()));
        assertZeroPositionsAndRelease(bf.newDoubleBlockBuilder(0).build());
        assertZeroPositionsAndRelease(bf.newDoubleArrayVector(new double[] {}, 0));
        assertZeroPositionsAndRelease(bf.newDoubleVectorBuilder(0).build());
        assertZeroPositionsAndRelease(
            bf.newBytesRefArrayBlock(new BytesRefArray(0, bf.bigArrays()), 0, new int[] { 0 }, new BitSet(), randomOrdering())
        );
        assertZeroPositionsAndRelease(bf.newBytesRefBlockBuilder(0).build());
        assertZeroPositionsAndRelease(bf.newBytesRefArrayVector(new BytesRefArray(0, bf.bigArrays()), 0));
        assertZeroPositionsAndRelease(bf.newBytesRefVectorBuilder(0).build());
        assertZeroPositionsAndRelease(bf.newBooleanArrayBlock(new boolean[] {}, 0, new int[] { 0 }, new BitSet(), randomOrdering()));
        assertZeroPositionsAndRelease(bf.newBooleanBlockBuilder(0).build());
        assertZeroPositionsAndRelease(bf.newBooleanArrayVector(new boolean[] {}, 0));
        assertZeroPositionsAndRelease(bf.newBooleanVectorBuilder(0).build());
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

            initialBlock = block.asVector().asBlock();
        }
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

            try (IntBlock.Builder blockBuilder = blockFactory.newIntBlockBuilder(1)) {
                IntBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
                assertThat(copy, equalTo(block));
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
                releaseAndAssertBreaker(vector.asBlock());
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
            releaseAndAssertBreaker(block);

            try (IntVector.Builder vectorBuilder = blockFactory.newIntVectorBuilder(0)) {
                IntVector vector = vectorBuilder.build();
                assertThat(vector.min(), equalTo(Integer.MAX_VALUE));
                assertThat(vector.max(), equalTo(Integer.MIN_VALUE));
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
            releaseAndAssertBreaker(vector.asBlock());
        }
    }

    public void testBytesRefBlock() {
        testBytesRefBlock(() -> new BytesRef(randomByteArrayOfLength(between(1, 20))), true, b -> {});
    }

    public void testBytesRefBlockOnGeoPoints() {
        testBytesRefBlock(() -> GEO.asWkb(GeometryTestUtils.randomPoint()), false, GEO::wkbToWkt);
    }

    public void testBytesRefBlockOnCartesianPoints() {
        testBytesRefBlock(() -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint()), false, CARTESIAN::wkbToWkt);
    }

    public void testBytesRefBlockBuilderWithNulls() {
        int positionCount = randomIntBetween(0, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
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
            releaseAndAssertBreaker(block);
        }
    }

    public void testConstantBytesRefBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            BytesRef value = new BytesRef(randomByteArrayOfLength(between(1, 20)));
            BytesRefBlock block = blockFactory.newConstantBytesRefBlockWith(value, positionCount);
            assertThat(block.getPositionCount(), is(positionCount));

            BytesRef bytes = new BytesRef();
            bytes = block.getBytesRef(0, bytes);
            assertThat(bytes, is(value));
            bytes = block.getBytesRef(positionCount - 1, bytes);
            assertThat(bytes, is(value));
            bytes = block.getBytesRef(randomPosition(positionCount), bytes);
            assertThat(bytes, is(value));
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
            releaseAndAssertBreaker(block);
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

            try (BooleanBlock.Builder blockBuilder = blockFactory.newBooleanBlockBuilder(1)) {
                BooleanBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
                assertThat(copy, equalTo(block));
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
            IntStream.range(0, positionCount).mapToObj(ii -> randomBoolean()).forEach(vectorBuilder::appendBoolean);
            BooleanVector vector = vectorBuilder.build();
            assertSingleValueDenseBlock(vector.asBlock());
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
                try (var filter = block.filter(0)) {
                    assertThat(filter.toString(), containsString("IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]"));
                }
                try (var filter = block.filter(1)) {
                    assertThat(filter.toString(), containsString("IntVectorBlock[vector=ConstantIntVector[positions=1, value=2]]"));
                }
                try (var filter = block.filter(0, 1)) {
                    assertThat(filter.toString(), containsString("IntVectorBlock[vector=IntArrayVector[positions=2, values=[1, 2]]]"));
                }
                try (var filter = block.filter()) {
                    assertThat(filter.toString(), containsString("IntVectorBlock[vector=IntArrayVector[positions=0, values=[]]]"));
                }
            }
            for (IntVector vector : List.of(intVector, intBlock.asVector())) {
                try (var filter = vector.filter(0)) {
                    assertThat(filter.toString(), containsString("ConstantIntVector[positions=1, value=1]"));
                }
                try (IntVector filter = vector.filter(1)) {
                    assertThat(filter.toString(), containsString("ConstantIntVector[positions=1, value=2]"));
                }
                try (IntVector filter = vector.filter(0, 1)) {
                    assertThat(filter.toString(), containsString("IntArrayVector[positions=2, values=[1, 2]]"));
                }
                try (IntVector filter = vector.filter()) {
                    assertThat(filter.toString(), containsString("IntArrayVector[positions=0, values=[]]"));
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

    public static List<List<Object>> valuesAtPositions(Block block, int from, int to) {
        List<List<Object>> result = new ArrayList<>(to - from);
        for (int p = from; p < to; p++) {
            if (block.isNull(p)) {
                result.add(null);
                continue;
            }
            int count = block.getValueCount(p);
            List<Object> positionValues = new ArrayList<>(count);
            int i = block.getFirstValueIndex(p);
            for (int v = 0; v < count; v++) {
                positionValues.add(switch (block.elementType()) {
                    case INT -> ((IntBlock) block).getInt(i++);
                    case LONG -> ((LongBlock) block).getLong(i++);
                    case DOUBLE -> ((DoubleBlock) block).getDouble(i++);
                    case BYTES_REF -> ((BytesRefBlock) block).getBytesRef(i++, new BytesRef());
                    case BOOLEAN -> ((BooleanBlock) block).getBoolean(i++);
                    default -> throw new IllegalArgumentException("unsupported element type [" + block.elementType() + "]");
                });
            }
            result.add(positionValues);
        }
        return result;
    }

    public record RandomBlock(List<List<Object>> values, Block block) {
        int valueCount() {
            return values.stream().mapToInt(l -> l == null ? 0 : l.size()).sum();
        }

        /**
         * Build a {@link RandomBlock} contain the values of two blocks, preserving the relative order.
         */
        public BasicBlockTests.RandomBlock merge(BasicBlockTests.RandomBlock rhs) {
            int estimatedSize = values().size() + rhs.values().size();
            int l = 0;
            int r = 0;
            List<List<Object>> mergedValues = new ArrayList<>(estimatedSize);
            try (Block.Builder mergedBlock = block.elementType().newBlockBuilder(estimatedSize, block.blockFactory())) {
                while (l < values.size() && r < rhs.values.size()) {
                    if (randomBoolean()) {
                        mergedValues.add(values.get(l));
                        mergedBlock.copyFrom(block, l, l + 1);
                        l++;
                    } else {
                        mergedValues.add(rhs.values.get(r));
                        mergedBlock.copyFrom(rhs.block, r, r + 1);
                        r++;
                    }
                }
                while (l < values.size()) {
                    mergedValues.add(values.get(l));
                    mergedBlock.copyFrom(block, l, l + 1);
                    l++;
                }
                while (r < rhs.values.size()) {
                    mergedValues.add(rhs.values.get(r));
                    mergedBlock.copyFrom(rhs.block, r, r + 1);
                    r++;
                }
                return new BasicBlockTests.RandomBlock(mergedValues, mergedBlock.build());
            }
        }

    }

    public static RandomBlock randomBlock(
        ElementType elementType,
        int positionCount,
        boolean nullAllowed,
        int minValuesPerPosition,
        int maxValuesPerPosition,
        int minDupsPerPosition,
        int maxDupsPerPosition
    ) {
        return randomBlock(
            TestBlockFactory.getNonBreakingInstance(),
            elementType,
            positionCount,
            nullAllowed,
            minValuesPerPosition,
            maxValuesPerPosition,
            minDupsPerPosition,
            maxDupsPerPosition
        );
    }

    public static RandomBlock randomBlock(
        BlockFactory blockFactory,
        ElementType elementType,
        int positionCount,
        boolean nullAllowed,
        int minValuesPerPosition,
        int maxValuesPerPosition,
        int minDupsPerPosition,
        int maxDupsPerPosition
    ) {
        List<List<Object>> values = new ArrayList<>();
        Block.MvOrdering mvOrdering = Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING;
        try (var builder = elementType.newBlockBuilder(positionCount, blockFactory)) {
            boolean bytesRefFromPoints = randomBoolean();
            Supplier<Point> pointSupplier = randomBoolean() ? GeometryTestUtils::randomPoint : ShapeTestUtils::randomPoint;
            for (int p = 0; p < positionCount; p++) {
                if (elementType == ElementType.NULL) {
                    assert nullAllowed;
                    values.add(null);
                    builder.appendNull();
                    continue;
                }
                int valueCount = between(minValuesPerPosition, maxValuesPerPosition);
                if (valueCount == 0 || nullAllowed && randomBoolean()) {
                    values.add(null);
                    builder.appendNull();
                    continue;
                }
                int dupCount = between(minDupsPerPosition, maxDupsPerPosition);
                if (valueCount != 1 || dupCount != 0) {
                    builder.beginPositionEntry();
                }
                List<Object> valuesAtPosition = new ArrayList<>();
                values.add(valuesAtPosition);
                for (int v = 0; v < valueCount; v++) {
                    switch (elementType) {
                        case INT -> {
                            int i = randomInt();
                            valuesAtPosition.add(i);
                            ((IntBlock.Builder) builder).appendInt(i);
                        }
                        case LONG -> {
                            long l = randomLong();
                            valuesAtPosition.add(l);
                            ((LongBlock.Builder) builder).appendLong(l);
                        }
                        case DOUBLE -> {
                            double d = randomDouble();
                            valuesAtPosition.add(d);
                            ((DoubleBlock.Builder) builder).appendDouble(d);
                        }
                        case BYTES_REF -> {
                            BytesRef b = bytesRefFromPoints
                                ? GEO.asWkb(pointSupplier.get())
                                : new BytesRef(randomRealisticUnicodeOfLength(4));
                            valuesAtPosition.add(b);
                            ((BytesRefBlock.Builder) builder).appendBytesRef(b);
                        }
                        case BOOLEAN -> {
                            boolean b = randomBoolean();
                            valuesAtPosition.add(b);
                            ((BooleanBlock.Builder) builder).appendBoolean(b);
                        }
                        default -> throw new IllegalArgumentException("unsupported element type [" + elementType + "]");
                    }
                }
                for (int i = 0; i < dupCount; i++) {
                    BlockTestUtils.append(builder, randomFrom(valuesAtPosition));
                }
                if (valueCount != 1 || dupCount != 0) {
                    builder.endPositionEntry();
                }
                if (dupCount > 0) {
                    mvOrdering = Block.MvOrdering.UNORDERED;
                } else if (mvOrdering != Block.MvOrdering.UNORDERED) {
                    List<Object> dedupedAndSortedList = valuesAtPosition.stream().sorted().distinct().toList();
                    if (dedupedAndSortedList.size() != valuesAtPosition.size()) {
                        mvOrdering = Block.MvOrdering.UNORDERED;
                    } else if (dedupedAndSortedList.equals(valuesAtPosition) == false) {
                        mvOrdering = Block.MvOrdering.DEDUPLICATED_UNORDERD;
                    }
                }
            }
            if (randomBoolean()) {
                builder.mvOrdering(mvOrdering);
            }
            return new RandomBlock(values, builder.build());
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
        releaseAndAssertBreaker(block);
    }

    void assertZeroPositionsAndRelease(Block block) {
        assertThat(block.getPositionCount(), is(0));
        releaseAndAssertBreaker(block);
    }

    void assertZeroPositionsAndRelease(Vector vector) {
        assertThat(vector.getPositionCount(), is(0));
        releaseAndAssertBreaker(vector);
    }

    void releaseAndAssertBreaker(Block... blocks) {
        assertThat(breaker.getUsed(), greaterThan(0L));
        Page[] pages = Arrays.stream(blocks).map(Page::new).toArray(Page[]::new);
        Releasables.closeExpectNoException(blocks);
        Arrays.stream(blocks).forEach(block -> assertThat(block.isReleased(), is(true)));
        Arrays.stream(blocks).forEach(BasicBlockTests::assertCannotDoubleRelease);
        Arrays.stream(pages).forEach(BasicBlockTests::assertCannotReadFromPage);
        Arrays.stream(blocks).forEach(BasicBlockTests::assertCannotAddToPage);
        assertThat(breaker.getUsed(), is(0L));
    }

    void releaseAndAssertBreaker(Vector vector) {
        assertThat(breaker.getUsed(), greaterThan(0L));
        Releasables.closeExpectNoException(vector);
        assertThat(breaker.getUsed(), is(0L));
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

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
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
        DocBlock block = new DocVector(intVector(positionCount), intVector(positionCount), intVector(positionCount), true).asBlock();
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
        DocVector vector = new DocVector(intVector(positionCount), intVector(positionCount), intVector(positionCount), true);
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
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
}
