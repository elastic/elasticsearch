/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.compute.data.BasicBlockTests.assertEmptyLookup;
import static org.elasticsearch.compute.data.BasicBlockTests.assertLookup;
import static org.elasticsearch.compute.data.BasicBlockTests.positions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BigArrayVectorTests extends SerializationTestCase {

    final MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService());

    public void testBoolean() throws IOException {
        int positionCount = randomIntBetween(1, 16 * 1024);
        Boolean value = randomFrom(random(), null, true, false);
        Boolean[] values = IntStream.range(0, positionCount).mapToObj(i -> {
            if (value == null) {
                return randomBoolean();
            }
            return value;
        }).toArray(Boolean[]::new);
        BitArray array = new BitArray(positionCount, bigArrays);
        IntStream.range(0, positionCount).filter(i -> values[i]).forEach(array::set);
        try (var vector = new BooleanBigArrayVector(array, positionCount, blockFactory)) {
            assertThat(vector.elementType(), is(ElementType.BOOLEAN));
            assertThat(positionCount, is(vector.getPositionCount()));
            IntStream.range(0, positionCount).forEach(i -> assertThat(vector.getBoolean(i), is(values[i])));
            assertThat(vector.isConstant(), is(false));
            try (BooleanVector filtered = vector.filter(IntStream.range(0, positionCount).toArray())) {
                IntStream.range(0, positionCount).forEach(i -> assertThat(filtered.getBoolean(i), is(values[i])));
                assertThat(filtered.isConstant(), is(false));
            }
            BooleanBlock block = vector.asBlock();
            assertThat(block, is(notNullValue()));
            IntStream.range(0, positionCount).forEach(i -> {
                assertThat(block.getBoolean(i), is(values[i]));
                assertThat(block.isNull(i), is(false));
                assertThat(block.getValueCount(i), is(1));
                assertThat(block.getFirstValueIndex(i), is(i));
                try (BooleanBlock filter = block.filter(i)) {
                    assertThat(filter.getBoolean(0), is(values[i]));
                }
            });
            BasicBlockTests.assertSingleValueDenseBlock(vector.asBlock());
            if (positionCount > 1) {
                assertLookup(
                    vector.asBlock(),
                    positions(blockFactory, 0, 1, new int[] { 0, 1 }),
                    List.of(List.of(values[0]), List.of(values[1]), List.of(values[0], values[1]))
                );
            }
            assertLookup(vector.asBlock(), positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, vector.asBlock());
            assertSerialization(block);
            assertThat(vector.toString(), containsString("BooleanBigArrayVector[positions=" + positionCount));
            try (ToMask mask = block.toMask()) {
                assertThat(mask.hadMultivaluedFields(), equalTo(false));
                for (int p = 0; p < values.length; p++) {
                    assertThat(mask.mask().getBoolean(p), equalTo(values[p]));
                }
            }
            if (value == null) {
                assertThat(vector.allTrue(), equalTo(Arrays.stream(values).allMatch(v -> v)));
                assertThat(vector.allFalse(), equalTo(Arrays.stream(values).allMatch(v -> v == false)));
            } else {
                if (value) {
                    assertTrue(vector.allTrue());
                    assertFalse(vector.allFalse());
                } else {
                    assertFalse(vector.allTrue());
                    assertTrue(vector.allFalse());
                }
            }
        }
    }

    public void testInt() throws IOException {
        int positionCount = randomIntBetween(1, 16 * 1024);
        int[] values = IntStream.range(0, positionCount).map(i -> randomInt()).toArray();
        IntArray array = bigArrays.newIntArray(positionCount);
        IntStream.range(0, positionCount).forEach(i -> array.set(i, values[i]));
        try (var vector = new IntBigArrayVector(array, positionCount, blockFactory)) {
            assertThat(vector.elementType(), is(ElementType.INT));
            assertThat(positionCount, is(vector.getPositionCount()));
            IntStream.range(0, positionCount).forEach(i -> assertThat(vector.getInt(i), is(values[i])));
            assertThat(vector.isConstant(), is(false));
            try (IntVector filtered = vector.filter(IntStream.range(0, positionCount).toArray())) {
                IntStream.range(0, positionCount).forEach(i -> assertThat(filtered.getInt(i), is(values[i])));
                assertThat(filtered.isConstant(), is(false));
            }
            IntBlock block = vector.asBlock();
            assertThat(block, is(notNullValue()));
            IntStream.range(0, positionCount).forEach(i -> {
                assertThat(block.getInt(i), is(values[i]));
                assertThat(block.isNull(i), is(false));
                assertThat(block.getValueCount(i), is(1));
                assertThat(block.getFirstValueIndex(i), is(i));
                try (IntBlock filter = block.filter(i)) {
                    assertThat(filter.getInt(0), is(values[i]));
                }
            });
            BasicBlockTests.assertSingleValueDenseBlock(vector.asBlock());
            if (positionCount > 1) {
                assertLookup(
                    vector.asBlock(),
                    positions(blockFactory, 0, 1, new int[] { 0, 1 }),
                    List.of(List.of(values[0]), List.of(values[1]), List.of(values[0], values[1]))
                );
            }
            assertLookup(vector.asBlock(), positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, vector.asBlock());
            assertThat(OptionalInt.of(vector.min()), equalTo(Arrays.stream(values).min()));
            assertThat(OptionalInt.of(vector.max()), equalTo(Arrays.stream(values).max()));
            assertSerialization(block);
            assertThat(vector.toString(), containsString("IntBigArrayVector[positions=" + positionCount));
        }
    }

    public void testLong() throws IOException {
        int positionCount = randomIntBetween(1, 16 * 1024);
        long[] values = IntStream.range(0, positionCount).mapToLong(i -> randomLong()).toArray();
        LongArray array = bigArrays.newLongArray(positionCount);
        IntStream.range(0, positionCount).forEach(i -> array.set(i, values[i]));
        try (var vector = new LongBigArrayVector(array, positionCount, blockFactory)) {
            assertThat(vector.elementType(), is(ElementType.LONG));
            assertThat(positionCount, is(vector.getPositionCount()));
            IntStream.range(0, positionCount).forEach(i -> assertThat(vector.getLong(i), is(values[i])));
            assertThat(vector.isConstant(), is(false));
            try (LongVector filtered = vector.filter(IntStream.range(0, positionCount).toArray())) {
                IntStream.range(0, positionCount).forEach(i -> assertThat(filtered.getLong(i), is(values[i])));
                assertThat(filtered.isConstant(), is(false));
            }
            LongBlock block = vector.asBlock();
            assertThat(block, is(notNullValue()));
            IntStream.range(0, positionCount).forEach(i -> {
                assertThat(block.getLong(i), is(values[i]));
                assertThat(block.isNull(i), is(false));
                assertThat(block.getValueCount(i), is(1));
                assertThat(block.getFirstValueIndex(i), is(i));
                try (LongBlock filter = block.filter(i)) {
                    assertThat(filter.getLong(0), is(values[i]));
                }
            });
            BasicBlockTests.assertSingleValueDenseBlock(vector.asBlock());
            if (positionCount > 1) {
                assertLookup(
                    vector.asBlock(),
                    positions(blockFactory, 0, 1, new int[] { 0, 1 }),
                    List.of(List.of(values[0]), List.of(values[1]), List.of(values[0], values[1]))
                );
            }
            assertLookup(vector.asBlock(), positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, vector.asBlock());
            assertSerialization(block);
            assertThat(vector.toString(), containsString("LongBigArrayVector[positions=" + positionCount));
        }
    }

    public void testDouble() throws IOException {
        int positionCount = randomIntBetween(1, 16 * 1024);
        double[] values = IntStream.range(0, positionCount).mapToDouble(i -> randomDouble()).toArray();
        DoubleArray array = bigArrays.newDoubleArray(positionCount);
        IntStream.range(0, positionCount).forEach(i -> array.set(i, values[i]));
        try (var vector = new DoubleBigArrayVector(array, positionCount, blockFactory)) {
            assertThat(vector.elementType(), is(ElementType.DOUBLE));
            assertThat(positionCount, is(vector.getPositionCount()));
            IntStream.range(0, positionCount).forEach(i -> assertThat(vector.getDouble(i), is(values[i])));
            assertThat(vector.isConstant(), is(false));
            try (DoubleVector filtered = vector.filter(IntStream.range(0, positionCount).toArray())) {
                IntStream.range(0, positionCount).forEach(i -> assertThat(filtered.getDouble(i), is(values[i])));
                assertThat(filtered.isConstant(), is(false));
            }
            DoubleBlock block = vector.asBlock();
            assertThat(block, is(notNullValue()));
            IntStream.range(0, positionCount).forEach(i -> {
                assertThat(block.getDouble(i), is(values[i]));
                assertThat(block.isNull(i), is(false));
                assertThat(block.getValueCount(i), is(1));
                assertThat(block.getFirstValueIndex(i), is(i));
                try (DoubleBlock filter = block.filter(i)) {
                    assertThat(filter.getDouble(0), is(values[i]));
                }
            });
            BasicBlockTests.assertSingleValueDenseBlock(vector.asBlock());
            if (positionCount > 1) {
                assertLookup(
                    vector.asBlock(),
                    positions(blockFactory, 0, 1, new int[] { 0, 1 }),
                    List.of(List.of(values[0]), List.of(values[1]), List.of(values[0], values[1]))
                );
            }
            assertLookup(vector.asBlock(), positions(blockFactory, positionCount + 1000), singletonList(null));
            assertEmptyLookup(blockFactory, vector.asBlock());
            assertSerialization(block);
            assertThat(vector.toString(), containsString("DoubleBigArrayVector[positions=" + positionCount));
        }
    }

    void assertSerialization(Block origBlock) throws IOException {
        try (Block deserBlock = serializeDeserializeBlock(origBlock)) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock.asVector(), unused -> deserBlock.asVector());
            assertThat(deserBlock.asVector(), is(origBlock.asVector()));
            assertThat(deserBlock.asVector().isConstant(), is(origBlock.asVector().isConstant()));
        }
    }
}
