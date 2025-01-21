/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.IntSupplier;

import static org.hamcrest.Matchers.equalTo;

public class ArrayStateTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (boolean inOrder : new boolean[] { true, false }) {
            for (IntSupplier count : new IntSupplier[] { new Fixed(100), new Fixed(1000), new Random(100, 5000) }) {
                params.add(new Object[] { DataType.INTEGER, count, inOrder });
                params.add(new Object[] { DataType.LONG, count, inOrder });
                params.add(new Object[] { DataType.FLOAT, count, inOrder });
                params.add(new Object[] { DataType.DOUBLE, count, inOrder });
                params.add(new Object[] { DataType.IP, count, inOrder });
            }
        }
        return params;
    }

    private record Fixed(int i) implements IntSupplier {
        @Override
        public int getAsInt() {
            return i;
        }
    }

    private record Random(int min, int max) implements IntSupplier {
        @Override
        public int getAsInt() {
            return randomIntBetween(min, max);
        }
    }

    private final DataType type;
    private final ElementType elementType;
    private final int valueCount;
    private final boolean inOrder;

    public ArrayStateTests(@Name("type") DataType type, @Name("valueCount") IntSupplier valueCount, @Name("inOrder") boolean inOrder) {
        this.type = type;
        this.elementType = switch (type) {
            case INTEGER -> ElementType.INT;
            case LONG -> ElementType.LONG;
            case FLOAT -> ElementType.FLOAT;
            case DOUBLE -> ElementType.DOUBLE;
            case BOOLEAN -> ElementType.BOOLEAN;
            case IP -> ElementType.BYTES_REF;
            default -> throw new IllegalArgumentException();
        };
        this.valueCount = valueCount.getAsInt();
        this.inOrder = inOrder;
        logger.info("value count is {}", this.valueCount);
    }

    public void testSetNoTracking() {
        List<Object> values = randomList(valueCount, valueCount, this::randomValue);

        AbstractArrayState state = newState();
        setAll(state, values, 0);
        for (int i = 0; i < values.size(); i++) {
            assertTrue(state.hasValue(i));
            assertThat(get(state, i), equalTo(values.get(i)));
        }
    }

    public void testSetWithoutTrackingThenSetWithTracking() {
        List<Object> values = randomList(valueCount, valueCount, this::nullableRandomValue);

        AbstractArrayState state = newState();
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        setAll(state, values, 0);
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) == null) {
                assertFalse(state.hasValue(i));
            } else {
                assertTrue(state.hasValue(i));
                assertThat(get(state, i), equalTo(values.get(i)));
            }
        }
    }

    public void testSetWithTracking() {
        List<Object> withoutNulls = randomList(valueCount, valueCount, this::randomValue);
        List<Object> withNulls = randomList(valueCount, valueCount, this::nullableRandomValue);

        AbstractArrayState state = newState();
        setAll(state, withoutNulls, 0);
        state.enableGroupIdTracking(new SeenGroupIds.Range(0, withoutNulls.size()));
        setAll(state, withNulls, withoutNulls.size());

        for (int i = 0; i < withoutNulls.size(); i++) {
            assertTrue(state.hasValue(i));
            assertThat(get(state, i), equalTo(withoutNulls.get(i)));
        }
        for (int i = 0; i < withNulls.size(); i++) {
            if (withNulls.get(i) == null) {
                assertFalse(state.hasValue(i + withoutNulls.size()));
            } else {
                assertTrue(state.hasValue(i + withoutNulls.size()));
                assertThat(get(state, i + withoutNulls.size()), equalTo(withNulls.get(i)));
            }
        }
    }

    public void testSetNotNullableThenOverwriteNullable() {
        List<Object> first = randomList(valueCount, valueCount, this::randomValue);
        List<Object> second = randomList(valueCount, valueCount, this::nullableRandomValue);

        AbstractArrayState state = newState();
        setAll(state, first, 0);
        state.enableGroupIdTracking(new SeenGroupIds.Range(0, valueCount));
        setAll(state, second, 0);

        for (int i = 0; i < valueCount; i++) {
            assertTrue(state.hasValue(i));
            Object expected = second.get(i);
            expected = expected == null ? first.get(i) : expected;
            assertThat(get(state, i), equalTo(expected));
        }
    }

    public void testSetNullableThenOverwriteNullable() {
        List<Object> first = randomList(valueCount, valueCount, this::nullableRandomValue);
        List<Object> second = randomList(valueCount, valueCount, this::nullableRandomValue);

        AbstractArrayState state = newState();
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        setAll(state, first, 0);
        setAll(state, second, 0);

        for (int i = 0; i < valueCount; i++) {
            Object expected = second.get(i);
            expected = expected == null ? first.get(i) : expected;
            if (expected == null) {
                assertFalse(state.hasValue(i));
            } else {
                assertTrue(state.hasValue(i));
                assertThat(get(state, i), equalTo(expected));
            }
        }
    }

    public void testToIntermediate() {
        AbstractArrayState state = newState();
        List<Object> values = randomList(valueCount, valueCount, this::randomValue);
        setAll(state, values, 0);
        Block[] intermediate = new Block[2];
        DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance());
        state.toIntermediate(intermediate, 0, IntVector.range(0, valueCount, ctx.blockFactory()), ctx);
        try {
            assertThat(intermediate[0].elementType(), equalTo(elementType));
            assertThat(intermediate[1].elementType(), equalTo(ElementType.BOOLEAN));
            assertThat(intermediate[0].getPositionCount(), equalTo(values.size()));
            assertThat(intermediate[1].getPositionCount(), equalTo(values.size()));
            for (int i = 0; i < values.size(); i++) {
                Object v = values.get(i);
                assertThat(
                    String.format(Locale.ROOT, "%05d: %s", i, v != null ? v : "init"),
                    BlockUtils.toJavaObject(intermediate[0], i),
                    equalTo(v != null ? v : initialValue())
                );
                assertThat(BlockUtils.toJavaObject(intermediate[1], i), equalTo(true));
            }
        } finally {
            Releasables.close(intermediate);
        }
    }

    /**
     * Calls {@link GroupingAggregatorState#toIntermediate} with a range that's greater than
     * any collected values. This is acceptable if {@link AbstractArrayState#enableGroupIdTracking}
     * is called, so we do that.
     */
    public void testToIntermediatePastEnd() {
        int end = valueCount + between(1, 10000);
        AbstractArrayState state = newState();
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        List<Object> values = randomList(valueCount, valueCount, this::randomValue);
        setAll(state, values, 0);
        Block[] intermediate = new Block[2];
        DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance());
        state.toIntermediate(intermediate, 0, IntVector.range(0, end, ctx.blockFactory()), ctx);
        try {
            assertThat(intermediate[0].elementType(), equalTo(elementType));
            assertThat(intermediate[1].elementType(), equalTo(ElementType.BOOLEAN));
            assertThat(intermediate[0].getPositionCount(), equalTo(end));
            assertThat(intermediate[1].getPositionCount(), equalTo(end));
            for (int i = 0; i < values.size(); i++) {
                Object v = values.get(i);
                assertThat(
                    String.format(Locale.ROOT, "%05d: %s", i, v != null ? v : "init"),
                    BlockUtils.toJavaObject(intermediate[0], i),
                    equalTo(v != null ? v : initialValue())
                );
                assertThat(BlockUtils.toJavaObject(intermediate[1], i), equalTo(v != null));
            }
            for (int i = values.size(); i < end; i++) {
                assertThat(BlockUtils.toJavaObject(intermediate[1], i), equalTo(false));
            }
        } finally {
            Releasables.close(intermediate);
        }
    }

    private record ValueAndIndex(int index, Object value) {}

    private void setAll(AbstractArrayState state, List<Object> values, int offset) {
        if (inOrder) {
            for (int i = 0; i < values.size(); i++) {
                if (values.get(i) != null) {
                    set(state, i + offset, values.get(i));
                }
            }
            return;
        }
        List<ValueAndIndex> shuffled = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            shuffled.add(new ValueAndIndex(i, values.get(i)));
        }
        Randomness.shuffle(shuffled);
        for (ValueAndIndex v : shuffled) {
            if (v.value != null) {
                set(state, v.index + offset, v.value);
            }
        }
    }

    private AbstractArrayState newState() {
        return switch (type) {
            case INTEGER -> new IntArrayState(BigArrays.NON_RECYCLING_INSTANCE, 1);
            case LONG -> new LongArrayState(BigArrays.NON_RECYCLING_INSTANCE, 1);
            case FLOAT -> new FloatArrayState(BigArrays.NON_RECYCLING_INSTANCE, 1);
            case DOUBLE -> new DoubleArrayState(BigArrays.NON_RECYCLING_INSTANCE, 1);
            case BOOLEAN -> new BooleanArrayState(BigArrays.NON_RECYCLING_INSTANCE, false);
            case IP -> new IpArrayState(BigArrays.NON_RECYCLING_INSTANCE, new BytesRef(new byte[16]));
            default -> throw new IllegalArgumentException();
        };
    }

    private Object initialValue() {
        return switch (type) {
            case INTEGER -> 1;
            case LONG -> 1L;
            case FLOAT -> 1F;
            case DOUBLE -> 1d;
            case BOOLEAN -> false;
            case IP -> new BytesRef(new byte[16]);
            default -> throw new IllegalArgumentException();
        };
    }

    private void set(AbstractArrayState state, int groupId, Object value) {
        switch (type) {
            case INTEGER -> ((IntArrayState) state).set(groupId, (Integer) value);
            case LONG -> ((LongArrayState) state).set(groupId, (Long) value);
            case FLOAT -> ((FloatArrayState) state).set(groupId, (Float) value);
            case DOUBLE -> ((DoubleArrayState) state).set(groupId, (Double) value);
            case BOOLEAN -> ((BooleanArrayState) state).set(groupId, (Boolean) value);
            case IP -> ((IpArrayState) state).set(groupId, (BytesRef) value);
            default -> throw new IllegalArgumentException();
        }
    }

    private Object get(AbstractArrayState state, int index) {
        return switch (type) {
            case INTEGER -> ((IntArrayState) state).get(index);
            case LONG -> ((LongArrayState) state).get(index);
            case FLOAT -> ((FloatArrayState) state).get(index);
            case DOUBLE -> ((DoubleArrayState) state).get(index);
            case BOOLEAN -> ((BooleanArrayState) state).get(index);
            case IP -> ((IpArrayState) state).get(index, new BytesRef());
            default -> throw new IllegalArgumentException();
        };
    }

    private Object randomValue() {
        return switch (type) {
            case INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN -> BlockTestUtils.randomValue(elementType);
            case IP -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
            default -> throw new IllegalArgumentException();
        };
    }

    private Object nullableRandomValue() {
        return randomBoolean() ? null : randomValue();
    }

}
