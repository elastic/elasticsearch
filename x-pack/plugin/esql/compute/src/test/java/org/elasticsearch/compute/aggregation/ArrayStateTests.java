/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ArrayStateTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (boolean inOrder : new boolean[] { true, false }) {
            params.add(new Object[] { DataType.INTEGER, 1000, inOrder });
            params.add(new Object[] { DataType.LONG, 1000, inOrder });
            params.add(new Object[] { DataType.FLOAT, 1000, inOrder });
            params.add(new Object[] { DataType.DOUBLE, 1000, inOrder });
            params.add(new Object[] { DataType.IP, 1000, inOrder });
        }
        return params;
    }

    private final DataType type;
    private final ElementType elementType;
    private final int valueCount;
    private final boolean inOrder;

    public ArrayStateTests(DataType type, int valueCount, boolean inOrder) {
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
        this.valueCount = valueCount;
        this.inOrder = inOrder;
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
