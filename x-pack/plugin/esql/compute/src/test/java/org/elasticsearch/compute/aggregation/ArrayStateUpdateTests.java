/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ArrayStateUpdateTests extends ComputeTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (ElementType type : List.of(ElementType.INT, ElementType.LONG, ElementType.FLOAT, ElementType.DOUBLE, ElementType.BOOLEAN)) {
            for (int groups : new int[] { 1, 100, 10_000, BooleanArrayState.PAGE_SIZE }) {
                params.add(new Object[] { type, groups });
            }
        }
        return params;
    }

    private final ElementType type;
    private final int groups;
    private final List<Releasable> states = new ArrayList<>();

    public ArrayStateUpdateTests(@Name("type") ElementType type, @Name("groups") int groups) {
        this.type = type;
        this.groups = groups;
    }

    @After
    public void closeStates() {
        Releasables.close(states);
    }

    public void testMin() {
        boolean tracking = randomBoolean();
        Object init = switch (type) {
            case INT -> Integer.MAX_VALUE;
            case LONG -> Long.MAX_VALUE;
            case FLOAT -> Float.POSITIVE_INFINITY;
            case DOUBLE -> Double.POSITIVE_INFINITY;
            case BOOLEAN -> true;
            default -> throw new IllegalArgumentException(type.toString());
        };
        AbstractArrayState state = newState(init);
        if (tracking) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
        }
        Object[] expected = new Object[groups];
        for (int i = 0; i < groups * 3; i++) {
            int groupId = randomInt(groups - 1);
            Object value = BlockTestUtils.randomValue(type);
            min(state, groupId, value);
            Object current = expected[groupId] == null ? init : expected[groupId];
            expected[groupId] = switch (type) {
                case INT -> Math.min((Integer) current, (Integer) value);
                case LONG -> Math.min((Long) current, (Long) value);
                case FLOAT -> Math.min((Float) current, (Float) value);
                case DOUBLE -> Math.min((Double) current, (Double) value);
                case BOOLEAN -> (Boolean) current && (Boolean) value;
                default -> throw new IllegalArgumentException(type.toString());
            };
        }
        assertValues(state, expected, tracking);
    }

    public void testMax() {
        boolean tracking = randomBoolean();
        Object init = switch (type) {
            case INT -> Integer.MIN_VALUE;
            case LONG -> Long.MIN_VALUE;
            case FLOAT -> -Float.MAX_VALUE;
            case DOUBLE -> -Double.MAX_VALUE;
            case BOOLEAN -> false;
            default -> throw new IllegalArgumentException(type.toString());
        };
        AbstractArrayState state = newState(init);
        if (tracking) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
        }
        Object[] expected = new Object[groups];
        for (int i = 0; i < groups * 3; i++) {
            int groupId = randomInt(groups - 1);
            Object value = BlockTestUtils.randomValue(type);
            max(state, groupId, value);
            Object current = expected[groupId] == null ? init : expected[groupId];
            expected[groupId] = switch (type) {
                case INT -> Math.max((Integer) current, (Integer) value);
                case LONG -> Math.max((Long) current, (Long) value);
                case FLOAT -> Math.max((Float) current, (Float) value);
                case DOUBLE -> Math.max((Double) current, (Double) value);
                case BOOLEAN -> (Boolean) current || (Boolean) value;
                default -> throw new IllegalArgumentException(type.toString());
            };
        }
        assertValues(state, expected, tracking);
    }

    public void testAddExact() {
        assumeTrue("addExact is only for long", type == ElementType.LONG);
        boolean tracking = randomBoolean();
        LongArrayState state = (LongArrayState) newState(0L);
        if (tracking) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
        }
        Object[] expected = new Object[groups];
        for (int i = 0; i < groups * 3; i++) {
            int groupId = randomInt(groups - 1);
            long value = randomLongBetween(-1_000_000, 1_000_000);
            state.addExact(groupId, value);
            long current = expected[groupId] == null ? 0L : (Long) expected[groupId];
            expected[groupId] = current + value;
        }
        assertValues(state, expected, tracking);

        int groupId = randomInt(groups - 1);
        state.set(groupId, Long.MAX_VALUE);
        expectThrows(ArithmeticException.class, () -> state.addExact(groupId, 1));
        assertThat(state.get(groupId), equalTo(Long.MAX_VALUE));
    }

    private void assertValues(AbstractArrayState state, Object[] expected, boolean tracking) {
        for (int groupId = 0; groupId < groups; groupId++) {
            if (expected[groupId] != null) {
                assertTrue(state.hasValue(groupId));
                assertThat(get(state, groupId), equalTo(expected[groupId]));
            } else if (tracking) {
                assertFalse(state.hasValue(groupId));
            }
        }
    }

    private AbstractArrayState newState(Object init) {
        BlockFactory blockFactory = blockFactory();
        AbstractArrayState state = switch (type) {
            case INT -> new IntArrayState(blockFactory.bigArrays(), blockFactory.breaker(), (Integer) init);
            case LONG -> new LongArrayState(blockFactory.bigArrays(), blockFactory.breaker(), (Long) init);
            case FLOAT -> new FloatArrayState(blockFactory.bigArrays(), blockFactory.breaker(), (Float) init);
            case DOUBLE -> new DoubleArrayState(blockFactory.bigArrays(), blockFactory.breaker(), (Double) init);
            case BOOLEAN -> new BooleanArrayState(blockFactory.bigArrays(), blockFactory.breaker(), (Boolean) init);
            default -> throw new IllegalArgumentException(type.toString());
        };
        states.add(state);
        return state;
    }

    private void min(AbstractArrayState state, int groupId, Object value) {
        switch (type) {
            case INT -> ((IntArrayState) state).min(groupId, (Integer) value);
            case LONG -> ((LongArrayState) state).min(groupId, (Long) value);
            case FLOAT -> ((FloatArrayState) state).min(groupId, (Float) value);
            case DOUBLE -> ((DoubleArrayState) state).min(groupId, (Double) value);
            case BOOLEAN -> ((BooleanArrayState) state).min(groupId, (Boolean) value);
            default -> throw new IllegalArgumentException(type.toString());
        }
    }

    private void max(AbstractArrayState state, int groupId, Object value) {
        switch (type) {
            case INT -> ((IntArrayState) state).max(groupId, (Integer) value);
            case LONG -> ((LongArrayState) state).max(groupId, (Long) value);
            case FLOAT -> ((FloatArrayState) state).max(groupId, (Float) value);
            case DOUBLE -> ((DoubleArrayState) state).max(groupId, (Double) value);
            case BOOLEAN -> ((BooleanArrayState) state).max(groupId, (Boolean) value);
            default -> throw new IllegalArgumentException(type.toString());
        }
    }

    private Object get(AbstractArrayState state, int groupId) {
        return switch (type) {
            case INT -> ((IntArrayState) state).get(groupId);
            case LONG -> ((LongArrayState) state).get(groupId);
            case FLOAT -> ((FloatArrayState) state).get(groupId);
            case DOUBLE -> ((DoubleArrayState) state).get(groupId);
            case BOOLEAN -> ((BooleanArrayState) state).get(groupId);
            default -> throw new IllegalArgumentException(type.toString());
        };
    }
}
