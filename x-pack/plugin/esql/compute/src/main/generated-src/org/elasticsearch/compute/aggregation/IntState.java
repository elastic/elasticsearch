/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.IntVector;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Aggregator state for a single int.
 * This class is generated. Do not edit it.
 */
@Experimental
final class IntState implements AggregatorState<IntState> {
    private int value;

    IntState() {
        this(0);
    }

    IntState(int init) {
        this.value = init;
    }

    int intValue() {
        return value;
    }

    void intValue(int value) {
        this.value = value;
    }

    @Override
    public long getEstimatedSize() {
        return Integer.BYTES;
    }

    @Override
    public void close() {}

    @Override
    public AggregatorStateSerializer<IntState> serializer() {
        return new IntStateSerializer();
    }

    private static class IntStateSerializer implements AggregatorStateSerializer<IntState> {
        private static final VarHandle handle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int size() {
            return Integer.BYTES;
        }

        @Override
        public int serialize(IntState state, byte[] ba, int offset, IntVector selected) {
            assert selected.getPositionCount() == 1;
            assert selected.getInt(0) == 0;
            handle.set(ba, offset, state.value);
            return Integer.BYTES; // number of bytes written
        }

        @Override
        public void deserialize(IntState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            state.value = (int) handle.get(ba, offset);
        }
    }
}
