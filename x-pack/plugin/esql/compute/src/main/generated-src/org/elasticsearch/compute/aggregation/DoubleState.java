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
 * Aggregator state for a single double.
 * This class is generated. Do not edit it.
 */
@Experimental
final class DoubleState implements AggregatorState<DoubleState> {
    private double value;

    DoubleState() {
        this(0);
    }

    DoubleState(double init) {
        this.value = init;
    }

    double doubleValue() {
        return value;
    }

    void doubleValue(double value) {
        this.value = value;
    }

    @Override
    public long getEstimatedSize() {
        return Double.BYTES;
    }

    @Override
    public void close() {}

    @Override
    public AggregatorStateSerializer<DoubleState> serializer() {
        return new DoubleStateSerializer();
    }

    private static class DoubleStateSerializer implements AggregatorStateSerializer<DoubleState> {
        private static final VarHandle handle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int size() {
            return Double.BYTES;
        }

        @Override
        public int serialize(DoubleState state, byte[] ba, int offset, IntVector selected) {
            assert selected.getPositionCount() == 1;
            assert selected.getInt(0) == 0;
            handle.set(ba, offset, state.value);
            return Double.BYTES; // number of bytes written
        }

        @Override
        public void deserialize(DoubleState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            state.value = (double) handle.get(ba, offset);
        }
    }
}
