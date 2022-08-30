/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.aggregation;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

final class DoubleState implements AggregatorState<DoubleState> {

    private double doubleValue;

    private final DoubleStateSerializer serializer;

    DoubleState() {
        this(0);
    }

    DoubleState(long value) {
        this.doubleValue = value;
        this.serializer = new DoubleStateSerializer();
    }

    double doubleValue() {
        return doubleValue;
    }

    void doubleValue(double value) {
        this.doubleValue = value;
    }

    @Override
    public AggregatorStateSerializer<DoubleState> serializer() {
        return serializer;
    }

    static class DoubleStateSerializer implements AggregatorStateSerializer<DoubleState> {

        static final int BYTES_SIZE = Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(DoubleState state, byte[] ba, int offset) {
            doubleHandle.set(ba, offset, state.doubleValue());
            return BYTES_SIZE; // number of bytes written
        }

        // sets the long value in the given state.
        @Override
        public void deserialize(DoubleState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            state.doubleValue = (double) doubleHandle.get(ba, offset);
        }
    }
}
