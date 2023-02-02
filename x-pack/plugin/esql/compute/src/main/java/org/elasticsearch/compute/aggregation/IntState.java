/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Experimental;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Experimental
final class IntState implements AggregatorState<IntState> {
    private int intValue;

    private final LongStateSerializer serializer;

    IntState() {
        this(0);
    }

    IntState(int value) {
        this.intValue = value;
        this.serializer = new LongStateSerializer();
    }

    int intValue() {
        return intValue;
    }

    void intValue(int value) {
        this.intValue = value;
    }

    @Override
    public long getEstimatedSize() {
        return Integer.BYTES;
    }

    @Override
    public void close() {}

    @Override
    public AggregatorStateSerializer<IntState> serializer() {
        return serializer;
    }

    static class LongStateSerializer implements AggregatorStateSerializer<IntState> {

        static final int BYTES_SIZE = Integer.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(IntState state, byte[] ba, int offset) {
            intHandle.set(ba, offset, state.intValue);
            return BYTES_SIZE; // number of bytes written
        }

        // sets the long value in the given state.
        @Override
        public void deserialize(IntState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            state.intValue = (int) intHandle.get(ba, offset);
        }
    }
}
