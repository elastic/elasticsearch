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

final class LongState implements AggregatorState<LongState> {

    private long longValue;

    private final LongStateSerializer serializer;

    LongState() {
        this(0);
    }

    LongState(long value) {
        this.longValue = value;
        this.serializer = new LongStateSerializer();
    }

    long longValue() {
        return longValue;
    }

    void longValue(long value) {
        this.longValue = value;
    }

    @Override
    public AggregatorStateSerializer<LongState> serializer() {
        return serializer;
    }

    static class LongStateSerializer implements AggregatorStateSerializer<LongState> {

        static final int BYTES_SIZE = Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(LongState state, byte[] ba, int offset) {
            longHandle.set(ba, offset, state.longValue);
            return BYTES_SIZE; // number of bytes written
        }

        // sets the long value in the given state.
        @Override
        public void deserialize(LongState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            state.longValue = (long) longHandle.get(ba, offset);
        }
    }
}
