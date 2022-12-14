/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Experimental
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
    public long getEstimatedSize() {
        return Long.BYTES;
    }

    @Override
    public void close() {}

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
