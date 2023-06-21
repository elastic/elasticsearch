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
 * Aggregator state for a single long.
 * This class is generated. Do not edit it.
 */
@Experimental
final class LongState implements AggregatorState<LongState> {
    private long value;
    private boolean seen;

    LongState() {
        this(0);
    }

    LongState(long init) {
        this.value = init;
    }

    long longValue() {
        return value;
    }

    void longValue(long value) {
        this.value = value;
    }

    boolean seen() {
        return seen;
    }

    void seen(boolean seen) {
        this.seen = seen;
    }

    @Override
    public long getEstimatedSize() {
        return Long.BYTES + 1;
    }

    @Override
    public void close() {}

    @Override
    public AggregatorStateSerializer<LongState> serializer() {
        return new LongStateSerializer();
    }

    private static class LongStateSerializer implements AggregatorStateSerializer<LongState> {
        private static final VarHandle handle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int size() {
            return Long.BYTES + 1;
        }

        @Override
        public int serialize(LongState state, byte[] ba, int offset, IntVector selected) {
            assert selected.getPositionCount() == 1;
            assert selected.getInt(0) == 0;
            handle.set(ba, offset, state.value);
            ba[offset + Long.BYTES] = (byte) (state.seen ? 1 : 0);
            return size(); // number of bytes written
        }

        @Override
        public void deserialize(LongState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            state.value = (long) handle.get(ba, offset);
            state.seen = ba[offset + Long.BYTES] == (byte) 1;
        }
    }
}
