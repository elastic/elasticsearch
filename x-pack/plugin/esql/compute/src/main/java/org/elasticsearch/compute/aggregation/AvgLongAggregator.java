/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Aggregator
class AvgLongAggregator { // TODO use @GroupingAggregator to generate AvgLongGroupingAggregator
    public static AvgState init() {
        return new AvgState();
    }

    public static void combine(AvgState current, long v) {
        current.value = Math.addExact(current.value, v);
    }

    public static void combineValueCount(AvgState current, int positions) {
        current.count += positions;
    }

    public static void combineStates(AvgState current, AvgState state) {
        current.value = Math.addExact(current.value, state.value);
        current.count += state.count;
    }

    public static Block evaluateFinal(AvgState state) {
        double result = ((double) state.value) / state.count;
        return BlockBuilder.newConstantDoubleBlockWith(result, 1);
    }

    static class AvgState implements AggregatorState<AvgLongAggregator.AvgState> {

        long value;
        long count;

        private final AvgLongAggregator.AvgStateSerializer serializer;

        AvgState() {
            this(0, 0);
        }

        AvgState(long value, long count) {
            this.value = value;
            this.count = count;
            this.serializer = new AvgLongAggregator.AvgStateSerializer();
        }

        @Override
        public long getEstimatedSize() {
            return AvgLongAggregator.AvgStateSerializer.BYTES_SIZE;
        }

        @Override
        public void close() {}

        @Override
        public AggregatorStateSerializer<AvgLongAggregator.AvgState> serializer() {
            return serializer;
        }
    }

    // @SerializedSize(value = Long.BYTES + Long.BYTES)
    static class AvgStateSerializer implements AggregatorStateSerializer<AvgLongAggregator.AvgState> {

        // record Shape (long value, long count) {}

        static final int BYTES_SIZE = Long.BYTES + Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(AvgLongAggregator.AvgState value, byte[] ba, int offset) {
            longHandle.set(ba, offset, value.value);
            longHandle.set(ba, offset + 8, value.count);
            return BYTES_SIZE; // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(AvgLongAggregator.AvgState value, byte[] ba, int offset) {
            Objects.requireNonNull(value);
            long kvalue = (long) longHandle.get(ba, offset);
            long count = (long) longHandle.get(ba, offset + 8);

            value.value = kvalue;
            value.count = count;
        }
    }
}
