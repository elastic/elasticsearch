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
class AvgDoubleAggregator { // TODO use @GroupingAggregator to generate AvgLongGroupingAggregator
    public static AvgState init() {
        return new AvgState();
    }

    public static void combine(AvgState current, double v) {
        current.add(v);
    }

    public static void combineValueCount(AvgState current, int positions) {
        current.count += positions;
    }

    public static void combineStates(AvgState current, AvgState state) {
        current.add(state.value, state.delta);
        current.count += state.count;
    }

    public static Block evaluateFinal(AvgState state) {
        double result = state.value / state.count;
        return BlockBuilder.newConstantDoubleBlockWith(result, 1);
    }

    // @SerializedSize(value = Double.BYTES + Double.BYTES + Long.BYTES)
    static class AvgState implements AggregatorState<AvgDoubleAggregator.AvgState> {

        private double value;
        private double delta;

        private long count;

        private final AvgDoubleAggregator.AvgStateSerializer serializer;

        AvgState() {
            this(0, 0, 0);
        }

        AvgState(double value, double delta, long count) {
            this.value = value;
            this.delta = delta;
            this.count = count;
            this.serializer = new AvgDoubleAggregator.AvgStateSerializer();
        }

        void add(double valueToAdd) {
            add(valueToAdd, 0d);
        }

        void add(double valueToAdd, double deltaToAdd) {
            // If the value is Inf or NaN, just add it to the running tally to "convert" to
            // Inf/NaN. This keeps the behavior bwc from before kahan summing
            if (Double.isFinite(valueToAdd) == false) {
                value = valueToAdd + value;
            }

            if (Double.isFinite(value)) {
                double correctedSum = valueToAdd + (delta + deltaToAdd);
                double updatedValue = value + correctedSum;
                delta = correctedSum - (updatedValue - value);
                value = updatedValue;
            }
        }

        @Override
        public long getEstimatedSize() {
            return AvgStateSerializer.BYTES_SIZE;
        }

        @Override
        public void close() {}

        @Override
        public AggregatorStateSerializer<AvgDoubleAggregator.AvgState> serializer() {
            return serializer;
        }
    }

    // @SerializedSize(value = Double.BYTES + Double.BYTES + Long.BYTES)
    static class AvgStateSerializer implements AggregatorStateSerializer<AvgDoubleAggregator.AvgState> {

        // record Shape (double value, double delta, long count) {}

        static final int BYTES_SIZE = Double.BYTES + Double.BYTES + Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(AvgDoubleAggregator.AvgState value, byte[] ba, int offset) {
            doubleHandle.set(ba, offset, value.value);
            doubleHandle.set(ba, offset + 8, value.delta);
            longHandle.set(ba, offset + 16, value.count);
            return BYTES_SIZE; // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(AvgDoubleAggregator.AvgState value, byte[] ba, int offset) {
            Objects.requireNonNull(value);
            double kvalue = (double) doubleHandle.get(ba, offset);
            double kdelta = (double) doubleHandle.get(ba, offset + 8);
            long count = (long) longHandle.get(ba, offset + 16);

            value.value = kvalue;
            value.delta = kdelta;
            value.count = count;
        }
    }
}
