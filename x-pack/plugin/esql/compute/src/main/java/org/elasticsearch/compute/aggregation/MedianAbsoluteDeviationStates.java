/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

final class MedianAbsoluteDeviationStates {
    private MedianAbsoluteDeviationStates() {

    }

    private static final double DEFAULT_COMPRESSION = 1000.0;

    static class UngroupedState implements AggregatorState<UngroupedState> {
        private QuantileState quantile;

        UngroupedState() {
            this(new QuantileState(DEFAULT_COMPRESSION));
        }

        UngroupedState(QuantileState quantile) {
            this.quantile = quantile;
        }

        @Override
        public long getEstimatedSize() {
            return quantile.estimateSizeInBytes();
        }

        @Override
        public void close() {

        }

        void add(double v) {
            quantile.add(v);
        }

        void add(UngroupedState other) {
            quantile.add(other.quantile);
        }

        Block evaluateFinal() {
            double result = quantile.computeMedianAbsoluteDeviation();
            return BlockBuilder.newConstantDoubleBlockWith(result, 1);
        }

        @Override
        public AggregatorStateSerializer<UngroupedState> serializer() {
            return new UngroupedStateSerializer();
        }
    }

    static class UngroupedStateSerializer implements AggregatorStateSerializer<UngroupedState> {
        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int serialize(UngroupedState state, byte[] ba, int offset) {
            return state.quantile.serialize(ba, offset);
        }

        @Override
        public void deserialize(UngroupedState state, byte[] ba, int offset) {
            state.quantile = QuantileState.deserialize(ba, offset);
        }
    }

    static class GroupingState implements AggregatorState<GroupingState> {
        private final GroupingStateSerializer serializer;
        private long largestGroupId = -1;
        private ObjectArray<QuantileState> quantiles;
        private final BigArrays bigArrays;

        GroupingState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.serializer = new GroupingStateSerializer();
            this.quantiles = bigArrays.newObjectArray(1);
        }

        private TDigestState getOrAddGroup(int groupId) {
            if (groupId > largestGroupId) {
                quantiles = bigArrays.grow(quantiles, groupId + 1);
                largestGroupId = groupId;
            }
            QuantileState qs = quantiles.get(groupId);
            if (qs == null) {
                qs = new QuantileState(DEFAULT_COMPRESSION);
                quantiles.set(groupId, qs);
            }
            return qs;
        }

        void add(int groupId, double v) {
            getOrAddGroup(groupId).add(v);
        }

        void add(int groupId, TDigestState other) {
            getOrAddGroup(groupId).add(other);
        }

        TDigestState get(int position) {
            return quantiles.get(position);
        }

        Block evaluateFinal() {
            final int positions = Math.toIntExact(largestGroupId + 1);
            double[] result = new double[positions];
            for (int i = 0; i < positions; i++) {
                result[i] = quantiles.get(i).computeMedianAbsoluteDeviation();
            }
            return new DoubleVector(result, positions).asBlock();
        }

        @Override
        public long getEstimatedSize() {
            long size = 8;
            for (long i = 0; i <= largestGroupId; i++) {
                size += quantiles.get(i).estimateSizeInBytes();
            }
            return size;
        }

        @Override
        public void close() {
            quantiles.close();
        }

        @Override
        public AggregatorStateSerializer<GroupingState> serializer() {
            return serializer;
        }
    }

    static class GroupingStateSerializer implements AggregatorStateSerializer<GroupingState> {
        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int serialize(GroupingState state, byte[] ba, int offset) {
            final int origOffset = offset;
            final ObjectArray<QuantileState> digests = state.quantiles;
            longHandle.set(ba, offset, state.largestGroupId);
            offset += 8;
            for (long i = 0; i <= state.largestGroupId; i++) {
                offset += digests.get(i).serialize(ba, offset);
            }
            return origOffset - offset;
        }

        @Override
        public void deserialize(GroupingState state, byte[] ba, int offset) {
            state.largestGroupId = (long) longHandle.get(ba, offset);
            offset += 8;
            state.quantiles = state.bigArrays.newObjectArray(state.largestGroupId + 1);
            for (long i = 0; i <= state.largestGroupId; i++) {
                QuantileState qs = QuantileState.deserialize(ba, offset);
                offset += qs.estimateSizeInBytes();
                state.quantiles.set(i, qs);
            }
        }
    }
}
