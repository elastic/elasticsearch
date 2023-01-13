/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.tdunning.math.stats.Centroid;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

final class QuantileStates {
    private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    private QuantileStates() {

    }

    static int estimateSizeInBytes(TDigestState digest) {
        return 12 + (12 * digest.centroidCount());
    }

    static int serializeDigest(TDigestState digest, byte[] ba, int offset) {
        doubleHandle.set(ba, offset, digest.compression());
        intHandle.set(ba, offset + 8, digest.centroidCount());
        offset += 12;
        for (Centroid centroid : digest.centroids()) {
            doubleHandle.set(ba, offset, centroid.mean());
            intHandle.set(ba, offset + 8, centroid.count());
            offset += 12;
        }
        return estimateSizeInBytes(digest);
    }

    static TDigestState deserializeDigest(byte[] ba, int offset) {
        final double compression = (double) doubleHandle.get(ba, offset);
        final TDigestState digest = new TDigestState(compression);
        final int positions = (int) intHandle.get(ba, offset + 8);
        offset += 12;
        for (int i = 0; i < positions; i++) {
            double mean = (double) doubleHandle.get(ba, offset);
            int count = (int) intHandle.get(ba, offset + 8);
            digest.add(mean, count);
            offset += 12;
        }
        return digest;
    }

    private static final double DEFAULT_COMPRESSION = 1000.0;

    static class SingleState implements AggregatorState<SingleState> {
        private TDigestState digest;

        SingleState() {
            this(new TDigestState(DEFAULT_COMPRESSION));
        }

        SingleState(TDigestState digest) {
            this.digest = digest;
        }

        @Override
        public long getEstimatedSize() {
            return estimateSizeInBytes(digest);
        }

        @Override
        public void close() {

        }

        void add(double v) {
            digest.add(v);
        }

        void add(SingleState other) {
            digest.add(other.digest);
        }

        Block evaluateMedianAbsoluteDeviation() {
            double result = digest.computeMedianAbsoluteDeviation();
            return DoubleBlock.newConstantBlockWith(result, 1);
        }

        @Override
        public AggregatorStateSerializer<SingleState> serializer() {
            return new SingleStateSerializer();
        }
    }

    static class SingleStateSerializer implements AggregatorStateSerializer<SingleState> {
        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int serialize(SingleState state, byte[] ba, int offset) {
            return serializeDigest(state.digest, ba, offset);
        }

        @Override
        public void deserialize(SingleState state, byte[] ba, int offset) {
            state.digest = deserializeDigest(ba, offset);
        }
    }

    static class GroupingState implements AggregatorState<GroupingState> {
        private final GroupingStateSerializer serializer;
        private long largestGroupId = -1;
        private ObjectArray<TDigestState> digests;
        private final BigArrays bigArrays;

        GroupingState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.serializer = new GroupingStateSerializer();
            this.digests = bigArrays.newObjectArray(1);
        }

        private TDigestState getOrAddGroup(int groupId) {
            if (groupId > largestGroupId) {
                digests = bigArrays.grow(digests, groupId + 1);
                largestGroupId = groupId;
            }
            TDigestState qs = digests.get(groupId);
            if (qs == null) {
                qs = new TDigestState(DEFAULT_COMPRESSION);
                digests.set(groupId, qs);
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
            return digests.get(position);
        }

        Block evaluateMedianAbsoluteDeviation() {
            final int positions = Math.toIntExact(largestGroupId + 1);
            double[] result = new double[positions];
            for (int i = 0; i < positions; i++) {
                result[i] = digests.get(i).computeMedianAbsoluteDeviation();
            }
            return new DoubleArrayVector(result, positions).asBlock();
        }

        @Override
        public long getEstimatedSize() {
            long size = 8;
            for (long i = 0; i <= largestGroupId; i++) {
                size += estimateSizeInBytes(digests.get(i));
            }
            return size;
        }

        @Override
        public void close() {
            digests.close();
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
            final ObjectArray<TDigestState> digests = state.digests;
            longHandle.set(ba, offset, state.largestGroupId);
            offset += 8;
            for (long i = 0; i <= state.largestGroupId; i++) {
                offset += serializeDigest(digests.get(i), ba, offset);
            }
            return origOffset - offset;
        }

        @Override
        public void deserialize(GroupingState state, byte[] ba, int offset) {
            state.largestGroupId = (long) longHandle.get(ba, offset);
            offset += 8;
            state.digests = state.bigArrays.newObjectArray(state.largestGroupId + 1);
            for (long i = 0; i <= state.largestGroupId; i++) {
                TDigestState digest = deserializeDigest(ba, offset);
                offset += estimateSizeInBytes(digest);
                state.digests.set(i, digest);
            }
        }
    }
}
