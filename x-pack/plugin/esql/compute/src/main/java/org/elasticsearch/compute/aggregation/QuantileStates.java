/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.LongStream;

public final class QuantileStates {
    public static final double MEDIAN = 50.0;
    static final double DEFAULT_COMPRESSION = 1000.0;

    private QuantileStates() {}

    private static Double percentileParam(double p) {
        // Percentile must be a double between 0 and 100 inclusive
        // If percentile parameter is wrong, the aggregation will return NULL
        return 0 <= p && p <= 100 ? p : null;
    }

    static BytesRef serializeDigest(TDigestState digest) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
        try {
            TDigestState.write(digest, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new BytesRef(baos.toByteArray());
    }

    static TDigestState deserializeDigest(CircuitBreaker breaker, BytesRef bytesRef) {
        ByteArrayStreamInput in = new ByteArrayStreamInput(bytesRef.bytes);
        in.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        try {
            return TDigestState.read(breaker, in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class SingleState implements AggregatorState {
        private final CircuitBreaker breaker;
        private final TDigestState digest;
        private final Double percentile;

        SingleState(CircuitBreaker breaker, double percentile) {
            this.breaker = breaker;
            this.digest = TDigestState.create(breaker, DEFAULT_COMPRESSION);
            this.percentile = percentileParam(percentile);
        }

        @Override
        public void close() {
            Releasables.close(digest);
        }

        void add(double v) {
            digest.add(v);
        }

        void add(BytesRef other) {
            try (var otherDigest = deserializeDigest(breaker, other)) {
                digest.add(otherDigest);
            }
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(serializeDigest(this.digest), 1);
        }

        Block evaluateMedianAbsoluteDeviation(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            assert percentile == MEDIAN : "Median must be 50th percentile [percentile = " + percentile + "]";
            if (digest.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            double result = InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation(digest);
            return blockFactory.newConstantDoubleBlockWith(result, 1);
        }

        Block evaluatePercentile(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            if (percentile == null || digest.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            double result = digest.quantile(percentile / 100);
            return blockFactory.newConstantDoubleBlockWith(result, 1);
        }
    }

    static class GroupingState implements GroupingAggregatorState {
        private long largestGroupId = -1;
        private ObjectArray<TDigestState> digests;
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private final Double percentile;

        GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double percentile) {
            this.breaker = breaker;
            this.bigArrays = bigArrays;
            this.digests = bigArrays.newObjectArray(1);
            this.percentile = percentileParam(percentile);
        }

        private TDigestState getOrAddGroup(int groupId) {
            digests = bigArrays.grow(digests, groupId + 1);
            TDigestState qs = digests.get(groupId);
            if (qs == null) {
                qs = TDigestState.create(breaker, DEFAULT_COMPRESSION);
                digests.set(groupId, qs);
            }
            return qs;
        }

        void add(int groupId, double v) {
            getOrAddGroup(groupId).add(v);
        }

        void add(int groupId, TDigestState other) {
            if (other != null) {
                getOrAddGroup(groupId).add(other);
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // We always enable.
        }

        void add(int groupId, BytesRef other) {
            try (var digestToAdd = deserializeDigest(breaker, other)) {
                getOrAddGroup(groupId).add(digestToAdd);
            }
        }

        TDigestState getOrNull(int position) {
            if (position < digests.size()) {
                return digests.get(position);
            } else {
                return null;
            }
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    TDigestState state;
                    boolean closeState = false;
                    if (group < digests.size()) {
                        state = getOrNull(group);
                        if (state == null) {
                            state = TDigestState.create(breaker, DEFAULT_COMPRESSION);
                            closeState = true;
                        }
                    } else {
                        state = TDigestState.create(breaker, DEFAULT_COMPRESSION);
                        closeState = true;
                    }
                    builder.appendBytesRef(serializeDigest(state));

                    if (closeState) {
                        state.close();
                    }
                }
                blocks[offset] = builder.build();
            }
        }

        Block evaluateMedianAbsoluteDeviation(IntVector selected, DriverContext driverContext) {
            assert percentile == MEDIAN : "Median must be 50th percentile [percentile = " + percentile + "]";
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int si = selected.getInt(i);
                    if (si >= digests.size()) {
                        builder.appendNull();
                        continue;
                    }
                    final TDigestState digest = digests.get(si);
                    if (digest != null && digest.size() > 0) {
                        builder.appendDouble(InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation(digest));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        Block evaluatePercentile(IntVector selected, DriverContext driverContext) {
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int si = selected.getInt(i);
                    if (si >= digests.size()) {
                        builder.appendNull();
                        continue;
                    }
                    final TDigestState digest = digests.get(si);
                    if (percentile != null && digest != null && digest.size() > 0) {
                        builder.appendDouble(digest.quantile(percentile / 100));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            Releasables.close(Releasables.wrap(LongStream.range(0, digests.size()).mapToObj(i -> digests.get(i)).toList()), digests);
        }
    }
}
