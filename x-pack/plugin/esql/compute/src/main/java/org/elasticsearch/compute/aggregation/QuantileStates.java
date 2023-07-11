/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantBytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

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

    static TDigestState deserializeDigest(BytesRef bytesRef) {
        ByteArrayStreamInput in = new ByteArrayStreamInput(bytesRef.bytes);
        in.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        try {
            return TDigestState.read(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class SingleState implements AggregatorState {
        private TDigestState digest;
        private final Double percentile;

        SingleState(double percentile) {
            this.digest = TDigestState.create(DEFAULT_COMPRESSION);
            this.percentile = percentileParam(percentile);
        }

        @Override
        public void close() {}

        void add(double v) {
            digest.add(v);
        }

        void add(SingleState other) {
            digest.add(other.digest);
        }

        void add(BytesRef other) {
            digest.add(deserializeDigest(other));
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset) {
            assert blocks.length >= offset + 1;
            blocks[offset] = new ConstantBytesRefVector(serializeDigest(this.digest), 1).asBlock();
        }

        Block evaluateMedianAbsoluteDeviation() {
            assert percentile == MEDIAN : "Median must be 50th percentile [percentile = " + percentile + "]";
            if (digest.size() == 0) {
                return Block.constantNullBlock(1);
            }
            double result = InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation(digest);
            return DoubleBlock.newConstantBlockWith(result, 1);
        }

        Block evaluatePercentile() {
            if (percentile == null) {
                return DoubleBlock.newBlockBuilder(1).appendNull().build();
            }
            if (digest.size() == 0) {
                return Block.constantNullBlock(1);
            }
            double result = digest.quantile(percentile / 100);
            return DoubleBlock.newConstantBlockWith(result, 1);
        }
    }

    static class GroupingState implements GroupingAggregatorState {
        private long largestGroupId = -1;
        private ObjectArray<TDigestState> digests;
        private final BigArrays bigArrays;
        private final Double percentile;

        GroupingState(BigArrays bigArrays, double percentile) {
            this.bigArrays = bigArrays;
            this.digests = bigArrays.newObjectArray(1);
            this.percentile = percentileParam(percentile);
        }

        private TDigestState getOrAddGroup(int groupId) {
            if (groupId > largestGroupId) {
                digests = bigArrays.grow(digests, groupId + 1);
                largestGroupId = groupId;
            }
            TDigestState qs = digests.get(groupId);
            if (qs == null) {
                qs = TDigestState.create(DEFAULT_COMPRESSION);
                digests.set(groupId, qs);
            }
            return qs;
        }

        void putNull(int groupId) {
            getOrAddGroup(groupId);
        }

        void add(int groupId, double v) {
            getOrAddGroup(groupId).add(v);
        }

        void add(int groupId, TDigestState other) {
            getOrAddGroup(groupId).add(other);
        }

        void add(int groupId, BytesRef other) {
            getOrAddGroup(groupId).add(deserializeDigest(other));
        }

        TDigestState get(int position) {
            return digests.get(position);
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected) {
            assert blocks.length >= offset + 1;
            var builder = BytesRefBlock.newBlockBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                builder.appendBytesRef(serializeDigest(get(group)));
            }
            blocks[offset] = builder.build();
        }

        Block evaluateMedianAbsoluteDeviation(IntVector selected) {
            assert percentile == MEDIAN : "Median must be 50th percentile [percentile = " + percentile + "]";
            final DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                final TDigestState digest = digests.get(selected.getInt(i));
                if (digest != null && digest.size() > 0) {
                    builder.appendDouble(InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation(digest));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }

        Block evaluatePercentile(IntVector selected) {
            final DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                final TDigestState digest = digests.get(selected.getInt(i));
                if (percentile != null && digest != null && digest.size() > 0) {
                    builder.appendDouble(digest.quantile(percentile / 100));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }

        @Override
        public void close() {
            digests.close();
        }
    }
}
