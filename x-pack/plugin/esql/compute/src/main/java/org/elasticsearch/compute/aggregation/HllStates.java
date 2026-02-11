/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.BytesRefStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.io.IOException;
import java.io.UncheckedIOException;

final class HllStates {
    private HllStates() {}

    static class SingleState implements AggregatorState {

        private static final int SINGLE_BUCKET_ORD = 0;
        final HyperLogLogPlusPlus hll;
        private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        SingleState(DriverContext driverContext, int precision) {
            this.hll = new HyperLogLogPlusPlus(
                HyperLogLogPlusPlus.precisionFromThreshold(precision),
                driverContext.bigArrays(),
                driverContext.breaker(),
                1
            );
        }

        void collect(long v) {
            doCollect(BitMixer.mix64(v));
        }

        void collect(int v) {
            doCollect(BitMixer.mix64(v));
        }

        void collect(double v) {
            doCollect(BitMixer.mix64(Double.doubleToLongBits(v)));
        }

        void collect(BytesRef bytes) {
            MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
            collect(hash.h1);
        }

        private void doCollect(long hash) {
            hll.collect(SINGLE_BUCKET_ORD, hash);
        }

        long cardinality() {
            return hll.cardinality(SINGLE_BUCKET_ORD);
        }

        // TODO: bulk and reuse buffer
        void merge(int groupId, BytesRef other, int otherGroup) {
            try {
                hll.combine(groupId, other);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            BytesRefStreamOutput out = new BytesRefStreamOutput();
            try {
                hll.writeTo(SINGLE_BUCKET_ORD, out);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(out.get(), 1);
        }

        @Override
        public void close() {
            Releasables.close(hll);
        }
    }

    static class GroupingState implements GroupingAggregatorState {

        private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        final HyperLogLogPlusPlus hll;

        GroupingState(DriverContext driverContext, int precision) {
            this.hll = new HyperLogLogPlusPlus(
                HyperLogLogPlusPlus.precisionFromThreshold(precision),
                driverContext.bigArrays(),
                driverContext.breaker(),
                1
            );
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // Nothing to do
        }

        void collect(int groupId, long v) {
            doCollect(groupId, BitMixer.mix64(v));
        }

        void collect(int groupId, int v) {
            doCollect(groupId, BitMixer.mix64(v));
        }

        void collect(int groupId, double v) {
            doCollect(groupId, BitMixer.mix64(Double.doubleToLongBits(v)));
        }

        void collect(int groupId, BytesRef bytes) {
            MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
            collect(groupId, hash.h1);
        }

        private void doCollect(int groupId, long hash) {
            hll.collect(groupId, hash);
        }

        long cardinality(int groupId) {
            return hll.cardinality(groupId);
        }

        void merge(int groupId, BytesRef other, int otherGroup) {
            try {
                hll.combine(groupId, other);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                BytesRefStreamOutput out = new BytesRefStreamOutput();
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    hll.writeTo(group, out);
                    builder.appendBytesRef(out.get());
                    out.reset();
                }
                blocks[offset] = builder.build();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() {
            Releasables.close(hll);
        }
    }
}
