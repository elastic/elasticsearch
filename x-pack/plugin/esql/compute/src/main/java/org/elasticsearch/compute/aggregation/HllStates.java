/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesRefScratch;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.io.IOException;

final class HllStates {
    private HllStates() {}

    static BytesRef serializeHLL(int groupId, HyperLogLogPlusPlus hll, BytesStreamOutput scratch, BytesRefScratch scratchBytes) {
        scratch.seek(0);
        try {
            hll.writeTo(groupId, scratch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // BytesRefScratch reuses a single BytesRef and buffer; callers must consume immediately.
        // If callers ever need to retain the data, use BytesRefScratch.copy.
        return scratchBytes.wrap(scratch.bytes());
    }

    static void mergeSerialized(HyperLogLogPlusPlus hll, int groupId, BytesRef bytesRef, ByteArrayStreamInput scratchInput) {
        scratchInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        try {
            hll.mergeSerialized(groupId, scratchInput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class SingleState implements AggregatorState {

        private static final int SINGLE_BUCKET_ORD = 0;
        final HyperLogLogPlusPlus hll;
        private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        private final BytesStreamOutput scratch;
        private final ByteArrayStreamInput scratchInput;
        private final BytesRefScratch scratchBytes = new BytesRefScratch();

        SingleState(BigArrays bigArrays, int precision) {
            this.hll = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.precisionFromThreshold(precision), bigArrays, 1);
            this.scratch = new BytesStreamOutput();
            this.scratchInput = new ByteArrayStreamInput(new byte[0]);
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

        void merge(int groupId, AbstractHyperLogLogPlusPlus other, int otherGroup) {
            hll.merge(groupId, other, otherGroup);
        }

        void merge(int groupId, BytesRef other, int otherGroup) {
            assert otherGroup == 0;
            mergeSerialized(hll, groupId, other, scratchInput);
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            BytesRef serialized = serializeHLL(SINGLE_BUCKET_ORD, hll, scratch, scratchBytes);
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(serialized, 1);
        }

        @Override
        public void close() {
            Releasables.close(hll);
        }
    }

    static class GroupingState implements GroupingAggregatorState {

        private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        final HyperLogLogPlusPlus hll;
        private final BytesStreamOutput scratch;
        private final ByteArrayStreamInput scratchInput;
        private final BytesRefScratch scratchBytes = new BytesRefScratch();

        GroupingState(BigArrays bigArrays, int precision) {
            this.hll = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.precisionFromThreshold(precision), bigArrays, 1);
            this.scratch = new BytesStreamOutput();
            this.scratchInput = new ByteArrayStreamInput(new byte[0]);
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
            assert otherGroup == 0;
            mergeSerialized(hll, groupId, other, scratchInput);
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    builder.appendBytesRef(serializeHLL(group, hll, scratch, scratchBytes));
                }
                blocks[offset] = builder.build();
            }
        }

        @Override
        public void close() {
            Releasables.close(hll);
        }
    }
}
