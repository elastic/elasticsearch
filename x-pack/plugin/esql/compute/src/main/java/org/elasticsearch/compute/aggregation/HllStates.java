/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.hppc.BitMixer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

final class HllStates {
    private HllStates() {}

    static BytesStreamOutput serializeHLL(int groupId, HyperLogLogPlusPlus hll) {
        BytesStreamOutput out = new BytesStreamOutput();
        try {
            hll.writeTo(groupId, out);
            return out;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Copies the content of the BytesReference to an array of bytes. The byte[] must
     * have enough space to fit the bytesReference object, otherwise an
     * {@link ArrayIndexOutOfBoundsException} will be thrown.
     *
     * @return number of bytes copied
     */
    static int copyToArray(BytesReference bytesReference, byte[] arr, int offset) {
        int origOffset = offset;
        final BytesRefIterator iterator = bytesReference.iterator();
        try {
            BytesRef slice;
            while ((slice = iterator.next()) != null) {
                System.arraycopy(slice.bytes, slice.offset, arr, offset, slice.length);
                offset += slice.length;
            }
            return offset - origOffset;
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    static class SingleState implements AggregatorState<SingleState> {

        private static final int SINGLE_BUCKET_ORD = 0;
        private final SingleStateSerializer serializer;
        final HyperLogLogPlusPlus hll;
        private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        SingleState(BigArrays bigArrays, int precision) {
            this.serializer = new SingleStateSerializer();
            this.hll = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.precisionFromThreshold(precision), bigArrays, 1);
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

        @Override
        public long getEstimatedSize() {
            return serializeHLL(SINGLE_BUCKET_ORD, hll).size();
        }

        @Override
        public void close() {
            Releasables.close(hll);
        }

        @Override
        public AggregatorStateSerializer<SingleState> serializer() {
            return serializer;
        }
    }

    static class SingleStateSerializer implements AggregatorStateSerializer<SingleState> {
        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int serialize(SingleState state, byte[] ba, int offset, IntVector selected) {
            assert selected.getPositionCount() == 1;
            assert selected.getInt(0) == 0;

            int groupId = selected.getInt(0);
            BytesReference r = serializeHLL(groupId, state.hll).bytes();
            int len = copyToArray(r, ba, offset);
            assert len == r.length() : "Failed to serialize HLL state";
            return len; // number of bytes written
        }

        @Override
        public void deserialize(SingleState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            ByteArrayStreamInput in = new ByteArrayStreamInput();
            AbstractHyperLogLogPlusPlus hll = null;
            try {
                in.reset(ba, offset, ba.length - offset);
                hll = HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
                state.merge(SingleState.SINGLE_BUCKET_ORD, hll, SingleState.SINGLE_BUCKET_ORD);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                Releasables.close(hll);
            }
        }
    }

    static class GroupingState implements AggregatorState<GroupingState> {

        private final GroupingStateSerializer serializer;
        private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        final HyperLogLogPlusPlus hll;

        GroupingState(BigArrays bigArrays, int precision) {
            this.serializer = new GroupingStateSerializer();
            this.hll = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.precisionFromThreshold(precision), bigArrays, 1);
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

        void putNull(int groupId) {
            // no-op
        }

        void merge(int groupId, AbstractHyperLogLogPlusPlus other, int otherGroup) {
            hll.merge(groupId, other, otherGroup);
        }

        @Override
        public long getEstimatedSize() {
            int len = Integer.BYTES; // Serialize number of groups
            for (int groupId = 0; groupId < hll.maxOrd(); groupId++) {
                len += Integer.BYTES; // Serialize length of hll byte array
                // Serialize hll byte array. Unfortunately, the hll data structure
                // is not fixed length, so we must serialize it and then get its length
                len += serializeHLL(groupId, hll).size();
            }
            return len;
        }

        @Override
        public AggregatorStateSerializer<GroupingState> serializer() {
            return serializer;
        }

        @Override
        public void close() {
            Releasables.close(hll);
        }
    }

    static class GroupingStateSerializer implements AggregatorStateSerializer<GroupingState> {

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(GroupingState state, byte[] ba, int offset, IntVector selected) {
            final int origOffset = offset;
            intHandle.set(ba, offset, selected.getPositionCount());
            offset += Integer.BYTES;
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                BytesReference r = serializeHLL(groupId, state.hll).bytes();
                int len = r.length();
                intHandle.set(ba, offset, len);
                offset += Integer.BYTES;

                copyToArray(r, ba, offset);
                assert len == r.length() : "Failed to serialize HLL state";
                offset += len;
            }
            return offset - origOffset;
        }

        @Override
        public void deserialize(GroupingState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positionCount = (int) intHandle.get(ba, offset);
            offset += Integer.BYTES;
            ByteArrayStreamInput in = new ByteArrayStreamInput();
            AbstractHyperLogLogPlusPlus hll = null;
            try {
                for (int i = 0; i < positionCount; i++) {
                    int len = (int) intHandle.get(ba, offset);
                    offset += Integer.BYTES;
                    in.reset(ba, offset, len);
                    offset += len;
                    hll = HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
                    state.merge(i, hll, 0);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                Releasables.close(hll);
            }
        }
    }
}
