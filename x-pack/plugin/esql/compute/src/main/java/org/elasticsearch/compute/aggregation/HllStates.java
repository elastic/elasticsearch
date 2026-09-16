/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.BytesRefStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import static org.elasticsearch.common.util.PartitionedHashTable.NUM_PARTITIONS;
import static org.elasticsearch.common.util.PartitionedHashTable.PARTITION_WRITE_BATCH;

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

        void ensureCapacity(int groupCount) {
            // HLL auto-grows as groups are collected; no explicit pre-allocation needed
        }

        GroupingAggregatorFunction.PartitionSplitter createPartitioningSplitter(CircuitBreaker breaker) {
            return new HllPartitionSplitter(breaker);
        }

        BytesRef[] partitionValues(GroupingAggregatorFunction.PartitionedState source, int partition) {
            return ((HllPartitionedState) source).values[partition];
        }

        boolean[] partitionSeen(GroupingAggregatorFunction.PartitionedState source, int partition) {
            return null;
        }

        void appendPartition(BytesRef[] src, int firstId, int length) {
            for (int i = 0; i < length; i++) {
                if (src[i] != null) {
                    merge(firstId + i, src[i], 0);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(hll);
        }

        private static long bytesUsedByPointerPage(int length) {
            return RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * length
            );
        }

        private static long bytesUsedByValue(BytesRef value) {
            return RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + value.length
            );
        }

        private static final class HllPartitionedState implements GroupingAggregatorFunction.PartitionedState {
            private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOf(HllPartitionedState.class);
            static final String LABEL = "HllStates#partition";

            private final long baseBytes;
            private final BytesRef[][] values;

            HllPartitionedState(CircuitBreaker breaker, int partitionSize) {
                baseBytes = BASE_RAM_USAGE + bytesUsedByPointerPage(NUM_PARTITIONS);
                long pageBytes = bytesUsedByPointerPage(partitionSize);
                breaker.addEstimateBytesAndMaybeBreak(baseBytes + NUM_PARTITIONS * pageBytes, LABEL);
                values = new BytesRef[NUM_PARTITIONS][partitionSize];
            }

            @Override
            public boolean hasAllValues(int partition) {
                return true;
            }

            @Override
            public void releasePartition(CircuitBreaker breaker, int partition) {
                long usedBytes = 0;
                if (values[partition] != null) {
                    for (BytesRef v : values[partition]) {
                        if (v != null) {
                            usedBytes += bytesUsedByValue(v);
                        }
                    }
                    usedBytes += bytesUsedByPointerPage(values[partition].length);
                    values[partition] = null;
                }
                breaker.addWithoutBreaking(-usedBytes);
            }

            @Override
            public void releaseAll(CircuitBreaker breaker) {
                long usedBytes = baseBytes;
                for (int p = 0; p < NUM_PARTITIONS; p++) {
                    if (values[p] != null) {
                        for (BytesRef v : values[p]) {
                            if (v != null) {
                                usedBytes += bytesUsedByValue(v);
                            }
                        }
                        usedBytes += bytesUsedByPointerPage(values[p].length);
                        values[p] = null;
                    }
                }
                breaker.addWithoutBreaking(-usedBytes);
            }
        }

        private final class HllPartitionSplitter implements GroupingAggregatorFunction.PartitionSplitter {
            private final CircuitBreaker partitionBreaker;
            private HllPartitionedState partitionedState;

            HllPartitionSplitter(CircuitBreaker partitionBreaker) {
                this.partitionBreaker = partitionBreaker;
                int partitionSize = ArrayUtil.oversize(PARTITION_WRITE_BATCH, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                partitionedState = new HllPartitionedState(partitionBreaker, partitionSize);
            }

            @Override
            public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
                BytesRefStreamOutput out = new BytesRefStreamOutput();
                try {
                    for (int p = 0; p < NUM_PARTITIONS; p++) {
                        final int count = batchPartitionCounts[p];
                        if (count == 0) {
                            continue;
                        }
                        final int offset = partitionOffsets[p];
                        ensurePartitionCapacity(p, offset + count);
                        final int base = p * PARTITION_WRITE_BATCH;
                        for (int i = 0; i < count; i++) {
                            final int id = firstId + shiftedIds[base + i];
                            hll.writeTo(id, out);
                            BytesRef copy = BytesRef.deepCopyOf(out.get());
                            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByValue(copy), HllPartitionedState.LABEL);
                            partitionedState.values[p][offset + i] = copy;
                            out.reset();
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private void ensurePartitionCapacity(int partition, int minSize) {
                BytesRef[] oldValues = partitionedState.values[partition];
                if (oldValues.length >= minSize) {
                    return;
                }
                int newSize = ArrayUtil.oversize(minSize, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByPointerPage(newSize), HllPartitionedState.LABEL);
                partitionedState.values[partition] = Arrays.copyOf(oldValues, newSize);
                partitionBreaker.addWithoutBreaking(-bytesUsedByPointerPage(oldValues.length));
            }

            @Override
            public HllPartitionedState finish() {
                HllPartitionedState result = partitionedState;
                partitionedState = null;
                return result;
            }

            @Override
            public void release(CircuitBreaker breaker) {
                if (partitionedState != null) {
                    partitionedState.releaseAll(breaker);
                    partitionedState = null;
                }
            }
        }
    }
}
