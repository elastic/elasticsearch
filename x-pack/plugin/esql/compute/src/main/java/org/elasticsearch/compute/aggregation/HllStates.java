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

        BytesRefSequence partitionValues(GroupingAggregatorFunction.PartitionedState source, int partition) {
            HllPartitionedState state = (HllPartitionedState) source;
            return new BytesRefSequence.Flat(state.partitionData[partition], state.partitionOffsets[partition], state.partitionCounts[partition]);
        }

        boolean[] partitionSeen(GroupingAggregatorFunction.PartitionedState source, int partition) {
            return null;
        }

        void appendPartition(BytesRefSequence src, int firstId, int length) {
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < length; i++) {
                merge(firstId + i, src.get(i, scratch), 0);
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

        private static long bytesUsedByIntPage(int length) {
            return RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * Integer.BYTES
            );
        }

        private static final class HllPartitionedState implements GroupingAggregatorFunction.PartitionedState {
            private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOf(HllPartitionedState.class);
            static final String LABEL = "HllStates#partition";

            private final long baseBytes;
            private byte[][] partitionData;
            private int[][] partitionOffsets;
            private int[] partitionDataUsed;
            private int[] partitionCounts;

            HllPartitionedState(CircuitBreaker breaker, int initialKeysPerPartition, int initialBytesPerPartition) {
                baseBytes = BASE_RAM_USAGE
                    + bytesUsedByPointerPage(NUM_PARTITIONS)   // partitionData outer ref[]
                    + bytesUsedByPointerPage(NUM_PARTITIONS)   // partitionOffsets outer ref[]
                    + bytesUsedByIntPage(NUM_PARTITIONS)       // partitionDataUsed
                    + bytesUsedByIntPage(NUM_PARTITIONS);      // partitionCounts
                final int initialOffsets = ArrayUtil.oversize(Math.max(initialKeysPerPartition + 1, 2), Integer.BYTES);
                final int initialBytes = ArrayUtil.oversize(Math.max(initialBytesPerPartition, 1), 1);
                long perPartitionBytes = (long) NUM_PARTITIONS * initialBytes
                    + (long) NUM_PARTITIONS * bytesUsedByIntPage(initialOffsets);
                breaker.addEstimateBytesAndMaybeBreak(baseBytes + perPartitionBytes, LABEL);
                partitionDataUsed = new int[NUM_PARTITIONS];
                partitionCounts = new int[NUM_PARTITIONS];
                partitionData = new byte[NUM_PARTITIONS][];
                partitionOffsets = new int[NUM_PARTITIONS][];
                for (int p = 0; p < NUM_PARTITIONS; p++) {
                    partitionData[p] = new byte[initialBytes];
                    partitionOffsets[p] = new int[initialOffsets];
                }
            }

            @Override
            public boolean hasAllValues(int partition) {
                return true;
            }

            @Override
            public void releasePartition(CircuitBreaker breaker, int partition) {
                long bytes = 0;
                if (partitionData[partition] != null) {
                    bytes += partitionData[partition].length;
                    partitionData[partition] = null;
                }
                if (partitionOffsets[partition] != null) {
                    bytes += bytesUsedByIntPage(partitionOffsets[partition].length);
                    partitionOffsets[partition] = null;
                }
                breaker.addWithoutBreaking(-bytes);
            }

            @Override
            public void releaseAll(CircuitBreaker breaker) {
                long bytes = baseBytes;
                if (partitionData != null) {
                    for (int p = 0; p < NUM_PARTITIONS; p++) {
                        if (partitionData[p] != null) {
                            bytes += partitionData[p].length;
                        }
                    }
                    partitionData = null;
                }
                if (partitionOffsets != null) {
                    for (int p = 0; p < NUM_PARTITIONS; p++) {
                        if (partitionOffsets[p] != null) {
                            bytes += bytesUsedByIntPage(partitionOffsets[p].length);
                        }
                    }
                    partitionOffsets = null;
                }
                breaker.addWithoutBreaking(-bytes);
            }
        }

        private final class HllPartitionSplitter implements GroupingAggregatorFunction.PartitionSplitter {
            private final CircuitBreaker partitionBreaker;
            private HllPartitionedState partitionedState;

            HllPartitionSplitter(CircuitBreaker partitionBreaker) {
                this.partitionBreaker = partitionBreaker;
                partitionedState = new HllPartitionedState(partitionBreaker, PARTITION_WRITE_BATCH, PARTITION_WRITE_BATCH * 512);
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
                        final int keyBase = partitionedState.partitionCounts[p];
                        final int base = p * PARTITION_WRITE_BATCH;
                        ensureOffsetCapacity(p, keyBase + count + 1);
                        for (int i = 0; i < count; i++) {
                            final int id = firstId + shiftedIds[base + i];
                            partitionedState.partitionOffsets[p][keyBase + i] = partitionedState.partitionDataUsed[p];
                            hll.writeTo(id, out);
                            BytesRef ref = out.get();
                            ensureDataCapacity(p, partitionedState.partitionDataUsed[p] + ref.length);
                            System.arraycopy(ref.bytes, ref.offset, partitionedState.partitionData[p], partitionedState.partitionDataUsed[p], ref.length);
                            partitionedState.partitionDataUsed[p] += ref.length;
                            out.reset();
                        }
                        partitionedState.partitionOffsets[p][keyBase + count] = partitionedState.partitionDataUsed[p];
                        partitionedState.partitionCounts[p] += count;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private void ensureDataCapacity(int p, int minLength) {
                final byte[] sub = partitionedState.partitionData[p];
                if (sub.length >= minLength) {
                    return;
                }
                final int newLength = ArrayUtil.oversize(minLength, 1);
                partitionBreaker.addEstimateBytesAndMaybeBreak(newLength, HllPartitionedState.LABEL);
                partitionedState.partitionData[p] = Arrays.copyOf(sub, newLength);
                partitionBreaker.addWithoutBreaking(-sub.length);
            }

            private void ensureOffsetCapacity(int p, int minCount) {
                final int[] sub = partitionedState.partitionOffsets[p];
                if (sub.length >= minCount) {
                    return;
                }
                final int newCount = ArrayUtil.oversize(minCount, Integer.BYTES);
                partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByIntPage(newCount), HllPartitionedState.LABEL);
                partitionedState.partitionOffsets[p] = Arrays.copyOf(sub, newCount);
                partitionBreaker.addWithoutBreaking(-bytesUsedByIntPage(sub.length));
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
