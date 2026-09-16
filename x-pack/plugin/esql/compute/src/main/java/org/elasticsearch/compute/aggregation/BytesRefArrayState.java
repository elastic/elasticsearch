/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

import static org.elasticsearch.common.util.PartitionedHashTable.NUM_PARTITIONS;
import static org.elasticsearch.common.util.PartitionedHashTable.PARTITION_WRITE_BATCH;

/**
 * Aggregator state for an array of BytesRefs. It is created in a mode where it
 * won't track the {@code groupId}s that are sent to it and it is the
 * responsibility of the caller to only fetch values for {@code groupId}s
 * that it has sent using the {@code selected} parameter when building the
 * results. This is fine when there are no {@code null} values in the input
 * data. But once there are null values in the input data it is
 * <strong>much</strong> more convenient to only send non-null values and
 * the tracking built into the grouping code can't track that. In that case
 * call {@link #enableGroupIdTracking} to transition the state into a mode
 * where it'll track which {@code groupIds} have been written.
 * <p>
 *     This class is a specialized version of the {@code X-ArrayState.java.st} template.
 * </p>
 */
public final class BytesRefArrayState implements GroupingAggregatorState, Releasable {
    /**
     * Total bytes per partition above which we fall back to the sparse (per-value BytesRef) layout.
     * Matches {@code BytesRefSwissHash.PAGED_PARTITION_THRESHOLD_BYTES}.
     */
    static final long PAGED_PARTITION_THRESHOLD_BYTES = 400L * 1024 * 1024;

    private final BigArrays bigArrays;
    private final CircuitBreaker breaker;
    private final String breakerLabel;
    private ObjectArray<BreakingBytesRefBuilder> values;
    private long totalValueBytes;
    private int totalValueCount;
    /**
     * If false, no group id is expected to have nulls.
     * If true, they may have nulls.
     */
    private boolean groupIdTrackingEnabled;

    BytesRefArrayState(BigArrays bigArrays, CircuitBreaker breaker, String breakerLabel) {
        this.bigArrays = bigArrays;
        this.breaker = breaker;
        this.breakerLabel = breakerLabel;
        this.values = bigArrays.newObjectArray(0);
    }

    BytesRef get(int groupId) {
        return values.get(groupId).bytesRefView();
    }

    void set(int groupId, BytesRef value) {
        ensureCapacity(groupId + 1);

        var currentBuilder = values.get(groupId);
        if (currentBuilder == null) {
            currentBuilder = new BreakingBytesRefBuilder(breaker, breakerLabel, value.length);
            values.set(groupId, currentBuilder);
            totalValueCount++;
        } else {
            totalValueBytes -= currentBuilder.length();
        }

        currentBuilder.copyBytes(value);
        totalValueBytes += value.length;
    }

    Block toValuesBlock(IntVector selected, DriverContext driverContext) {
        if (false == groupIdTrackingEnabled) {
            try (var builder = driverContext.blockFactory().newBytesRefVectorBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    var value = get(group);
                    builder.appendBytesRef(value);
                }
                return builder.build().asBlock();
            }
        }
        try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    var value = get(group);
                    builder.appendBytesRef(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    void ensureCapacity(int minSize) {
        if (minSize > values.size()) {
            values = bigArrays.grow(values, minSize);
        }
    }

    /** Extracts an intermediate view of the contents of this state.  */
    public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        assert blocks.length >= offset + 2;
        try (
            var valuesBuilder = driverContext.blockFactory().newBytesRefVectorBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            var emptyBytesRef = new BytesRef();
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    var value = get(group);
                    valuesBuilder.appendBytesRef(value);
                } else {
                    valuesBuilder.appendBytesRef(emptyBytesRef); // TODO can we just use null?
                }
                hasValueBuilder.appendBoolean(i, hasValue(group));
            }
            blocks[offset] = valuesBuilder.build().asBlock();
            blocks[offset + 1] = hasValueBuilder.build().asBlock();
        }
    }

    boolean hasValue(int groupId) {
        return groupId < values.size() && values.get(groupId) != null;
    }

    /**
     * Switches this array state into tracking which group ids are set. This is
     * idempotent and fast if already tracking so it's safe to, say, call it once
     * for every block of values that arrives containing {@code null}.
     *
     * <p>
     *     This class tracks seen group IDs differently from {@code AbstractArrayState}, as it just
     *     stores a flag to know if optimizations can be made.
     * </p>
     */
    @Override
    public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
        this.groupIdTrackingEnabled = true;
    }

    @Override
    public void close() {
        for (int i = 0; i < values.size(); i++) {
            Releasables.closeWhileHandlingException(values.get(i));
        }

        Releasables.close(values);
    }

    private static long bytesUsedByPointerPage(int length) {
        return RamUsageEstimator.alignObjectSize(
            (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * length
        );
    }

    private static long bytesUsedBySeenPage(int length) {
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + length);
    }

    private static long bytesUsedByIntPage(int length) {
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * Integer.BYTES);
    }

    private static long bytesUsedByValue(BytesRef value) {
        return RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) + RamUsageEstimator.alignObjectSize(
            (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + value.length
        );
    }

    /**
     * Dense partition state: one flat {@code byte[]} buffer per partition with an {@code int[]} offset array (end-offset encoding).
     * Used when total value bytes across all partitions is at most {@link #PAGED_PARTITION_THRESHOLD_BYTES}.
     */
    private static final class DenseBytesRefPartitionedState implements GroupingAggregatorFunction.PartitionedState {
        private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOf(DenseBytesRefPartitionedState.class);

        private final long baseBytes;
        private byte[][] partitionData;
        private final int[] partitionDataUsed;
        private int[][] partitionOffsets;
        private final int[] partitionCounts;
        private boolean[][] seen;

        DenseBytesRefPartitionedState(CircuitBreaker breaker, int avgKeysPerPartition, int avgBytesPerPartition, boolean trackSeen) {
            final int initialBytes = ArrayUtil.oversize(Math.max(avgBytesPerPartition, 1), 1);
            final int initialOffsets = ArrayUtil.oversize(Math.max(avgKeysPerPartition + 1, 2), Integer.BYTES);
            baseBytes = BASE_RAM_USAGE + bytesUsedByIntPage(NUM_PARTITIONS)        // partitionDataUsed
                + bytesUsedByIntPage(NUM_PARTITIONS)        // partitionCounts
                + bytesUsedByPointerPage(NUM_PARTITIONS)    // partitionData outer ref[]
                + bytesUsedByPointerPage(NUM_PARTITIONS)    // partitionOffsets outer ref[]
                + (trackSeen ? bytesUsedByPointerPage(NUM_PARTITIONS) : 0);  // seen outer ref[]
            long perPartitionBytes = (long) NUM_PARTITIONS * initialBytes + (long) NUM_PARTITIONS * bytesUsedByIntPage(initialOffsets);
            breaker.addEstimateBytesAndMaybeBreak(baseBytes + perPartitionBytes, BytesRefPartitionedState.LABEL);
            partitionDataUsed = new int[NUM_PARTITIONS];
            partitionCounts = new int[NUM_PARTITIONS];
            partitionData = new byte[NUM_PARTITIONS][];
            partitionOffsets = new int[NUM_PARTITIONS][];
            seen = trackSeen ? new boolean[NUM_PARTITIONS][] : null;
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                partitionData[p] = new byte[initialBytes];
                partitionOffsets[p] = new int[initialOffsets];
            }
        }

        @Override
        public boolean hasAllValues(int partition) {
            return seen == null;
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            long bytes = 0;
            if (partitionData[partition] != null) {
                bytes += partitionData[partition].length;
                partitionData[partition] = null;
            }
            if (partitionOffsets != null && partitionOffsets[partition] != null) {
                bytes += bytesUsedByIntPage(partitionOffsets[partition].length);
                partitionOffsets[partition] = null;
            }
            if (seen != null && seen[partition] != null) {
                bytes += bytesUsedBySeenPage(seen[partition].length);
                seen[partition] = null;
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
            if (seen != null) {
                for (int p = 0; p < NUM_PARTITIONS; p++) {
                    if (seen[p] != null) {
                        bytes += bytesUsedBySeenPage(seen[p].length);
                        seen[p] = null;
                    }
                }
                seen = null;
            }
            breaker.addWithoutBreaking(-bytes);
        }
    }

    private static final class BytesRefPartitionedState implements GroupingAggregatorFunction.PartitionedState {
        private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOf(BytesRefPartitionedState.class);
        static final String LABEL = "BytesRefArrayState#partition";

        private final long baseBytes;
        private final BytesRef[][] values;
        private final boolean[][] seen;

        BytesRefPartitionedState(CircuitBreaker breaker, int partitionSize, boolean trackSeen) {
            long pageBytes = bytesUsedByPointerPage(partitionSize) + (trackSeen ? bytesUsedBySeenPage(partitionSize) : 0);
            baseBytes = BASE_RAM_USAGE + bytesUsedByPointerPage(NUM_PARTITIONS) + (trackSeen ? bytesUsedByPointerPage(NUM_PARTITIONS) : 0);
            breaker.addEstimateBytesAndMaybeBreak(baseBytes + NUM_PARTITIONS * pageBytes, LABEL);
            values = new BytesRef[NUM_PARTITIONS][partitionSize];
            seen = trackSeen ? new boolean[NUM_PARTITIONS][partitionSize] : null;
        }

        @Override
        public boolean hasAllValues(int partition) {
            return seen == null;
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
            if (seen != null && seen[partition] != null) {
                usedBytes += bytesUsedBySeenPage(seen[partition].length);
                seen[partition] = null;
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
                if (seen != null && seen[p] != null) {
                    usedBytes += bytesUsedBySeenPage(seen[p].length);
                    seen[p] = null;
                }
            }
            breaker.addWithoutBreaking(-usedBytes);
        }
    }

    private final class BytesRefPartitionSplitter implements GroupingAggregatorFunction.PartitionSplitter {
        private final CircuitBreaker partitionBreaker;
        private DenseBytesRefPartitionedState denseState;
        private BytesRefPartitionedState sparseState;

        BytesRefPartitionSplitter(CircuitBreaker partitionBreaker) {
            this.partitionBreaker = partitionBreaker;
            final int avgKeysPerPartition = Math.max(Math.ceilDiv(totalValueCount, NUM_PARTITIONS), 1);
            final int avgBytesPerPartition = (int) Math.ceilDiv(Math.max(totalValueBytes, 1L), NUM_PARTITIONS);
            if (totalValueBytes <= PAGED_PARTITION_THRESHOLD_BYTES) {
                denseState = new DenseBytesRefPartitionedState(
                    partitionBreaker,
                    avgKeysPerPartition,
                    avgBytesPerPartition,
                    groupIdTrackingEnabled
                );
                sparseState = null;
            } else {
                sparseState = new BytesRefPartitionedState(
                    partitionBreaker,
                    ArrayUtil.oversize(avgKeysPerPartition, Long.BYTES),
                    groupIdTrackingEnabled
                );
                denseState = null;
            }
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
            if (denseState != null) {
                splitDense(firstId, shiftedIds, batchPartitionCounts);
            } else {
                splitSparse(firstId, shiftedIds, batchPartitionCounts, partitionOffsets);
            }
        }

        private void splitDense(int firstId, short[] shiftedIds, int[] batchPartitionCounts) {
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int count = batchPartitionCounts[p];
                if (count == 0) {
                    continue;
                }
                final int base = p * PARTITION_WRITE_BATCH;
                final int keyBase = denseState.partitionCounts[p];
                ensureOffsetCapacity(p, keyBase + count + 1);
                if (denseState.seen != null) {
                    ensureSeenCapacity(p, keyBase + count);
                }
                for (int i = 0; i < count; i++) {
                    final int id = firstId + shiftedIds[base + i];
                    denseState.partitionOffsets[p][keyBase + i] = denseState.partitionDataUsed[p];
                    if (id < values.size()) {
                        BreakingBytesRefBuilder builder = values.get(id);
                        if (builder != null) {
                            final int valueLen = builder.length();
                            ensureDataCapacity(p, denseState.partitionDataUsed[p] + valueLen);
                            System.arraycopy(builder.bytes(), 0, denseState.partitionData[p], denseState.partitionDataUsed[p], valueLen);
                            denseState.partitionDataUsed[p] += valueLen;
                            if (denseState.seen != null) {
                                denseState.seen[p][keyBase + i] = true;
                            }
                        }
                    }
                }
                // sentinel: end offset of last entry = bytes written so far
                denseState.partitionOffsets[p][keyBase + count] = denseState.partitionDataUsed[p];
                denseState.partitionCounts[p] += count;
            }
        }

        private void splitSparse(int firstId, short[] shiftedIds, int[] batchPartitionCounts, int[] partitionOffsets) {
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
                    if (id < values.size()) {
                        BreakingBytesRefBuilder builder = values.get(id);
                        if (builder != null) {
                            BytesRef copy = BytesRef.deepCopyOf(builder.bytesRefView());
                            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByValue(copy), BytesRefPartitionedState.LABEL);
                            sparseState.values[p][offset + i] = copy;
                            if (sparseState.seen != null) {
                                sparseState.seen[p][offset + i] = true;
                            }
                        }
                    }
                }
            }
        }

        private void ensureDataCapacity(int p, int minLength) {
            final byte[] sub = denseState.partitionData[p];
            if (sub.length >= minLength) {
                return;
            }
            final int newLength = ArrayUtil.oversize(minLength, 1);
            partitionBreaker.addEstimateBytesAndMaybeBreak(newLength, BytesRefPartitionedState.LABEL);
            denseState.partitionData[p] = Arrays.copyOf(sub, newLength);
            partitionBreaker.addWithoutBreaking(-sub.length);
        }

        private void ensureOffsetCapacity(int p, int minCount) {
            final int[] sub = denseState.partitionOffsets[p];
            if (sub.length >= minCount) {
                return;
            }
            final int newCount = ArrayUtil.oversize(minCount, Integer.BYTES);
            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByIntPage(newCount), BytesRefPartitionedState.LABEL);
            denseState.partitionOffsets[p] = Arrays.copyOf(sub, newCount);
            partitionBreaker.addWithoutBreaking(-bytesUsedByIntPage(sub.length));
        }

        private void ensureSeenCapacity(int p, int minCount) {
            if (denseState.seen[p] == null) {
                final int newSize = ArrayUtil.oversize(minCount, 1);
                partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), BytesRefPartitionedState.LABEL);
                denseState.seen[p] = new boolean[newSize];
                return;
            }
            final boolean[] sub = denseState.seen[p];
            if (sub.length >= minCount) {
                return;
            }
            final int newSize = ArrayUtil.oversize(minCount, 1);
            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), BytesRefPartitionedState.LABEL);
            denseState.seen[p] = Arrays.copyOf(sub, newSize);
            partitionBreaker.addWithoutBreaking(-bytesUsedBySeenPage(sub.length));
        }

        private void ensurePartitionCapacity(int partition, int minSize) {
            BytesRef[] oldValues = sparseState.values[partition];
            if (oldValues.length >= minSize) {
                return;
            }
            int newSize = ArrayUtil.oversize(minSize, Long.BYTES);
            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByPointerPage(newSize), BytesRefPartitionedState.LABEL);
            sparseState.values[partition] = Arrays.copyOf(oldValues, newSize);
            partitionBreaker.addWithoutBreaking(-bytesUsedByPointerPage(oldValues.length));
            if (sparseState.seen != null) {
                boolean[] oldSeen = sparseState.seen[partition];
                partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), BytesRefPartitionedState.LABEL);
                sparseState.seen[partition] = Arrays.copyOf(oldSeen, newSize);
                partitionBreaker.addWithoutBreaking(-bytesUsedBySeenPage(oldSeen.length));
            }
        }

        @Override
        public GroupingAggregatorFunction.PartitionedState finish() {
            if (denseState != null) {
                GroupingAggregatorFunction.PartitionedState result = denseState;
                denseState = null;
                return result;
            } else {
                GroupingAggregatorFunction.PartitionedState result = sparseState;
                sparseState = null;
                return result;
            }
        }

        @Override
        public void release(CircuitBreaker breaker) {
            if (denseState != null) {
                denseState.releaseAll(breaker);
                denseState = null;
            }
            if (sparseState != null) {
                sparseState.releaseAll(breaker);
                sparseState = null;
            }
        }
    }

    GroupingAggregatorFunction.PartitionSplitter createPartitioningSplitter(CircuitBreaker breaker) {
        return new BytesRefPartitionSplitter(breaker);
    }

    BytesRef[] partitionValues(GroupingAggregatorFunction.PartitionedState source, int partition) {
        if (source instanceof DenseBytesRefPartitionedState dense) {
            final int count = dense.partitionCounts[partition];
            final BytesRef[] result = new BytesRef[count];
            for (int i = 0; i < count; i++) {
                final int start = dense.partitionOffsets[partition][i];
                final int end = dense.partitionOffsets[partition][i + 1];
                result[i] = new BytesRef(dense.partitionData[partition], start, end - start);
            }
            return result;
        }
        return ((BytesRefPartitionedState) source).values[partition];
    }

    boolean[] partitionSeen(GroupingAggregatorFunction.PartitionedState source, int partition) {
        if (source instanceof DenseBytesRefPartitionedState dense) {
            return dense.seen == null ? null : dense.seen[partition];
        }
        boolean[][] seen = ((BytesRefPartitionedState) source).seen;
        return seen == null ? null : seen[partition];
    }

    void appendPartition(BytesRef[] src, int firstId, int length) {
        for (int i = 0; i < length; i++) {
            if (src[i] != null) {
                set(firstId + i, src[i]);
            }
        }
    }
}
