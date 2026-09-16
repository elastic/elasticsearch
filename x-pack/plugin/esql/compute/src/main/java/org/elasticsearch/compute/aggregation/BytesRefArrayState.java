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
import org.elasticsearch.common.util.BytesRefArray;
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
     * Total bytes per partition above which we fall back to the paged layout.
     * Matches {@code BytesRefSwissHash.PAGED_PARTITION_THRESHOLD_BYTES}.
     */
    static final long PAGED_PARTITION_THRESHOLD_BYTES = 400L * 1024 * 1024;

    static final String PARTITION_LABEL = "BytesRefArrayState#partition";

    private final BigArrays bigArrays;
    private final CircuitBreaker breaker;
    private final String breakerLabel;
    private final long pagedPartitionThresholdBytes;
    private ObjectArray<BreakingBytesRefBuilder> values;
    private long totalValueBytes;
    private int totalValueCount;
    /**
     * If false, no group id is expected to have nulls.
     * If true, they may have nulls.
     */
    private boolean groupIdTrackingEnabled;

    BytesRefArrayState(BigArrays bigArrays, CircuitBreaker breaker, String breakerLabel) {
        this(bigArrays, breaker, breakerLabel, PAGED_PARTITION_THRESHOLD_BYTES);
    }

    BytesRefArrayState(BigArrays bigArrays, CircuitBreaker breaker, String breakerLabel, long pagedPartitionThresholdBytes) {
        this.bigArrays = bigArrays;
        this.breaker = breaker;
        this.breakerLabel = breakerLabel;
        this.pagedPartitionThresholdBytes = pagedPartitionThresholdBytes;
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

    /**
     * Flat partition state: one contiguous {@code byte[]} buffer per partition with an
     * {@code int[]} end-offset array. Used when total value bytes is at most
     * {@link #PAGED_PARTITION_THRESHOLD_BYTES}. Matches the layout of
     * {@code BytesRefSwissHash.FlatBytesRefPartitionedHashKeys}.
     */
    private static final class FlatBytesRefPartitionedState implements GroupingAggregatorFunction.PartitionedState {
        private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOf(FlatBytesRefPartitionedState.class);

        private final long baseBytes;
        private byte[][] partitionData;
        private final int[] partitionDataUsed;
        private int[][] partitionOffsets;
        private final int[] partitionCounts;
        private boolean[][] seen;

        FlatBytesRefPartitionedState(CircuitBreaker breaker, int avgKeysPerPartition, int avgBytesPerPartition, boolean trackSeen) {
            final int initialBytes = ArrayUtil.oversize(Math.max(avgBytesPerPartition, 1), 1);
            final int initialOffsets = ArrayUtil.oversize(Math.max(avgKeysPerPartition + 1, 2), Integer.BYTES);
            baseBytes = BASE_RAM_USAGE + bytesUsedByIntPage(NUM_PARTITIONS)        // partitionDataUsed
                + bytesUsedByIntPage(NUM_PARTITIONS)        // partitionCounts
                + bytesUsedByPointerPage(NUM_PARTITIONS)    // partitionData outer ref[]
                + bytesUsedByPointerPage(NUM_PARTITIONS)    // partitionOffsets outer ref[]
                + (trackSeen ? bytesUsedByPointerPage(NUM_PARTITIONS) : 0);  // seen outer ref[]
            long perPartitionBytes = (long) NUM_PARTITIONS * initialBytes + (long) NUM_PARTITIONS * bytesUsedByIntPage(initialOffsets);
            breaker.addEstimateBytesAndMaybeBreak(baseBytes + perPartitionBytes, PARTITION_LABEL);
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

    /**
     * Paged partition state: one {@link BytesRefArray} per partition backed by {@link BigArrays}
     * paged storage. Used when total value bytes exceeds {@link #PAGED_PARTITION_THRESHOLD_BYTES}.
     * Matches the structure of {@code BytesRefSwissHash.PagedBytesRefPartitionedHashKeys}.
     */
    private static final class PagedBytesRefPartitionedState implements GroupingAggregatorFunction.PartitionedState {
        BytesRefArray[] partitionArrays;
        boolean[][] seen;

        PagedBytesRefPartitionedState(BigArrays bigArrays, int avgKeysPerPartition, int avgBytesPerPartition, boolean trackSeen) {
            partitionArrays = new BytesRefArray[NUM_PARTITIONS];
            boolean success = false;
            try {
                for (int p = 0; p < NUM_PARTITIONS; p++) {
                    partitionArrays[p] = new BytesRefArray(avgKeysPerPartition, bigArrays, avgBytesPerPartition);
                }
                seen = trackSeen ? new boolean[NUM_PARTITIONS][] : null;
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(partitionArrays);
                }
            }
        }

        @Override
        public boolean hasAllValues(int partition) {
            return seen == null;
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            if (partitionArrays[partition] != null) {
                partitionArrays[partition].close();
                partitionArrays[partition] = null;
            }
            if (seen != null && seen[partition] != null) {
                breaker.addWithoutBreaking(-bytesUsedBySeenPage(seen[partition].length));
                seen[partition] = null;
            }
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                if (partitionArrays[p] != null) {
                    partitionArrays[p].close();
                    partitionArrays[p] = null;
                }
                if (seen != null && seen[p] != null) {
                    breaker.addWithoutBreaking(-bytesUsedBySeenPage(seen[p].length));
                    seen[p] = null;
                }
            }
        }
    }

    private final class BytesRefPartitionSplitter implements GroupingAggregatorFunction.PartitionSplitter {
        private final CircuitBreaker partitionBreaker;
        private FlatBytesRefPartitionedState flatState;
        private PagedBytesRefPartitionedState pagedState;

        BytesRefPartitionSplitter(CircuitBreaker partitionBreaker) {
            this.partitionBreaker = partitionBreaker;
            final int avgKeysPerPartition = Math.max(Math.ceilDiv(totalValueCount, NUM_PARTITIONS), 1);
            final int avgBytesPerPartition = (int) Math.ceilDiv(Math.max(totalValueBytes, 1L), NUM_PARTITIONS);
            if (totalValueBytes <= pagedPartitionThresholdBytes) {
                flatState = new FlatBytesRefPartitionedState(
                    partitionBreaker,
                    avgKeysPerPartition,
                    avgBytesPerPartition,
                    groupIdTrackingEnabled
                );
            } else {
                pagedState = new PagedBytesRefPartitionedState(
                    bigArrays,
                    avgKeysPerPartition,
                    avgBytesPerPartition,
                    groupIdTrackingEnabled
                );
            }
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
            if (flatState != null) {
                splitFlat(firstId, shiftedIds, batchPartitionCounts);
            } else {
                splitPaged(firstId, shiftedIds, batchPartitionCounts);
            }
        }

        private void splitFlat(int firstId, short[] shiftedIds, int[] batchPartitionCounts) {
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int count = batchPartitionCounts[p];
                if (count == 0) {
                    continue;
                }
                final int base = p * PARTITION_WRITE_BATCH;
                final int keyBase = flatState.partitionCounts[p];
                ensureOffsetCapacity(p, keyBase + count + 1);
                if (flatState.seen != null) {
                    ensureSeenCapacity(p, keyBase + count);
                }
                for (int i = 0; i < count; i++) {
                    final int id = firstId + shiftedIds[base + i];
                    flatState.partitionOffsets[p][keyBase + i] = flatState.partitionDataUsed[p];
                    if (id < values.size()) {
                        BreakingBytesRefBuilder builder = values.get(id);
                        if (builder != null) {
                            final int valueLen = builder.length();
                            ensureDataCapacity(p, flatState.partitionDataUsed[p] + valueLen);
                            System.arraycopy(builder.bytes(), 0, flatState.partitionData[p], flatState.partitionDataUsed[p], valueLen);
                            flatState.partitionDataUsed[p] += valueLen;
                            if (flatState.seen != null) {
                                flatState.seen[p][keyBase + i] = true;
                            }
                        }
                    }
                }
                flatState.partitionOffsets[p][keyBase + count] = flatState.partitionDataUsed[p];
                flatState.partitionCounts[p] += count;
            }
        }

        private void splitPaged(int firstId, short[] shiftedIds, int[] batchPartitionCounts) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int count = batchPartitionCounts[p];
                if (count == 0) {
                    continue;
                }
                final int base = p * PARTITION_WRITE_BATCH;
                final int currentCount = (int) pagedState.partitionArrays[p].size();
                if (pagedState.seen != null) {
                    ensurePagedSeenCapacity(p, currentCount + count);
                }
                for (int i = 0; i < count; i++) {
                    final int id = firstId + shiftedIds[base + i];
                    BreakingBytesRefBuilder builder = id < values.size() ? values.get(id) : null;
                    if (builder != null) {
                        pagedState.partitionArrays[p].append(builder.bytesRefView());
                        if (pagedState.seen != null) {
                            pagedState.seen[p][currentCount + i] = true;
                        }
                    } else {
                        pagedState.partitionArrays[p].append(scratch);
                    }
                }
            }
        }

        private void ensureDataCapacity(int p, int minLength) {
            final byte[] sub = flatState.partitionData[p];
            if (sub.length >= minLength) {
                return;
            }
            final int newLength = ArrayUtil.oversize(minLength, 1);
            partitionBreaker.addEstimateBytesAndMaybeBreak(newLength, PARTITION_LABEL);
            flatState.partitionData[p] = Arrays.copyOf(sub, newLength);
            partitionBreaker.addWithoutBreaking(-sub.length);
        }

        private void ensureOffsetCapacity(int p, int minCount) {
            final int[] sub = flatState.partitionOffsets[p];
            if (sub.length >= minCount) {
                return;
            }
            final int newCount = ArrayUtil.oversize(minCount, Integer.BYTES);
            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByIntPage(newCount), PARTITION_LABEL);
            flatState.partitionOffsets[p] = Arrays.copyOf(sub, newCount);
            partitionBreaker.addWithoutBreaking(-bytesUsedByIntPage(sub.length));
        }

        private void ensureSeenCapacity(int p, int minCount) {
            if (flatState.seen[p] == null) {
                final int newSize = ArrayUtil.oversize(minCount, 1);
                partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), PARTITION_LABEL);
                flatState.seen[p] = new boolean[newSize];
                return;
            }
            final boolean[] sub = flatState.seen[p];
            if (sub.length >= minCount) {
                return;
            }
            final int newSize = ArrayUtil.oversize(minCount, 1);
            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), PARTITION_LABEL);
            flatState.seen[p] = Arrays.copyOf(sub, newSize);
            partitionBreaker.addWithoutBreaking(-bytesUsedBySeenPage(sub.length));
        }

        private void ensurePagedSeenCapacity(int p, int minCount) {
            if (pagedState.seen[p] == null) {
                final int newSize = ArrayUtil.oversize(minCount, 1);
                partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), PARTITION_LABEL);
                pagedState.seen[p] = new boolean[newSize];
                return;
            }
            final boolean[] sub = pagedState.seen[p];
            if (sub.length >= minCount) {
                return;
            }
            final int newSize = ArrayUtil.oversize(minCount, 1);
            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), PARTITION_LABEL);
            pagedState.seen[p] = Arrays.copyOf(sub, newSize);
            partitionBreaker.addWithoutBreaking(-bytesUsedBySeenPage(sub.length));
        }

        @Override
        public GroupingAggregatorFunction.PartitionedState finish() {
            if (flatState != null) {
                GroupingAggregatorFunction.PartitionedState result = flatState;
                flatState = null;
                return result;
            } else {
                GroupingAggregatorFunction.PartitionedState result = pagedState;
                pagedState = null;
                return result;
            }
        }

        @Override
        public void release(CircuitBreaker breaker) {
            if (flatState != null) {
                flatState.releaseAll(breaker);
                flatState = null;
            }
            if (pagedState != null) {
                pagedState.releaseAll(breaker);
                pagedState = null;
            }
        }
    }

    GroupingAggregatorFunction.PartitionSplitter createPartitioningSplitter(CircuitBreaker breaker) {
        return new BytesRefPartitionSplitter(breaker);
    }

    BytesRef[] partitionValues(GroupingAggregatorFunction.PartitionedState source, int partition) {
        if (source instanceof FlatBytesRefPartitionedState flat) {
            final int count = flat.partitionCounts[partition];
            final BytesRef[] result = new BytesRef[count];
            for (int i = 0; i < count; i++) {
                final int start = flat.partitionOffsets[partition][i];
                final int end = flat.partitionOffsets[partition][i + 1];
                result[i] = new BytesRef(flat.partitionData[partition], start, end - start);
            }
            return result;
        }
        final PagedBytesRefPartitionedState paged = (PagedBytesRefPartitionedState) source;
        final int count = (int) paged.partitionArrays[partition].size();
        final BytesRef[] result = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            result[i] = new BytesRef();
            paged.partitionArrays[partition].get(i, result[i]);
        }
        return result;
    }

    boolean[] partitionSeen(GroupingAggregatorFunction.PartitionedState source, int partition) {
        if (source instanceof FlatBytesRefPartitionedState flat) {
            return flat.seen == null ? null : flat.seen[partition];
        }
        final PagedBytesRefPartitionedState paged = (PagedBytesRefPartitionedState) source;
        return paged.seen == null ? null : paged.seen[partition];
    }

    void appendPartition(BytesRef[] src, int firstId, int length) {
        for (int i = 0; i < length; i++) {
            set(firstId + i, src[i]);
        }
    }
}
