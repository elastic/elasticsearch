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
    private final BigArrays bigArrays;
    private final CircuitBreaker breaker;
    private final String breakerLabel;
    private ObjectArray<BreakingBytesRefBuilder> values;
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
        growForGroupId(groupId);

        var currentBuilder = values.get(groupId);
        if (currentBuilder == null) {
            currentBuilder = new BreakingBytesRefBuilder(breaker, breakerLabel, value.length);
            values.set(groupId, currentBuilder);
        }

        currentBuilder.copyBytes(value);
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

    private void growForGroupId(int groupId) {
        ensureCapacity(groupId + 1);
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

    private static long bytesUsedByValue(BytesRef value) {
        return RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) + RamUsageEstimator.alignObjectSize(
            (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + value.length
        );
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
        private BytesRefPartitionedState partitionedState;

        BytesRefPartitionSplitter(CircuitBreaker partitionBreaker) {
            this.partitionBreaker = partitionBreaker;
            int partitionSize = ArrayUtil.oversize(Math.max(1, Math.ceilDiv((int) values.size(), NUM_PARTITIONS)), Long.BYTES);
            partitionedState = new BytesRefPartitionedState(partitionBreaker, partitionSize, groupIdTrackingEnabled);
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
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
                            partitionedState.values[p][offset + i] = copy;
                            if (partitionedState.seen != null) {
                                partitionedState.seen[p][offset + i] = true;
                            }
                        }
                    }
                }
            }
        }

        private void ensurePartitionCapacity(int partition, int minSize) {
            BytesRef[] oldValues = partitionedState.values[partition];
            if (oldValues.length >= minSize) {
                return;
            }
            int newSize = ArrayUtil.oversize(minSize, Long.BYTES);
            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByPointerPage(newSize), BytesRefPartitionedState.LABEL);
            partitionedState.values[partition] = Arrays.copyOf(oldValues, newSize);
            partitionBreaker.addWithoutBreaking(-bytesUsedByPointerPage(oldValues.length));
            if (partitionedState.seen != null) {
                boolean[] oldSeen = partitionedState.seen[partition];
                partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), BytesRefPartitionedState.LABEL);
                partitionedState.seen[partition] = Arrays.copyOf(oldSeen, newSize);
                partitionBreaker.addWithoutBreaking(-bytesUsedBySeenPage(oldSeen.length));
            }
        }

        @Override
        public BytesRefPartitionedState finish() {
            BytesRefPartitionedState result = partitionedState;
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

    GroupingAggregatorFunction.PartitionSplitter createPartitioningSplitter(CircuitBreaker breaker) {
        return new BytesRefPartitionSplitter(breaker);
    }

    BytesRef[] partitionValues(GroupingAggregatorFunction.PartitionedState source, int partition) {
        return ((BytesRefPartitionedState) source).values[partition];
    }

    boolean[] partitionSeen(GroupingAggregatorFunction.PartitionedState source, int partition) {
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
