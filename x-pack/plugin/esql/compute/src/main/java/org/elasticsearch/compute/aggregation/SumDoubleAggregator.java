/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.util.Arrays;

@Aggregator(
    {
        @IntermediateState(name = "value", type = "DOUBLE"),
        @IntermediateState(name = "delta", type = "DOUBLE"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator(supportsPartitioning = true)
class SumDoubleAggregator {

    public static SumState initSingle() {
        return new SumState();
    }

    public static void combine(SumState current, double v) {
        current.add(v);
    }

    public static void combine(SumState current, double value, double delta) {
        current.add(value, delta);
    }

    public static void combineIntermediate(SumState state, double inValue, double inDelta, boolean seen) {
        if (seen) {
            combine(state, inValue, inDelta);
            state.seen(true);
        }
    }

    public static void evaluateIntermediate(SumState state, DriverContext driverContext, Block[] blocks, int offset) {
        assert blocks.length >= offset + 3;
        BlockFactory blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(state.value(), 1);
        blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(state.delta(), 1);
        blocks[offset + 2] = blockFactory.newConstantBooleanBlockWith(state.seen(), 1);
    }

    public static Block evaluateFinal(SumState state, DriverContext driverContext) {
        double result = state.value();
        return driverContext.blockFactory().newConstantDoubleBlockWith(result, 1);
    }

    public static GroupingSumState initGrouping(BigArrays bigArrays) {
        return new GroupingSumState(bigArrays);
    }

    public static void combine(GroupingSumState current, int groupId, double v) {
        current.add(v, groupId);
    }

    public static void combineIntermediate(GroupingSumState current, int groupId, double inValue, double inDelta, boolean seen) {
        if (seen) {
            current.add(inValue, inDelta, groupId);
        }
    }

    public static void combinePartition(
        GroupingSumState current,
        GroupingAggregatorFunction.PartitionedState source,
        int partition,
        boolean appendOnly,
        int[] dstIds,
        int length
    ) {
        if (length == 0) {
            return;
        }
        GroupingSumState.PartitionedSumState partitioned = (GroupingSumState.PartitionedSumState) source;
        if (partitioned.seen == null) {
            if (appendOnly) {
                current.appendPartition(partitioned.data[partition], dstIds[0], length);
            } else {
                for (int i = 0; i < length; i++) {
                    current.add(partitioned.data[partition][2 * i], partitioned.data[partition][2 * i + 1], dstIds[i]);
                }
            }
        } else {
            for (int i = 0; i < length; i++) {
                if (partitioned.seen[partition][i]) {
                    current.add(partitioned.data[partition][2 * i], partitioned.data[partition][2 * i + 1], dstIds[i]);
                }
            }
        }
    }

    public static void evaluateIntermediate(
        GroupingSumState state,
        Block[] blocks,
        int offset,
        IntVector selected,
        DriverContext driverContext
    ) {
        assert blocks.length >= offset + 3;
        boolean allHaveValue = true;
        try (
            var valuesBuilder = driverContext.blockFactory().newDoubleVectorFixedBuilder(selected.getPositionCount());
            var deltaBuilder = driverContext.blockFactory().newDoubleVectorFixedBuilder(selected.getPositionCount());
            var seenBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < state.values.size() && state.hasValue(group)) {
                    valuesBuilder.appendDouble(i, state.values.get(group));
                    deltaBuilder.appendDouble(i, state.deltas.get(group));
                    seenBuilder.appendBoolean(i, true);
                } else {
                    allHaveValue = false;
                    valuesBuilder.appendDouble(i, 0);
                    deltaBuilder.appendDouble(i, 0);
                    seenBuilder.appendBoolean(i, false);
                }
            }
            blocks[offset + 0] = valuesBuilder.build().asBlock();
            blocks[offset + 1] = deltaBuilder.build().asBlock();
            if (allHaveValue) {
                // switch to a constant block to reduce memory usage and allow fast checks
                blocks[offset + 2] = driverContext.blockFactory().newConstantBooleanBlockWith(true, selected.getPositionCount());
            } else {
                blocks[offset + 2] = seenBuilder.build().asBlock();
            }
        }
    }

    public static Block evaluateFinal(GroupingSumState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        try (DoubleBlock.Builder builder = ctx.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int si = selected.getInt(i);
                if (state.hasValue(si) && si < state.values.size()) {
                    builder.appendDouble(state.values.get(si));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    static class SumState extends CompensatedSum implements AggregatorState {

        private boolean seen;

        SumState() {
            this(0, 0);
        }

        SumState(double value, double delta) {
            super(value, delta);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            SumDoubleAggregator.evaluateIntermediate(this, driverContext, blocks, offset);
        }

        @Override
        public void close() {}

        public boolean seen() {
            return seen;
        }

        public void seen(boolean seen) {
            this.seen = seen;
        }
    }

    static class GroupingSumState extends AbstractArrayState implements GroupingAggregatorState {
        static final long BYTES_SIZE = Double.BYTES + Double.BYTES;

        DoubleArray values;
        DoubleArray deltas;

        GroupingSumState(BigArrays bigArrays) {
            super(bigArrays);
            boolean success = false;
            try {
                this.values = bigArrays.newDoubleArray(1);
                this.deltas = bigArrays.newDoubleArray(1);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void add(double valueToAdd, int groupId) {
            add(valueToAdd, 0d, groupId);
        }

        void add(double valueToAdd, double deltaToAdd, int groupId) {
            ensureCapacity(groupId);
            // Reaching here means the group has an observation, whatever its value, so it must be reported as seen.
            // An untracked group is rendered as null, which would drop a group whose total is ±Inf or NaN.
            trackGroupId(groupId);

            // If the value is Inf or NaN, just add it to the running tally to "convert" to
            // Inf/NaN. This keeps the behavior bwc from before kahan summing
            if (Double.isFinite(valueToAdd) == false) {
                values.increment(groupId, valueToAdd);
                return;
            }

            double value = values.get(groupId);
            if (Double.isFinite(value) == false) {
                // It isn't going to get any more infinite.
                return;
            }
            double delta = deltas.get(groupId);
            double correctedSum = valueToAdd + (delta + deltaToAdd);
            double updatedValue = value + correctedSum;
            deltas.set(groupId, correctedSum - (updatedValue - value));
            values.set(groupId, updatedValue);
        }

        void ensureCapacity(int groupId) {
            values = bigArrays.grow(values, groupId + 1);
            deltas = bigArrays.grow(deltas, groupId + 1);
        }

        GroupingAggregatorFunction.PartitionSplitter createPartitioningSplitter(CircuitBreaker breaker) {
            return new SumPartitionSplitter(breaker);
        }

        void appendPartition(double[] partitionData, int firstId, int length) {
            ensureCapacity(firstId + length);
            for (int i = 0; i < length; i++) {
                int groupId = firstId + i;
                values.set(groupId, partitionData[2 * i]);
                deltas.set(groupId, partitionData[2 * i + 1]);
                trackGroupId(groupId);
            }
        }

        private static final class PartitionedSumState implements GroupingAggregatorFunction.PartitionedState {
            private final double[][] data;
            private final boolean[][] seen;
            private final long baseBytes;

            private PartitionedSumState(CircuitBreaker breaker, int partitionSize, boolean trackSeen) {
                this.data = new double[PartitionedHashTable.NUM_PARTITIONS][];
                this.seen = trackSeen ? new boolean[PartitionedHashTable.NUM_PARTITIONS][] : null;
                this.baseBytes = RamUsageEstimator.shallowSizeOf(PartitionedSumState.class) + arrayBytes(
                    PartitionedHashTable.NUM_PARTITIONS,
                    RamUsageEstimator.NUM_BYTES_OBJECT_REF
                ) + (trackSeen ? arrayBytes(PartitionedHashTable.NUM_PARTITIONS, RamUsageEstimator.NUM_BYTES_OBJECT_REF) : 0);
                final long usedBytes = baseBytes + partitionBytes(partitionSize, trackSeen) * PartitionedHashTable.NUM_PARTITIONS;
                breaker.addEstimateBytesAndMaybeBreak(usedBytes, "SumDoubleAggregator#partition");
                for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                    data[p] = new double[Math.multiplyExact(partitionSize, 2)];
                    if (seen != null) {
                        seen[p] = new boolean[partitionSize];
                    }
                }
            }

            @Override
            public boolean hasAllValues(int partition) {
                return seen == null;
            }

            @Override
            public void releasePartition(CircuitBreaker breaker, int partition) {
                if (data[partition] == null) {
                    return;
                }
                final long bytes = partitionBytes(data[partition].length / 2, seen != null);
                data[partition] = null;
                if (seen != null) {
                    seen[partition] = null;
                }
                breaker.addWithoutBreaking(-bytes);
            }

            @Override
            public void releaseAll(CircuitBreaker breaker) {
                long bytes = baseBytes;
                for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                    if (data[p] != null) {
                        bytes += partitionBytes(data[p].length / 2, seen != null);
                        data[p] = null;
                        if (seen != null) {
                            seen[p] = null;
                        }
                    }
                }
                breaker.addWithoutBreaking(-bytes);
            }

            private static long partitionBytes(int length, boolean trackSeen) {
                return arrayBytes(Math.multiplyExact(length, 2), Double.BYTES) + (trackSeen ? arrayBytes(length, Byte.BYTES) : 0);
            }

            private static long arrayBytes(int length, int elementSize) {
                return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * elementSize);
            }
        }

        private final class SumPartitionSplitter implements GroupingAggregatorFunction.PartitionSplitter {
            private final CircuitBreaker breaker;
            private PartitionedSumState partitionedState;

            private SumPartitionSplitter(CircuitBreaker breaker) {
                this.breaker = breaker;
                int partitionSize = ArrayUtil.oversize(
                    Math.max(1, Math.toIntExact(Math.ceilDiv(values.size(), (long) PartitionedHashTable.NUM_PARTITIONS))),
                    Double.BYTES
                );
                this.partitionedState = new PartitionedSumState(breaker, partitionSize, trackingGroupIds());
            }

            @Override
            public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
                for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                    int count = batchPartitionCounts[p];
                    if (count == 0) {
                        continue;
                    }
                    int offset = partitionOffsets[p];
                    ensurePartitionCapacity(p, offset + count);
                    int base = p * PartitionedHashTable.PARTITION_WRITE_BATCH;
                    for (int i = 0; i < count; i++) {
                        int groupId = firstId + shiftedIds[base + i];
                        if (groupId < values.size() && hasValue(groupId)) {
                            int dataOffset = 2 * (offset + i);
                            partitionedState.data[p][dataOffset] = values.get(groupId);
                            partitionedState.data[p][dataOffset + 1] = deltas.get(groupId);
                            if (partitionedState.seen != null) {
                                partitionedState.seen[p][offset + i] = true;
                            }
                        }
                    }
                }
            }

            private void ensurePartitionCapacity(int partition, int minSize) {
                double[] oldData = partitionedState.data[partition];
                int oldSize = oldData.length / 2;
                if (oldSize >= minSize) {
                    return;
                }
                int newSize = ArrayUtil.oversize(minSize, Double.BYTES);
                breaker.addEstimateBytesAndMaybeBreak(
                    PartitionedSumState.partitionBytes(newSize, partitionedState.seen != null),
                    "SumDoubleAggregator#partition"
                );
                partitionedState.data[partition] = Arrays.copyOf(oldData, Math.multiplyExact(newSize, 2));
                if (partitionedState.seen != null) {
                    partitionedState.seen[partition] = Arrays.copyOf(partitionedState.seen[partition], newSize);
                }
                breaker.addWithoutBreaking(-PartitionedSumState.partitionBytes(oldSize, partitionedState.seen != null));
            }

            @Override
            public PartitionedSumState finish() {
                PartitionedSumState result = partitionedState;
                partitionedState = null;
                return result;
            }

            @Override
            public void release(CircuitBreaker ignored) {
                if (partitionedState != null) {
                    partitionedState.releaseAll(breaker);
                    partitionedState = null;
                }
            }
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            SumDoubleAggregator.evaluateIntermediate(this, blocks, offset, selected, driverContext);
        }

        @Override
        public void close() {
            Releasables.close(values, deltas, super::close);
        }
    }
}
