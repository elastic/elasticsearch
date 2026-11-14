/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.util.PartitionedHashTable.NUM_PARTITIONS;
import static org.elasticsearch.common.util.PartitionedHashTable.PARTITION_WRITE_BATCH;

public final class DimensionValuesByteRefGroupingAggregatorFunction implements GroupingAggregatorFunction {

    public static final class FunctionSupplier implements AggregatorFunctionSupplier {

        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return INTERMEDIATE_STATE_DESC;
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public DimensionValuesByteRefGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new DimensionValuesByteRefGroupingAggregatorFunction(channels, driverContext);
        }

        @Override
        public String describe() {
            return "dimensions of bytes";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(new IntermediateStateDesc("values", ElementType.BYTES_REF));

    private final BytesRefBlock.Builder builder;
    private final int channel;
    private final DriverContext driverContext;
    private int maxGroupId = -1;
    private BytesRefBlock values;

    public DimensionValuesByteRefGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channel = channels.getFirst();
        this.driverContext = driverContext;
        this.builder = driverContext.blockFactory().newBytesRefBlockBuilder(4096);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // manage nulls
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        BytesRefBlock valuesBlock = page.getBlock(channel);
        if (valuesBlock.areAllValuesNull()) {
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {

                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {

                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {

                }

                @Override
                public void close() {

                }
            };
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addInputValuesBlock(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addInputValuesBlock(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                var valuesVector = valuesBlock.asVector();
                if (valuesVector != null) {
                    addInputValuesVector(positionOffset, groupIds, valuesVector);
                } else {
                    addInputValuesBlock(positionOffset, groupIds, valuesBlock);
                }
            }

            @Override
            public void close() {

            }
        };
    }

    // Note that this path can be executed randomly in tests, not in production
    void addInputValuesBlock(int positionOffset, IntBlock groups, BytesRefBlock valueBlock) {
        var scratch = new BytesRef();
        int positionCount = groups.getPositionCount();
        for (int p = 0; p < positionCount; p++) {
            if (groups.isNull(p)) {
                continue;
            }
            int valuePosition = p + positionOffset;
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            for (int g = groupStart; g < groupEnd; g++) {
                final int groupId = groups.getInt(g);
                if (maxGroupId < groupId) {
                    fillNullsUpTo(groupId);
                    builder.copyFrom(valueBlock, valuePosition, scratch);
                    maxGroupId = groupId;
                }
            }
        }
    }

    void addInputValuesBlock(int positionOffset, IntVector groups, BytesRefBlock valueBlock) {
        var scratch = new BytesRef();
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            if (groupId > maxGroupId) {
                fillNullsUpTo(groupId);
                builder.copyFrom(valueBlock, positionOffset, scratch);
                maxGroupId = groupId;
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int groupId = groups.getInt(p);
                if (groupId > maxGroupId) {
                    fillNullsUpTo(groupId);
                    builder.copyFrom(valueBlock, positionOffset + p, scratch);
                    maxGroupId = groupId;
                }
            }
        }
    }

    void addInputValuesVector(int positionOffset, IntVector groups, BytesRefVector valueVector) {
        var scratch = new BytesRef();
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            if (groupId > maxGroupId) {
                fillNullsUpTo(groupId);
                builder.appendBytesRef(valueVector.getBytesRef(positionOffset, scratch));
                maxGroupId = groupId;
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int groupId = groups.getInt(p);
                if (groupId > maxGroupId) {
                    fillNullsUpTo(groupId);
                    builder.appendBytesRef(valueVector.getBytesRef(positionOffset + p, scratch));
                    maxGroupId = groupId;
                }
            }
        }
    }

    private void fillNullsUpTo(int groupId) {
        for (int i = maxGroupId + 1; i < groupId; i++) {
            builder.appendNull();
        }
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        BytesRefBlock valuesBlock = page.getBlock(channel);
        if (valuesBlock.areAllValuesNull()) {
            return;
        }
        addInputValuesBlock(positionOffset, groups, valuesBlock);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        BytesRefBlock valuesBlock = page.getBlock(channel);
        if (valuesBlock.areAllValuesNull()) {
            return;
        }
        addInputValuesBlock(positionOffset, groups, valuesBlock);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        BytesRefBlock valuesBlock = page.getBlock(channel);
        if (valuesBlock.areAllValuesNull()) {
            return;
        }
        var valuesVector = valuesBlock.asVector();
        if (valuesVector != null) {
            addInputValuesVector(positionOffset, groups, valuesVector);
        } else {
            addInputValuesBlock(positionOffset, groups, valuesBlock);
        }
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        values = builder.build();
        return this::writeSelectedValues;
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return prepareEvaluateIntermediate(selected, ctx);
    }

    private void writeSelectedValues(Block[] blocks, int offset, IntVector selectedInPage) {
        if (selectsEveryGroupInOrder(selectedInPage)) {
            values.incRef();
            blocks[offset] = values;
            return;
        }
        final int positionCount = selectedInPage.getPositionCount();
        final BytesRef scratch = new BytesRef();
        try (var outputBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int groupId = selectedInPage.getInt(p);
                if (groupId < values.getPositionCount()) {
                    outputBuilder.copyFrom(values, groupId, scratch);
                } else {
                    outputBuilder.appendNull();
                }
            }
            blocks[offset] = outputBuilder.build();
        }
    }

    /**
     * Whether the page selects every materialized group exactly once and in order, so the materialized values block can
     * be reused as the output without copying. A chunked page selects only a slice and the output-filtered final page may
     * reorder or drop groups, so both fall back to copying. Reference equality on the selected vector is not enough to
     * decide this, because the final output-filtered path passes a vector that can be reordered.
     */
    private boolean selectsEveryGroupInOrder(IntVector selectedInPage) {
        if (selectedInPage.getPositionCount() != values.getPositionCount()) {
            return false;
        }
        for (int p = 0; p < selectedInPage.getPositionCount(); p++) {
            if (selectedInPage.getInt(p) != p) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean supportPartitioning() {
        return true;
    }

    @Override
    public PartitionSplitter createPartitioningSplitter(CircuitBreaker breaker) {
        if (values == null) {
            values = builder.build();
        }
        return new BlocksPartitionSplitter(values, driverContext.blockFactory());
    }

    @Override
    public void combinePartition(PartitionedState source, int partition, boolean appendOnly, int[] dstIds, int length) {
        final BytesRefBlock block = ((BlocksPartitionedState) source).blocks[partition];
        if (appendOnly && length > 0) {
            assert dstIds[0] > maxGroupId && dstIds[length - 1] == dstIds[0] + length - 1 : "appendOnly ids must be new and consecutive";
            fillNullsUpTo(dstIds[0]);
            builder.copyFrom(block, 0, length);
            maxGroupId = dstIds[length - 1];
            return;
        }
        final BytesRef scratch = new BytesRef();
        for (int i = 0; i < length; i++) {
            final int groupId = dstIds[i];
            if (groupId > maxGroupId) {
                fillNullsUpTo(groupId);
                builder.copyFrom(block, i, scratch);
                maxGroupId = groupId;
            }
        }
    }

    private record BlocksPartitionedState(BytesRefBlock[] blocks) implements PartitionedState {
        @Override
        public boolean hasAllValues(int partition) {
            return true;
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            Releasables.close(blocks[partition]);
            blocks[partition] = null;
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            Releasables.close(blocks);
            Arrays.fill(blocks, null);
        }
    }

    private static final class BlocksPartitionSplitter implements PartitionSplitter {
        private final BytesRefBlock values;
        private BytesRefBlock.Builder[] builders;

        BlocksPartitionSplitter(BytesRefBlock values, BlockFactory blockFactory) {
            this.values = values;
            final int estimatedSize = Math.max(1, Math.ceilDiv(values.getPositionCount(), NUM_PARTITIONS));
            builders = new BytesRefBlock.Builder[NUM_PARTITIONS];
            boolean success = false;
            try {
                for (int p = 0; p < NUM_PARTITIONS; p++) {
                    builders[p] = blockFactory.newBytesRefBlockBuilder(estimatedSize);
                }
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(builders);
                }
            }
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
            final BytesRef scratch = new BytesRef();
            final int positionCount = values.getPositionCount();
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int count = batchPartitionCounts[p];
                if (count == 0) {
                    continue;
                }
                final BytesRefBlock.Builder builder = builders[p];
                final int base = p * PARTITION_WRITE_BATCH;
                for (int i = 0; i < count; i++) {
                    final int id = firstId + shiftedIds[base + i];
                    if (id < positionCount) {
                        builder.copyFrom(values, id, scratch);
                    } else {
                        builder.appendNull();
                    }
                }
            }
        }

        @Override
        public PartitionedState finish() {
            final BytesRefBlock[] blocks = new BytesRefBlock[NUM_PARTITIONS];
            boolean success = false;
            try {
                for (int p = 0; p < NUM_PARTITIONS; p++) {
                    blocks[p] = builders[p].build();
                    blocks[p].allowPassingToDifferentDriver();
                }
                success = true;
            } finally {
                Releasables.close(builders);
                builders = null;
                if (success == false) {
                    Releasables.close(blocks);
                }
            }
            return new BlocksPartitionedState(blocks);
        }

        @Override
        public void release(CircuitBreaker breaker) {
            if (builders != null) {
                Releasables.close(builders);
                builders = null;
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(builder, values);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channel);
        sb.append("]");
        return sb.toString();
    }
}
