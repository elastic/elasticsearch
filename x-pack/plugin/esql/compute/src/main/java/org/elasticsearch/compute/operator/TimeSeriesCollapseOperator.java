/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Collapses expanded time-series rows into one multi-valued row per output series.
 * <p>
 * Groups are the output dimensions from the preceding aggregation. For each group this operator stores values by requested
 * time step, then emits the steps in range order so the {@code step} and {@code value} multi-valued columns stay positionally aligned.
 * <p>
 * The collapse state is a dense grid of {@code groupId x stepOrdinal} flattened into parallel {@code values} and {@code seen}
 * arrays:
 * <pre>{@code
 * stepCount = 4
 * steps     = [10, 20, 30, 40]
 *
 *                 |        group 0        |        group 1        |        group 2        |
 *                 |-----------------------|-----------------------|-----------------------|
 * stepOrdinal     |   0 |   1 |   2 |   3 |   0 |   1 |   2 |   3 |   0 |   1 |   2 |   3 |
 * timestamp       |  10 |  20 |  30 |  40 |  10 |  20 |  30 |  40 |  10 |  20 |  30 |  40 |
 * flat index      |   0 |   1 |   2 |   3 |   4 |   5 |   6 |   7 |   8 |   9 |  10 |  11 |
 *
 * values[]        | 1.2 |  -- | 3.4 |  -- |  -- | 8.1 |  -- | 8.4 | 5.0 |  -- |  -- |  -- |
 * seen[]          |   1 |   0 |   1 |   0 |   0 |   1 |   0 |   1 |   1 |   0 |   0 |   0 |
 * }</pre>
 * <p>
 * The grouping hash maps dimension keys to the {@code groupId} slices:
 * <pre>{@code
 * {host=h1, metric=cpu} -> group 0 -> flat indexes 0..3
 * {host=h2, metric=cpu} -> group 1 -> flat indexes 4..7
 * {host=h3, metric=mem} -> group 2 -> flat indexes 8..11
 * }</pre>
 * <p>
 * The value storage grows to the end of a group's slice when that group is first encountered, so observing {@code group 1}
 * grows the value array to at least flat index {@code 7 + 1}. The {@code seen} bitset still marks only the slots that
 * actually received values.
 * <p>
 * The value column is {@link ElementType#DOUBLE} only for now (e.g. PROMQL scalar outputs).
 * <p>
 * <b>Step semantics:</b> {@code step} is a fixed millisecond interval and the operator assumes input timestamps fall
 * on the uniform grid {@code start, start + step, start + 2*step, ..., end}. Calendar units (months, years) and
 * timezone-aware steps that span DST transitions are <em>not</em> supported — input timestamps that don't satisfy
 * {@code (timestamp - start) % step == 0} are rejected with an {@link IllegalArgumentException}. This matches PROMQL
 * {@code query_range} semantics; for calendar-aware bucketing, use a different operator wired to {@link
 * org.elasticsearch.common.Rounding.Prepared}.
 */
public class TimeSeriesCollapseOperator extends HashAggregationOperator {

    private final List<BlockHash.GroupSpec> groups;
    private final int valueChannel;
    private final int stepChannel;
    private final int maxPageSize;
    private final int outputBlockCount;
    private final CollapseState state;

    public record Factory(
        List<BlockHash.GroupSpec> groups,
        int valueChannel,
        int stepChannel,
        long start,
        long end,
        long step,
        int maxPageSize,
        int aggregationBatchSize
    ) implements OperatorFactory {

        public Factory(List<BlockHash.GroupSpec> groups, int valueChannel, int stepChannel, long start, long end, long step) {
            this(
                groups,
                valueChannel,
                stepChannel,
                start,
                end,
                step,
                Operator.TARGET_PAGE_SIZE / Long.SIZE,
                Operator.TARGET_PAGE_SIZE / Long.SIZE
            );
        }

        public Factory {
            Objects.requireNonNull(groups, "groups");
            if (step <= 0) {
                throw new IllegalArgumentException("step must be greater than 0; got " + step);
            }
            if (end < start) {
                throw new IllegalArgumentException("end must be greater than or equal to start; got [" + start + ", " + end + "]");
            }
            // Fail fast: stepCount overflows or does not fit int; outputBlockCount rejects invalid or colliding channels.
            stepCount(start, end, step);
            outputBlockCount(groups, valueChannel, stepChannel);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TimeSeriesCollapseOperator(
                groups,
                valueChannel,
                stepChannel,
                start,
                end,
                step,
                maxPageSize,
                () -> BlockHash.build(groups, driverContext.blockFactory(), aggregationBatchSize, false),
                driverContext
            );
        }

        @Override
        public String describe() {
            return "TimeSeriesCollapseOperator[groups = "
                + groups
                + ", valueChannel = "
                + valueChannel
                + ", valueElementType = "
                + ElementType.DOUBLE
                + ", stepChannel = "
                + stepChannel
                + ", start = "
                + start
                + ", end = "
                + end
                + ", step = "
                + step
                + "]";
        }
    }

    public TimeSeriesCollapseOperator(
        List<BlockHash.GroupSpec> groups,
        int valueChannel,
        int stepChannel,
        long start,
        long end,
        long step,
        int maxPageSize,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        this(
            new CollapseState(driverContext.bigArrays(), driverContext.blockFactory(), valueChannel, stepChannel, start, end, step),
            groups,
            valueChannel,
            stepChannel,
            maxPageSize,
            blockHash,
            driverContext
        );
    }

    private TimeSeriesCollapseOperator(
        CollapseState state,
        List<BlockHash.GroupSpec> groups,
        int valueChannel,
        int stepChannel,
        int maxPageSize,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        super(AggregatorMode.SINGLE, aggregators(state), blockHash, Integer.MAX_VALUE, 1.0, maxPageSize, driverContext);
        this.groups = groups;
        this.valueChannel = valueChannel;
        this.stepChannel = stepChannel;
        this.maxPageSize = maxPageSize;
        this.outputBlockCount = outputBlockCount(groups, valueChannel, stepChannel);
        this.state = state;
    }

    @Override
    protected void emit() {
        if (rowsAddedInCurrentBatch == 0) {
            return;
        }
        long startInNanos = System.nanoTime();
        IntVector selected = null;
        try {
            selected = blockHash.nonEmpty();
            if (selected.getPositionCount() <= maxPageSize) {
                output = ReleasableIterator.single(buildOutput(selected));
            } else {
                output = new CollapsePages(selected);
                selected = null;
            }
        } finally {
            rowsAddedInCurrentBatch = 0;
            Releasables.close(selected);
            emitNanos += System.nanoTime() - startInNanos;
            emitCount++;
        }
    }

    private Page buildOutput(IntVector selected) {
        Block[] blocks = new Block[outputBlockCount];
        Block[] keyBlocks = null;
        boolean success = false;
        try {
            keyBlocks = blockHash.getKeys(selected);
            for (int i = 0; i < keyBlocks.length; i++) {
                blocks[groups.get(i).channel()] = keyBlocks[i];
                keyBlocks[i] = null;
            }
            blocks[stepChannel] = state.buildSteps(selected);
            blocks[valueChannel] = state.buildValues(selected);
            checkAllChannelsSet(blocks);
            success = true;
            return new Page(blocks);
        } finally {
            if (keyBlocks != null) {
                Releasables.close(keyBlocks);
            }
            if (success == false) {
                Releasables.close(blocks);
            }
        }
    }

    @Override
    protected boolean shouldEmitPartialResultsPeriodically() {
        return false;
    }

    @Override
    public String toString() {
        return "TimeSeriesCollapseOperator[blockHash=" + blockHash + ", aggs=" + aggregators + "]";
    }

    private static List<GroupingAggregator.Factory> aggregators(CollapseState state) {
        return List.of(new CollapseAggregatorFactory(new CollapseFunction(state)));
    }

    private static int outputBlockCount(List<BlockHash.GroupSpec> groups, int valueChannel, int stepChannel) {
        if (valueChannel == stepChannel) {
            throw new IllegalArgumentException("valueChannel and stepChannel must be different; got [" + valueChannel + "]");
        }
        int max = Math.max(valueChannel, stepChannel);
        boolean[] used = new boolean[Math.max(max + 1, groups.size() + 2)];
        used[valueChannel] = true;
        used[stepChannel] = true;
        for (BlockHash.GroupSpec group : groups) {
            if (group.channel() < 0) {
                throw new IllegalArgumentException("group channel must be non-negative; got [" + group.channel() + "]");
            }
            if (used.length <= group.channel()) {
                used = Arrays.copyOf(used, group.channel() + 1);
            }
            if (used[group.channel()]) {
                throw new IllegalArgumentException("duplicate time series collapse output channel [" + group.channel() + "]");
            }
            used[group.channel()] = true;
            max = Math.max(max, group.channel());
        }
        return max + 1;
    }

    private static void checkAllChannelsSet(Block[] blocks) {
        for (int i = 0; i < blocks.length; i++) {
            if (blocks[i] == null) {
                throw new IllegalArgumentException(
                    "time series collapse output channel [" + i + "] is not covered by dimensions, step, or value"
                );
            }
        }
    }

    private static int stepCount(long start, long end, long step) {
        long delta = Math.subtractExact(end, start);
        return Math.toIntExact(Math.addExact(delta / step, 1));
    }

    /**
     * Splits the final selected group ids into output pages without copying the collapse state.
     */
    private class CollapsePages implements ReleasableIterator<Page> {
        private final IntVector selected;
        private int rowOffset;

        private CollapsePages(IntVector selected) {
            this.selected = selected;
        }

        @Override
        public boolean hasNext() {
            return rowOffset < selected.getPositionCount();
        }

        @Override
        public Page next() {
            long startInNanos = System.nanoTime();
            int endOffset = Math.min(maxPageSize + rowOffset, selected.getPositionCount());
            try (IntVector selectedInThisPage = selected.slice(rowOffset, endOffset)) {
                rowOffset = endOffset;
                return buildOutput(selectedInThisPage);
            } finally {
                emitNanos += System.nanoTime() - startInNanos;
            }
        }

        @Override
        public void close() {
            selected.close();
        }
    }

    /**
     * Adapts the single synthetic {@link CollapseFunction} as a {@link GroupingAggregator.Factory}. The collapse operator
     * overrides {@link HashAggregationOperator#emit()} and builds output blocks directly from {@link CollapseState}, so
     * one synthetic aggregator suffices regardless of how many output columns the operator emits.
     */
    private record CollapseAggregatorFactory(CollapseFunction function) implements GroupingAggregator.Factory {
        @Override
        public GroupingAggregator apply(DriverContext driverContext) {
            return new GroupingAggregator(function, AggregatorMode.SINGLE);
        }

        @Override
        public String describe() {
            return function.describe();
        }
    }

    /**
     * Synthetic aggregator that ingests raw input pages into {@link CollapseState} and owns its lifecycle. Output blocks
     * are produced by the operator's {@link #buildOutput(IntVector)}, not by {@link #prepareEvaluateFinal}.
     */
    private static final class CollapseFunction implements GroupingAggregatorFunction, Describable {
        private final CollapseState state;

        private CollapseFunction(CollapseState state) {
            this.state = state;
        }

        @Override
        public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
            return state.prepareProcessPage(page);
        }

        @Override
        public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {}

        @Override
        public void addIntermediateInput(int positionOffset, IntArrayBlock groupIdVector, Page page) {
            throw new UnsupportedOperationException("time series collapse does not support intermediate input");
        }

        @Override
        public void addIntermediateInput(int positionOffset, IntBigArrayBlock groupIdVector, Page page) {
            throw new UnsupportedOperationException("time series collapse does not support intermediate input");
        }

        @Override
        public void addIntermediateInput(int positionOffset, IntVector groupIdVector, Page page) {
            throw new UnsupportedOperationException("time series collapse does not support intermediate input");
        }

        @Override
        public PreparedForEvaluation prepareEvaluateIntermediate(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
            throw new UnsupportedOperationException("time series collapse does not emit intermediate output");
        }

        @Override
        public PreparedForEvaluation prepareEvaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
            throw new UnsupportedOperationException("time series collapse builds output blocks directly in the operator");
        }

        @Override
        public int intermediateBlockCount() {
            return 1;
        }

        @Override
        public String describe() {
            return "time series collapse";
        }

        @Override
        public String toString() {
            return describe();
        }

        @Override
        public void close() {
            state.close();
        }
    }

    /**
     * Stores collapsed values in a dense flattened {@code groupId x stepOrdinal} grid.
     */
    private static final class CollapseState implements AutoCloseable {
        private final BigArrays bigArrays;
        private final BlockFactory blockFactory;
        private final int valueChannel;
        private final int stepChannel;
        private final long start;
        private final long end;
        private final long step;
        private final int stepCount;
        private DoubleArray values;
        private final BitArray seen;
        private boolean closed;

        private CollapseState(
            BigArrays bigArrays,
            BlockFactory blockFactory,
            int valueChannel,
            int stepChannel,
            long start,
            long end,
            long step
        ) {
            this.bigArrays = bigArrays;
            this.blockFactory = blockFactory;
            this.valueChannel = valueChannel;
            this.stepChannel = stepChannel;
            this.start = start;
            this.end = end;
            this.step = step;
            this.stepCount = stepCount(start, end, step);
            boolean success = false;
            try {
                // One group occupies stepCount doubles and the same number of seen bits; size for group 0 up front to
                // avoid growing on the first insert.
                this.values = bigArrays.newDoubleArray(stepCount);
                this.seen = new BitArray(stepCount, bigArrays);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        private GroupingAggregatorFunction.AddInput prepareProcessPage(Page page) {
            Block rawValueBlock = page.getBlock(valueChannel);
            if (rawValueBlock.areAllValuesNull()) {
                return null;
            }
            if (rawValueBlock.elementType() != ElementType.DOUBLE) {
                throw new IllegalArgumentException(
                    "time series collapse expected value column of type ["
                        + ElementType.DOUBLE
                        + "] but got ["
                        + rawValueBlock.elementType()
                        + "]"
                );
            }
            DoubleBlock valueBlock = (DoubleBlock) rawValueBlock;
            Block rawStepBlock = page.getBlock(stepChannel);
            if (rawStepBlock.elementType() != ElementType.LONG) {
                throw new IllegalArgumentException(
                    "time series collapse expected step column of type ["
                        + ElementType.LONG
                        + "] but got ["
                        + rawStepBlock.elementType()
                        + "]"
                );
            }
            LongBlock stepBlock = (LongBlock) rawStepBlock;
            return new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    addBlock(positionOffset, groupIds);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    addBlock(positionOffset, groupIds);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (int position = 0; position < groupIds.getPositionCount(); position++) {
                        addPosition(positionOffset + position, groupIds.getInt(position), stepBlock, valueBlock);
                    }
                }

                private void addBlock(int positionOffset, IntBlock groupIds) {
                    for (int position = 0; position < groupIds.getPositionCount(); position++) {
                        if (groupIds.isNull(position)) {
                            continue;
                        }
                        int valueCount = groupIds.getValueCount(position);
                        int firstValueIndex = groupIds.getFirstValueIndex(position);
                        for (int i = 0; i < valueCount; i++) {
                            addPosition(positionOffset + position, groupIds.getInt(firstValueIndex + i), stepBlock, valueBlock);
                        }
                    }
                }

                @Override
                public void close() {}
            };
        }

        private void addPosition(int position, int groupId, LongBlock stepBlock, DoubleBlock valueBlock) {
            if (valueBlock.isNull(position)) {
                return;
            }
            if (stepBlock.isNull(position)) {
                throw new IllegalArgumentException("time series collapse found a value without a step at position [" + position + "]");
            }
            if (stepBlock.getValueCount(position) != 1) {
                throw new IllegalArgumentException(
                    "time series collapse expects a single-valued step column at position [" + position + "]"
                );
            }
            if (valueBlock.getValueCount(position) != 1) {
                throw new IllegalArgumentException(
                    "time series collapse expects a single-valued value column at position [" + position + "]"
                );
            }
            int stepOrdinal = stepOrdinal(stepBlock.getLong(stepBlock.getFirstValueIndex(position)));
            long index = index(groupId, stepOrdinal);
            values = bigArrays.grow(values, endIndexExclusive(groupId));
            values.set(index, valueBlock.getDouble(valueBlock.getFirstValueIndex(position)));
            seen.set(index);
        }

        private int stepOrdinal(long timestamp) {
            if (timestamp < start || timestamp > end) {
                throw new IllegalArgumentException(
                    "time series collapse timestamp [" + timestamp + "] is outside range [" + start + ", " + end + "]"
                );
            }
            long delta = timestamp - start;
            if (delta % step != 0) {
                // Upstream must produce timestamps on a fixed-ms UTC grid anchored at start. Calendar/timezone-aware
                // bucketing (e.g. BUCKET with a session timezone) breaks this on DST transition days, where one
                // wall-clock day is 23h or 25h in UTC ms.
                throw new IllegalArgumentException(
                    "time series collapse timestamp ["
                        + timestamp
                        + "] is not aligned to step ["
                        + step
                        + "] from start ["
                        + start
                        + "]; upstream bucket must be UTC-anchored (fixed-ms grid)"
                );
            }
            return Math.toIntExact(delta / step);
        }

        private long index(int groupId, int stepOrdinal) {
            return Math.addExact(Math.multiplyExact((long) groupId, stepCount), stepOrdinal);
        }

        private long endIndexExclusive(int groupId) {
            return Math.multiplyExact(Math.addExact((long) groupId, 1), stepCount);
        }

        private LongBlock buildSteps(IntVector selected) {
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
                builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
                forEachSelected(selected, builder, (groupId, stepOrdinal) -> builder.appendLong(timestamp(stepOrdinal)));
                return builder.build();
            }
        }

        private DoubleBlock buildValues(IntVector selected) {
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(selected.getPositionCount())) {
                forEachSelected(selected, builder, (groupId, stepOrdinal) -> builder.appendDouble(values.get(index(groupId, stepOrdinal))));
                return builder.build();
            }
        }

        private void forEachSelected(IntVector selected, Block.Builder builder, StepConsumer onStep) {
            for (int position = 0; position < selected.getPositionCount(); position++) {
                int groupId = selected.getInt(position);
                int first = -1;
                int second = -1;
                for (int stepOrdinal = 0; stepOrdinal < stepCount; stepOrdinal++) {
                    if (hasValue(groupId, stepOrdinal)) {
                        if (first < 0) {
                            first = stepOrdinal;
                        } else {
                            second = stepOrdinal;
                            break;
                        }
                    }
                }
                if (first < 0) {
                    builder.appendNull();
                } else if (second < 0) {
                    onStep.accept(groupId, first);
                } else {
                    builder.beginPositionEntry();
                    onStep.accept(groupId, first);
                    onStep.accept(groupId, second);
                    for (int stepOrdinal = second + 1; stepOrdinal < stepCount; stepOrdinal++) {
                        if (hasValue(groupId, stepOrdinal)) {
                            onStep.accept(groupId, stepOrdinal);
                        }
                    }
                    builder.endPositionEntry();
                }
            }
        }

        private boolean hasValue(int groupId, int stepOrdinal) {
            return seen.get(index(groupId, stepOrdinal));
        }

        private long timestamp(int stepOrdinal) {
            return start + stepOrdinal * step;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                Releasables.close(values, seen);
            }
        }

        @FunctionalInterface
        private interface StepConsumer {
            void accept(int groupId, int stepOrdinal);
        }
    }
}
