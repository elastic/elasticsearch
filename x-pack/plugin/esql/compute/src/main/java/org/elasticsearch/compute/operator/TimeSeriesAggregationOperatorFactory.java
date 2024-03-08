/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public record TimeSeriesAggregationOperatorFactory(
    AggregatorMode mode,
    int tsHashChannel,
    int timestampChannel,
    TimeValue timeSeriesPeriod,
    List<GroupingAggregator.Factory> aggregators
) implements Operator.OperatorFactory {

    @Override
    public String describe() {
        return null;
    }

    @Override
    public Operator get(DriverContext driverContext) {
        if (mode == AggregatorMode.FINAL) {
            var rounding = timeSeriesPeriod.equals(TimeValue.ZERO) == false ? Rounding.builder(timeSeriesPeriod).build() : null;
            return new DelayedImpl(tsHashChannel, timestampChannel, rounding, driverContext, aggregators);
        } else {
            return new LocalImpl(driverContext, aggregators);
        }
    }

    // TODO: implement local time series grouping
    // For now just do some re-ordering and remove doc block.
    static class LocalImpl extends AbstractPageMappingOperator {

        private final DriverContext driverContext;
        private final List<GroupingAggregator> aggregators;

        LocalImpl(DriverContext driverContext, List<GroupingAggregator.Factory> aggregatorFactories) {
            this.driverContext = driverContext;
            this.aggregators = new ArrayList<>(aggregatorFactories.size());
            for (GroupingAggregator.Factory factory : aggregatorFactories) {
                this.aggregators.add(factory.apply(driverContext));
            }
        }

        @Override
        protected Page process(Page page) {
            // This is not very robust:
            // Build on the assumption

            // Remove doc block and append tsid and timestamp at the end:
            List<Block> blocks = new ArrayList<>();
            int pageOffset = 3;
            for (GroupingAggregator aggregator : aggregators) {
                if (aggregator.evaluateBlockCount() == 1) {
                    blocks.add(page.getBlock(pageOffset++));
                } else {
                    Block[] tmpBlocks = new Block[aggregator.evaluateBlockCount()];
                    aggregator.evaluate(
                        tmpBlocks,
                        0,
                        IntVector.range(0, page.getPositionCount(), driverContext.blockFactory()),
                        driverContext
                    );
                    // The first block is values block, but that is empty. So replace that we the block that value source operator created:
                    blocks.add(page.getBlock(pageOffset++));
                    blocks.addAll(Arrays.asList(tmpBlocks).subList(1, tmpBlocks.length));
                }
            }
            // Works too, but feels even less robust...
            // for (int i = 3; i < page.getBlockCount(); i++) {
            // blocks.add(page.getBlock(i));
            // blocks.add(driverContext.blockFactory().newConstantBooleanBlockWith(true, page.getPositionCount()));
            // }

            // Append tsid and timestamo at the end:
            // (Only DelayedImpl will know that it exists)
            blocks.add(page.getBlock(1));
            blocks.add(page.getBlock(2));

            // Close and leave doc block behind:
            DocBlock docBlock = page.getBlock(0);
            docBlock.close();

            return new Page(blocks.toArray(new Block[0]));
        }

        @Override
        public String toString() {
            return "local time series grouping operator";
        }
    }

    /**
     * Groups in a delayed fashion by tsid hash and timestamp interval.
     *
     * Currently, all time serie and timestamp interval tuples are grouped in a delayed fashion.
     * Follow up changes should improve this and only perform delayed grouping for time serie and timestamp tuple this is really necessary.
     * In most cases grouping should have been performed by {@link LocalImpl} and
     * only for time series and timestamp interval tuple at backing index boundary delayed grouping is required.
     */
    static class DelayedImpl implements Operator {

        private final int tsHashChannel;
        private final int timestampChannel;
        private final BytesRefHash tsHashes;
        private final LongLongHash timestampHash;
        private final Rounding.Prepared preparedRounding;
        private final DriverContext driverContext;
        private final List<GroupingAggregator> aggregators;

        private Page output;
        private boolean finished;

        DelayedImpl(
            int tsHashChannel,
            int timestampChannel,
            Rounding rounding,
            DriverContext driverContext,
            List<GroupingAggregator.Factory> aggregatorFactories
        ) {
            this.tsHashChannel = tsHashChannel;
            this.timestampChannel = timestampChannel;
            this.tsHashes = new BytesRefHash(1, driverContext.bigArrays());
            if (rounding != null) {
                this.timestampHash = new LongLongHash(1, driverContext.bigArrays());
                this.preparedRounding = rounding.prepareForUnknown();
            } else {
                this.timestampHash = null;
                this.preparedRounding = null;
            }
            this.driverContext = driverContext;
            this.aggregators = new ArrayList<>(aggregatorFactories.size());
            for (GroupingAggregator.Factory factory : aggregatorFactories) {
                this.aggregators.add(factory.apply(driverContext));
            }
        }

        @Override
        public boolean needsInput() {
            return finished == false;
        }

        @Override
        public void addInput(Page page) {
            try {
                BytesRefBlock tsHashBlock = page.getBlock(tsHashChannel);
                LongBlock timestampIntervalBlock = page.getBlock(timestampChannel);

                SeenGroupIds seenGroupIds = new SeenGroupIds.Range(0, 0);
                var prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
                for (int i = 0; i < prepared.length; i++) {
                    prepared[i] = aggregators.get(i).prepareProcessPage(seenGroupIds, page);
                }

                BytesRefVector tsHashVector = tsHashBlock.asVector();
                if (tsHashVector != null) {
                    vectorAddInput(prepared, tsHashVector, timestampIntervalBlock.asVector());
                } else {
                    blockAddInput(prepared, tsHashBlock, timestampIntervalBlock);
                }
            } finally {
                page.releaseBlocks();
            }
        }

        private void blockAddInput(
            GroupingAggregatorFunction.AddInput[] prepared,
            BytesRefBlock tsHashBlock,
            LongBlock timestampIntervalBlock
        ) {
            try (var builder = driverContext.blockFactory().newIntVectorBuilder(tsHashBlock.getPositionCount())) {
                BytesRef spare = new BytesRef();
                if (preparedRounding != null) {
                    for (int i = 0; i < tsHashBlock.getPositionCount(); i++) {
                        BytesRef tsHash = tsHashBlock.getBytesRef(i, spare);
                        long groupId = tsHashes.add(tsHash);
                        if (groupId < 0) {
                            groupId = -1 - groupId;
                        }
                        long timestampInterval = preparedRounding.round(timestampIntervalBlock.getLong(i));
                        groupId = timestampHash.add(groupId, timestampInterval);
                        if (groupId < 0) {
                            groupId = -1 - groupId;
                        }
                        builder.appendInt(Math.toIntExact(groupId));
                    }
                } else {
                    for (int i = 0; i < tsHashBlock.getPositionCount(); i++) {
                        BytesRef tsHash = tsHashBlock.getBytesRef(i, spare);
                        long groupId = tsHashes.add(tsHash);
                        if (groupId < 0) {
                            groupId = -1 - groupId;
                        }
                        builder.appendInt(Math.toIntExact(groupId));
                    }
                }
                try (var vector = builder.build()) {
                    for (var addInput : prepared) {
                        addInput.add(0, vector);
                    }
                }
            }
        }

        private void vectorAddInput(
            GroupingAggregatorFunction.AddInput[] prepared,
            BytesRefVector tsHashVector,
            LongVector timestampIntervalVector
        ) {
            try (var builder = driverContext.blockFactory().newIntVectorBuilder(tsHashVector.getPositionCount())) {
                BytesRef spare = new BytesRef();
                if (preparedRounding != null) {
                    for (int i = 0; i < tsHashVector.getPositionCount(); i++) {
                        BytesRef tsHash = tsHashVector.getBytesRef(i, spare);
                        long groupId = tsHashes.add(tsHash);
                        if (groupId < 0) {
                            groupId = -1 - groupId;
                        }
                        long timestampInterval = preparedRounding.round(timestampIntervalVector.getLong(i));
                        groupId = timestampHash.add(groupId, timestampInterval);
                        if (groupId < 0) {
                            groupId = -1 - groupId;
                        }
                        builder.appendInt(Math.toIntExact(groupId));
                    }
                } else {
                    for (int i = 0; i < tsHashVector.getPositionCount(); i++) {
                        BytesRef tsHash = tsHashVector.getBytesRef(i, spare);
                        long groupId = tsHashes.add(tsHash);
                        if (groupId < 0) {
                            groupId = -1 - groupId;
                        }
                        builder.appendInt(Math.toIntExact(groupId));
                    }
                }
                try (var vector = builder.build()) {
                    for (var addInput : prepared) {
                        addInput.add(0, vector);
                    }
                }
            }
        }

        @Override
        public void finish() {
            if (finished) {
                return;
            }
            finished = true;
            Block[] blocks = null;
            IntVector selected = null;
            boolean success = false;
            try {
                selected = IntVector.range(0, Math.toIntExact(tsHashes.size()) + 1, driverContext.blockFactory());

                int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
                blocks = new Block[Arrays.stream(aggBlockCounts).sum()];

                // add block for unique tsid hashes
                // final int size = Math.toIntExact(tsHashes.size());
                // try (BytesStreamOutput out = new BytesStreamOutput()) {
                // tsHashes.getBytesRefs().writeTo(out);
                // try (StreamInput in = out.bytes().streamInput()) {
                // blocks[0] = driverContext.blockFactory()
                // .newBytesRefArrayVector(new BytesRefArray(in, BigArrays.NON_RECYCLING_INSTANCE), size)
                // .asBlock();
                // }
                // } catch (IOException e) {
                // throw new IllegalStateException(e);
                // }

                // Append the aggregator blocks
                int offset = 0;
                for (int i = 0; i < aggregators.size(); i++) {
                    var aggregator = aggregators.get(i);
                    aggregator.evaluate(blocks, offset, selected, driverContext);
                    offset += aggBlockCounts[i];
                }
                output = new Page(blocks);
                success = true;
            } finally {
                // selected should always be closed
                if (selected != null) {
                    selected.close();
                }
                if (success == false && blocks != null) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
        }

        @Override
        public boolean isFinished() {
            return finished && output == null;
        }

        @Override
        public Page getOutput() {
            var output = this.output;
            this.output = null;
            return output;
        }

        @Override
        public void close() {
            if (output != null) {
                output.releaseBlocks();
            }
            Releasables.close(tsHashes, timestampHash, () -> Releasables.close(aggregators));
        }
    }
}
