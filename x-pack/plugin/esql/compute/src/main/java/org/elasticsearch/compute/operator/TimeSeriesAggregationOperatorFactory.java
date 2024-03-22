/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public record TimeSeriesAggregationOperatorFactory(
    AggregatorMode mode,
    int tsHashChannel,
    int timestampChannel,
    TimeValue timeSeriesPeriod,
    List<GroupingAggregator.Factory> aggregators,
    int maxPageSize
) implements Operator.OperatorFactory {

    @Override
    public String describe() {
        return null;
    }

    @Override
    public Operator get(DriverContext driverContext) {
        if (mode == AggregatorMode.FINAL) {
            var rounding = timeSeriesPeriod.equals(TimeValue.ZERO) == false ? Rounding.builder(timeSeriesPeriod).build() : null;
            BlockHash blockHash = new TimeSeriesBlockHash(tsHashChannel, timestampChannel, rounding, driverContext);
            return new HashAggregationOperator(aggregators, () -> blockHash, driverContext);
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

}
