/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds output pages from a {@link BlockHash} and its aggregators' current state: the
 * evaluate-to-page flow shared by {@link HashAggregationOperator} and partitioned hash
 * aggregation, where each partition is an ordinary, unmodified {@code (BlockHash, aggregators)}
 * pair that reuses this exact same evaluate/selected/multi-page bookkeeping instead of
 * duplicating it.
 */
public final class GroupingAggregatorPageBuilder {
    /**
     * Hook to further restrict the group ids selected for evaluation, beyond
     * {@link BlockHash#nonEmpty()}; called once per aggregator. Must
     * {@link IntVector#incRef() incRef} and return {@code selected} unchanged if it doesn't need
     * to customize it (see {@code NO_CUSTOMIZATION}-style constants at call sites).
     */
    public interface CustomizeSelected {
        IntVector customize(GroupingAggregator aggregator, IntVector selected);
    }

    private final BlockHash blockHash;
    private final List<GroupingAggregator> aggregators;
    private final int maxPageSize;
    private final CustomizeSelected customizeSelected;

    /**
     * @param maxPageSize the result is split into multiple pages if it would otherwise exceed
     *                    this many positions
     */
    public GroupingAggregatorPageBuilder(
        BlockHash blockHash,
        List<GroupingAggregator> aggregators,
        int maxPageSize,
        CustomizeSelected customizeSelected
    ) {
        this.blockHash = blockHash;
        this.aggregators = aggregators;
        this.maxPageSize = maxPageSize;
        this.customizeSelected = customizeSelected;
    }

    /**
     * Builds an iterator of output pages reflecting the current contents of the hash/aggregators,
     * splitting into multiple pages if the result would exceed {@code maxPageSize} positions.
     * Takes ownership of {@code ctx}: it's closed before this returns if building fails, or by
     * the returned iterator's {@link ReleasableIterator#close} otherwise.
     */
    public ReleasableIterator<Page> build(GroupingAggregatorEvaluationContext ctx) {
        int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
        PreparedForEvaluation prepared = new PreparedForEvaluation(ctx);
        try {
            if (prepared.selected.keys.getPositionCount() <= maxPageSize) {
                return ReleasableIterator.single(prepared.buildPage(prepared.selected, aggBlockCounts));
            }
            ReleasableIterator<Page> result = new MultiPageResult(prepared, aggBlockCounts);
            prepared = null; // Prepared has moved into the result
            return result;
        } finally {
            Releasables.close(prepared);
        }
    }

    /**
     * Returns many pages of results from aggregations. Works by breaking chunks off
     * of the {@code selected} and {@code keys}.
     * <p>
     *     This is a step towards a system that breaks rows off of the {@link BlockHash}
     *     itself. Right now, the {@link BlockHash} implementations returns all results
     *     at once so the best we can do is break pieces off. But soon! Soon we can make
     *     them smarter.
     * </p>
     */
    private class MultiPageResult implements ReleasableIterator<Page> {
        private final PreparedForEvaluation prepared;
        private final int[] aggBlockCounts;

        private int rowOffset = 0;

        MultiPageResult(PreparedForEvaluation prepared, int[] aggBlockCounts) {
            this.prepared = prepared;
            this.aggBlockCounts = aggBlockCounts;
        }

        @Override
        public boolean hasNext() {
            return rowOffset < prepared.selected.keys.getPositionCount();
        }

        @Override
        public Page next() {
            int endOffset = Math.min(maxPageSize + rowOffset, prepared.selected.keys.getPositionCount());
            try (Selected selectedInThisPage = prepared.selected.slice(rowOffset, endOffset)) {
                Page output = prepared.buildPage(selectedInThisPage, aggBlockCounts);
                rowOffset = endOffset;
                return output;
            }
        }

        @Override
        public void close() {
            prepared.close();
        }
    }

    private class PreparedForEvaluation implements Releasable {
        private final GroupingAggregatorEvaluationContext ctx;
        private final Selected selected;
        private final List<GroupingAggregatorFunction.PreparedForEvaluation> preparedAggregators;

        private PreparedForEvaluation(GroupingAggregatorEvaluationContext ctx) {
            int count = aggregators.size();
            Selected selected = null;
            List<GroupingAggregatorFunction.PreparedForEvaluation> preparedAggregators = new ArrayList<>(count);
            boolean success = false;
            try {
                selected = new Selected(blockHash.nonEmpty(), new IntVector[count]);
                for (int a = 0; a < count; a++) {
                    selected.aggs[a] = customizeSelected.customize(aggregators.get(a), selected.keys);
                    preparedAggregators.add(aggregators.get(a).prepareForEvaluate(selected.aggs[a], ctx));
                }
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(ctx, selected, Releasables.wrap(preparedAggregators));
                }
            }
            this.ctx = ctx;
            this.selected = selected;
            this.preparedAggregators = preparedAggregators;
        }

        /**
         * Build a page or results.
         * @param selectedInPage The subset of {@link #selected} for this page. If we're
         *                       emitting a single page then this is {@code ==} to {@link #selected}.
         */
        Page buildPage(Selected selectedInPage, int[] aggBlockCounts) {
            Block[] keys = blockHash.getKeys(selectedInPage.keys);
            Block[] blocks = new Block[keys.length + Arrays.stream(aggBlockCounts).sum()];
            System.arraycopy(keys, 0, blocks, 0, keys.length);
            try {
                int blockOffset = keys.length;
                for (int i = 0; i < preparedAggregators.size(); i++) {
                    var aggregator = preparedAggregators.get(i);
                    aggregator.evaluate(blocks, blockOffset, selectedInPage.aggs[i]);
                    blockOffset += aggBlockCounts[i];
                }
                Page result = new Page(blocks);
                blocks = null;
                return result;
            } finally {
                if (blocks != null) {
                    Releasables.close(blocks);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(ctx, selected, Releasables.wrap(preparedAggregators));
        }
    }

    private record Selected(IntVector keys, IntVector[] aggs) implements Releasable {
        Selected slice(int beginInclude, int endExclusive) {
            Selected result = new Selected(keys.slice(beginInclude, endExclusive), new IntVector[aggs.length]);
            try {
                for (int a = 0; a < aggs.length; a++) {
                    result.aggs[a] = aggs[a].slice(beginInclude, endExclusive);
                }
                Selected r = result;
                result = null;
                return r;
            } finally {
                Releasables.close(result);
            }
        }

        @Override
        public void close() {
            Releasables.close(keys, Releasables.wrap(aggs));
        }
    }
}
