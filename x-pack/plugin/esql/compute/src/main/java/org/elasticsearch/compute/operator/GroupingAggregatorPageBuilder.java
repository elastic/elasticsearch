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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntUnaryOperator;

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
     * {@link BlockHash#nonEmpty()}. Called once per aggregator for {@link #build}, and once per
     * aggregator per non-empty partition for {@link #buildPartitioned}. Must
     * {@link IntVector#incRef() incRef} and return {@code selected} unchanged if no customization
     * is needed (see {@link #NO_CUSTOMIZATION}).
     */
    public interface CustomizeSelected {
        IntVector customize(GroupingAggregator aggregator, IntVector selected);
    }

    /** {@link CustomizeSelected} that does nothing: increments the ref count and returns {@code selected} unchanged. */
    public static final CustomizeSelected NO_CUSTOMIZATION = (aggregator, selected) -> {
        selected.incRef();
        return selected;
    };

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
     * Evaluates the hash table into one {@link Page} per non-empty partition, assigning each group
     * to a partition via {@code partitioner.applyAsInt(groupId)}. The returned array has exactly
     * {@code partitionCount} slots; empty partitions are {@code null}. The caller owns all returned
     * pages and must release them.
     *
     * <p>The {@code partitioner} receives the raw group ids from {@link BlockHash#nonEmpty()}, which
     * are always in ascending order. Within each partition the group ids also remain in ascending
     * order (the bucket-sort is stable), satisfying the
     * {@link org.elasticsearch.compute.aggregation.GroupingAggregatorFunction.PreparedForEvaluation}
     * contract that selected ids are ascending.
     */
    public Page[] buildPartitioned(int partitionCount, IntUnaryOperator partitioner, GroupingAggregatorEvaluationContext ctx) {
        int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
        PreparedForEvaluation prepared = new PreparedForEvaluation(ctx);
        Page[] result = new Page[partitionCount];
        boolean success = false;
        try {
            IntVector allOrdinals = prepared.selected.keys;
            int n = allOrdinals.getPositionCount();

            int[] partitionOf = new int[n];
            int[] counts = new int[partitionCount];
            for (int i = 0; i < n; i++) {
                int part = partitioner.applyAsInt(allOrdinals.getInt(i));
                partitionOf[i] = part;
                counts[part]++;
            }

            // Bucket sort by partition; stable, so ordinals within each partition stay ascending.
            int[] offsets = new int[partitionCount + 1];
            for (int p = 0; p < partitionCount; p++) {
                offsets[p + 1] = offsets[p] + counts[p];
            }
            int[] cursor = Arrays.copyOf(offsets, partitionCount);
            int[] sorted = new int[n];
            for (int i = 0; i < n; i++) {
                sorted[cursor[partitionOf[i]]++] = i;
            }

            BlockFactory bf = ctx.blockFactory();
            for (int p = 0; p < partitionCount; p++) {
                int start = offsets[p], end = offsets[p + 1];
                if (start == end) {
                    continue;
                }
                int count = end - start;
                IntVector partitionOrdinals = null;
                Selected partSelected = null;
                boolean innerSuccess = false;
                try {
                    try (var builder = bf.newIntVectorFixedBuilder(count)) {
                        for (int j = 0; j < count; j++) {
                            builder.appendInt(j, allOrdinals.getInt(sorted[start + j]));
                        }
                        partitionOrdinals = builder.build();
                    }
                    IntVector[] partitionAggs = new IntVector[aggregators.size()];
                    try {
                        for (int a = 0; a < aggregators.size(); a++) {
                            partitionAggs[a] = customizeSelected.customize(aggregators.get(a), partitionOrdinals);
                        }
                        innerSuccess = true;
                    } finally {
                        if (innerSuccess == false) {
                            Releasables.close(partitionAggs);
                        }
                    }
                    // partSelected takes ownership: keys (1 ref) + aggs (each incRef'd by customize)
                    partSelected = new Selected(partitionOrdinals, partitionAggs);
                    partitionOrdinals = null;
                    result[p] = prepared.buildPage(partSelected, aggBlockCounts);
                } finally {
                    Releasables.close(partSelected);
                    if (innerSuccess == false && partitionOrdinals != null) {
                        partitionOrdinals.close();
                    }
                }
            }
            success = true;
            return result;
        } finally {
            prepared.close();
            if (success == false) {
                for (Page page : result) {
                    if (page != null) {
                        page.releaseBlocks();
                    }
                }
            }
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
