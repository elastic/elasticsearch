/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Source operator that incrementally counts the results in Lucene searches
 * Returns always one entry that mimics the Count aggregation internal state:
 * 1. the count as a long (0 if no doc is seen)
 * 2. a bool flag (seen) that's always true meaning that the group (all items) always exists
 */
public class LuceneCountOperator extends LuceneOperator {
    public static class Factory extends LuceneOperator.Factory {
        private final IndexedByShardId<? extends RefCounted> shardRefCounters;
        private final List<ElementType> tagTypes;

        public Factory(
            IndexedByShardId<? extends ShardContext> contexts,
            Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            List<ElementType> tagTypes,
            int limit
        ) {
            super(
                contexts,
                queryFunction,
                dataPartitioning,
                query -> LuceneSliceQueue.PartitioningStrategy.SHARD,
                taskConcurrency,
                limit,
                false,
                shardContext -> ScoreMode.COMPLETE_NO_SCORES
            );
            this.shardRefCounters = contexts;
            this.tagTypes = tagTypes;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneCountOperator(shardRefCounters, driverContext, sliceQueue, tagTypes, limit);
        }

        @Override
        public String describe() {
            return "LuceneCountOperator[dataPartitioning = " + dataPartitioning + ", limit = " + limit + "]";
        }
    }

    private final List<ElementType> tagTypes;
    private final Map<List<Object>, PerTagsState> tagsToState = new HashMap<>();
    private int remainingDocs;
    private final DriverContext driverContext;

    public LuceneCountOperator(
        IndexedByShardId<? extends RefCounted> shardRefCounters,
        DriverContext driverContext,
        LuceneSliceQueue sliceQueue,
        List<ElementType> tagTypes,
        int limit
    ) {
        super(shardRefCounters, driverContext.blockFactory(), Integer.MAX_VALUE, sliceQueue);
        this.tagTypes = tagTypes;
        this.remainingDocs = limit;
        this.driverContext = driverContext;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting || remainingDocs == 0;
    }

    @Override
    public void finish() {
        doneCollecting = true;
    }

    @Override
    protected Page getCheckedOutput() throws IOException {
        if (isFinished()) {
            assert remainingDocs <= 0 : remainingDocs;
            return null;
        }
        long start = System.nanoTime();
        try {
            while (remainingDocs > 0) {
                final LuceneScorer scorer = getCurrentOrLoadNextScorer();
                if (scorer == null) {
                    remainingDocs = 0;
                } else {
                    count(scorer);
                }

                // Check if the query has been cancelled.
                driverContext.checkForEarlyTermination();
                // Even if this should almost never happen, we want to update the driver status even when a query runs "forever".
                if (System.nanoTime() - start > Driver.DEFAULT_STATUS_INTERVAL.getNanos()) {
                    break;
                }
            }

            if (remainingDocs <= 0) {
                return buildResult();
            } else {
                return null;
            }
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    private void count(LuceneScorer scorer) throws IOException {
        PerTagsState state = tagsToState.computeIfAbsent(scorer.tags(), t -> new PerTagsState());
        Weight weight = scorer.weight();
        var leafReaderContext = scorer.leafReaderContext();
        // see org.apache.lucene.search.TotalHitCountCollector
        int leafCount = weight.count(leafReaderContext);
        if (leafCount != -1) {
            // make sure to NOT multi count as the count _shortcut_ (which is segment wide)
            // handle doc partitioning where the same leaf can be seen multiple times
            // since the count is global, consider it only for the first partition and skip the rest
            // SHARD, SEGMENT and the first DOC_ reader in data partitioning contain the first doc (position 0)
            if (scorer.position() == 0) {
                // check to not count over the desired number of docs/limit
                var count = Math.min(leafCount, remainingDocs);
                state.totalHits += count;
                remainingDocs -= count;
            }
            scorer.markAsDone();
        } else {
            // could not apply shortcut, trigger the search
            // TODO: avoid iterating all documents in multiple calls to make cancellation more responsive.
            scorer.scoreNextRange(state, leafReaderContext.reader().getLiveDocs(), remainingDocs);
        }
    }

    private Page buildResult() {
        return switch (tagsToState.size()) {
            case 0 -> null;
            case 1 -> {
                Map.Entry<List<Object>, PerTagsState> e = tagsToState.entrySet().iterator().next();
                yield buildConstantBlocksResult(e.getKey(), e.getValue());
            }
            default -> buildNonConstantBlocksResult();
        };
    }

    private Page buildConstantBlocksResult(List<Object> tags, PerTagsState state) {
        Block[] blocks = new Block[2 + tagTypes.size()];
        int b = 0;
        try {
            for (Object e : tags) { // by
                blocks[b++] = BlockUtils.constantBlock(blockFactory, e, 1);
            }
            blocks[b++] = blockFactory.newConstantLongBlockWith(state.totalHits, 1); // count
            blocks[b] = blockFactory.newConstantBooleanBlockWith(true, 1); // seen
            Page page = new Page(1, blocks);
            blocks = null;
            return page;
        } finally {
            if (blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private Page buildNonConstantBlocksResult() {
        BlockUtils.BuilderWrapper[] builders = new BlockUtils.BuilderWrapper[tagTypes.size()];
        Block[] blocks = new Block[2 + tagTypes.size()];
        try (LongVector.Builder countBuilder = blockFactory.newLongVectorBuilder(tagsToState.size())) {
            int b = 0;
            for (ElementType t : tagTypes) {
                builders[b++] = BlockUtils.wrapperFor(blockFactory, t, tagsToState.size());
            }

            for (Map.Entry<List<Object>, PerTagsState> e : tagsToState.entrySet()) {
                countBuilder.appendLong(e.getValue().totalHits);
                b = 0;
                for (Object t : e.getKey()) {
                    builders[b++].accept(t);
                }
            }

            for (b = 0; b < builders.length; b++) { // by
                blocks[b] = builders[b].builder().build();
                builders[b] = null;
            }
            blocks[b++] = countBuilder.build().asBlock(); // count
            blocks[b] = blockFactory.newConstantBooleanBlockWith(true, tagsToState.size()); // seen
            Page page = new Page(tagsToState.size(), blocks);
            blocks = null;
            return page;
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(builders), blocks == null ? () -> {} : Releasables.wrap(blocks));
        }
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", remainingDocs=").append(remainingDocs);
    }

    private class PerTagsState implements LeafCollector {
        long totalHits;

        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(DocIdStream stream) throws IOException {
            if (remainingDocs > 0) {
                int count = Math.min(stream.count(), remainingDocs);
                totalHits += count;
                remainingDocs -= count;
            }
        }

        @Override
        public void collect(int doc) {
            if (remainingDocs > 0) {
                remainingDocs--;
                totalHits++;
            }
        }
    }
}
