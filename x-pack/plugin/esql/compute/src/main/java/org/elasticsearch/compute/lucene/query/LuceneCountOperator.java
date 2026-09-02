/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

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
            int docThresholdForAutoStrategy,
            int taskConcurrency,
            List<ElementType> tagTypes,
            int limit,
            LongSupplier directoryBytesRead,
            int minDocsPerSlice,
            QueryWarnings singleValueQueryWarnings
        ) {
            super(
                contexts,
                queryFunction,
                // DOC partitioning is now safe for count — see #count(LuceneScorer) which suppresses
                // the Weight.count() shortcut for sub-segment slices. The leaf-split guard below
                // keeps shortcut-eligible leaves whole so the leaf-wide shortcut still fires.
                dataPartitioning,
                (ctx, q) -> partitioningStrategyForCount(ctx, q, minDocsPerSlice),
                docThresholdForAutoStrategy,
                taskConcurrency,
                limit,
                false,
                shardContext -> ScoreMode.COMPLETE_NO_SCORES,
                directoryBytesRead,
                minDocsPerSlice,
                LuceneCountOperator::leafHasCountShortcut,
                singleValueQueryWarnings
            );
            this.shardRefCounters = contexts;
            this.tagTypes = tagTypes;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneCountOperator(
                shardRefCounters,
                driverContext,
                sliceQueue,
                tagTypes,
                limit,
                directoryBytesRead,
                singleValueQueryWarnings
            );
        }

        @Override
        public String describe() {
            return "LuceneCountOperator[dataPartitioning = " + dataPartitioning + ", limit = " + limit + "]";
        }
    }

    /**
     * Upper bound on the number of docs scored per call to {@link #count(LuceneScorer)} when the
     * leaf-wide {@link Weight#count(LeafReaderContext)} shortcut doesn't apply. Bounds the
     * uninterrupted scoring time so the surrounding driver loop can check cancellation and refresh
     * status promptly. Matches the value used by
     * {@link LuceneTopNSourceOperator}.
     */
    private static final int NUM_DOCS_INTERVAL = 1 << 12;

    private final List<ElementType> tagTypes;
    private final Map<List<Object>, PerTagsState> tagsToState = new HashMap<>();
    private int remainingDocs;
    private final DriverContext driverContext;

    public LuceneCountOperator(
        IndexedByShardId<? extends RefCounted> shardRefCounters,
        DriverContext driverContext,
        LuceneSliceQueue sliceQueue,
        List<ElementType> tagTypes,
        int limit,
        LongSupplier directoryBytesRead,
        QueryWarnings singleValueQueryWarnings
    ) {
        super(shardRefCounters, driverContext, Integer.MAX_VALUE, sliceQueue, directoryBytesRead, singleValueQueryWarnings);
        this.tagTypes = tagTypes;
        this.remainingDocs = limit;
        this.driverContext = driverContext;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting || remainingDocs <= 0;
    }

    @Override
    public void finish() {
        doneCollecting = true;
    }

    @Override
    protected Page getCheckedOutput() throws IOException {
        long start = System.nanoTime();
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            if (scorer != null && remainingDocs > 0) {
                count(scorer);
            }
            if (isFinished()) {
                return buildResult();
            }
        } finally {
            processingNanos += System.nanoTime() - start;
        }
        return null;
    }

    private void count(LuceneScorer scorer) throws IOException {
        PerTagsState state = tagsToState.computeIfAbsent(scorer.tags(), t -> new PerTagsState());
        Weight weight = scorer.weight();
        var leafReaderContext = scorer.leafReaderContext();
        // The Weight.count(leaf) shortcut returns the leaf-wide count, which is correct only when
        // this driver owns the full leaf. For DOC-partitioned slices the shortcut would over-count:
        // every sibling driver would apply the same leaf-total to its own range. Fall through to
        // iteration in that case; BulkScorer.score respects [position, maxPosition) and is safe
        // under the Lucene query cache (cached DocIdSet iterators honor the supplied doc range).
        if (scorer.coversFullLeaf()) {
            int leafCount = weight.count(leafReaderContext);
            if (leafCount != -1) {
                var count = Math.min(leafCount, remainingDocs);
                state.totalHits += count;
                remainingDocs -= count;
                scorer.markAsDone();
                return;
            }
        }
        // Could not apply the shortcut: iterate the matching docs. Bound per-call work to
        // NUM_DOCS_INTERVAL so the outer loop can yield to the driver between chunks for
        // cancellation and status updates. The loop in #getCheckedOutput will keep calling count()
        // on the same scorer until its slice range is exhausted.
        scorer.scoreNextRange(state, leafReaderContext.reader().getLiveDocs(), Math.min(remainingDocs, NUM_DOCS_INTERVAL));
    }

    private Page buildResult() {
        try {
            return switch (tagsToState.size()) {
                case 0 -> null;
                case 1 -> {
                    Map.Entry<List<Object>, PerTagsState> e = tagsToState.entrySet().iterator().next();
                    yield buildConstantBlocksResult(e.getKey(), e.getValue());
                }
                default -> buildNonConstantBlocksResult();
            };
        } finally {
            tagsToState.clear();
        }
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

    /**
     * Auto strategy for the count operator. Shares {@link LuceneSourceOperator.Factory#autoPartitioning} with the source
     * and TopN operators: a scan-heavy count parallelizes with {@link LuceneSliceQueue.PartitioningStrategy#DOC}, a cheap
     * one ({@code cost < minCostForDoc}) stays on the low-overhead {@link LuceneSliceQueue.PartitioningStrategy#SHARD}.
     *
     * <p>A costly-to-build clause falls back to {@link LuceneSliceQueue.PartitioningStrategy#SEGMENT}: for a leaf whose
     * {@link Weight#count(LeafReaderContext)} shortcut fires the {@link #leafHasCountShortcut leaf-split guard} keeps it
     * whole anyway, and for a leaf that must be iterated SEGMENT avoids re-building the full-segment scorer per slice.
     * {@link MatchAllDocsQuery}/{@link MatchNoDocsQuery} stay on {@link LuceneSliceQueue.PartitioningStrategy#SHARD} (the
     * count is {@code maxDoc - deletedDocs} or {@code 0}, no scan needed).
     */
    static LuceneSliceQueue.PartitioningStrategy partitioningStrategyForCount(ShardContext ctx, Query q, long minCostForDoc) {
        return LuceneSourceOperator.Factory.autoPartitioning(
            ctx,
            q,
            LuceneSliceQueue.PartitioningStrategy.SHARD, // matchAll: the count is maxDoc, no scan needed
            minCostForDoc,
            LuceneSliceQueue.PartitioningStrategy.SHARD  // cheap / empty
        );
    }

    /**
     * Leaf-split guard for the count operator: when {@link Weight#count(LeafReaderContext)}
     * returns a non-negative value for a leaf, that leaf is emitted as a single full-leaf slice
     * (bypassing the DOC adaptive splitter). The driver then re-applies
     * {@code Weight.count(leaf)} via the {@link LuceneOperator.LuceneScorer#coversFullLeaf()}
     * branch in {@link #count(LuceneScorer)} — a cheap call when the shortcut is supported.
     */
    static boolean leafHasCountShortcut(Weight weight, LeafReaderContext leaf) throws IOException {
        return weight.count(leaf) != -1;
    }
}
