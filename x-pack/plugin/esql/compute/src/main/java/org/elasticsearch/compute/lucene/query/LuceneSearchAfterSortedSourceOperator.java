/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;

/**
 * Source operator that pages through sorted docs using Lucene's {@link TopFieldCollectorManager}
 * with the {@code searchAfter} pattern: each call fetches up to {@code maxPageSize} docs starting
 * after the last doc returned by the previous call.
 *
 * <p>Memory usage is {@code O(maxPageSize)} per call, regardless of shard or index size. For
 * indices pre-sorted by the query sort field each page requires only a short scan; for unsorted
 * indices each page triggers a full index scan, but the downstream pipeline typically terminates
 * early (e.g. via a {@code LIMIT}) so the total work remains bounded.
 */
public final class LuceneSearchAfterSortedSourceOperator extends LuceneOperator {

    public static class Factory extends LuceneOperator.Factory {
        private final IndexedByShardId<? extends ShardContext> contexts;
        private final int maxPageSize;
        private final List<SortBuilder<?>> sorts;

        public Factory(
            IndexedByShardId<? extends ShardContext> contexts,
            java.util.function.Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
            int taskConcurrency,
            int maxPageSize,
            int limit,
            List<SortBuilder<?>> sorts,
            LongSupplier directoryBytesRead
        ) {
            super(
                contexts,
                queryFunction,
                DataPartitioning.SHARD,
                query -> { throw new UnsupportedOperationException("locked in SHARD partitioning"); },
                LuceneOperator.SMALL_INDEX_BOUNDARY,
                taskConcurrency,
                limit,
                false,
                shardContext -> COMPLETE_NO_SCORES,
                directoryBytesRead
            );
            this.contexts = contexts;
            this.maxPageSize = maxPageSize;
            this.sorts = sorts;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneSearchAfterSortedSourceOperator(
                contexts,
                driverContext.blockFactory(),
                maxPageSize,
                limit,
                sorts,
                sliceQueue,
                directoryBytesRead
            );
        }

        public int maxPageSize() {
            return maxPageSize;
        }

        @Override
        public String describe() {
            String notPrettySorts = sorts.stream().map(Strings::toString).collect(Collectors.joining(","));
            return "LuceneSearchAfterSortedSourceOperator[dataPartitioning = "
                + dataPartitioning
                + ", maxPageSize = "
                + maxPageSize
                + ", limit = "
                + limit
                + ", sorts = ["
                + notPrettySorts
                + "]]";
        }
    }

    private final int limit;
    private final List<SortBuilder<?>> sorts;

    /**
     * Scorer for the first leaf of the current shard's slice. Held so we can call
     * {@link LuceneScorer#markAsDone()} and force advancement to the next shard when done,
     * and to obtain the shard's rewritten {@link org.apache.lucene.search.Query} via its weight.
     */
    private LuceneScorer shardScorer;

    /** Context for the shard currently being paged through; null between shards. */
    private ShardContext currentShardContext;
    private Sort currentSort;
    private int currentShardId;

    /** Last {@link FieldDoc} returned; used as the {@code after} parameter for the next search. Null at the start of a shard. */
    private FieldDoc lastFieldDoc;

    /** Number of docs emitted for the current shard so far. */
    private int docsEmitted;

    public LuceneSearchAfterSortedSourceOperator(
        IndexedByShardId<? extends ShardContext> contexts,
        BlockFactory blockFactory,
        int maxPageSize,
        int limit,
        List<SortBuilder<?>> sorts,
        LuceneSliceQueue sliceQueue,
        LongSupplier directoryBytesRead
    ) {
        super(contexts, blockFactory, maxPageSize, sliceQueue, directoryBytesRead);
        this.limit = limit;
        this.sorts = sorts;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting && currentShardContext == null;
    }

    @Override
    public void finish() {
        doneCollecting = true;
        currentShardContext = null;
    }

    @Override
    protected Page getCheckedOutput() throws IOException {
        if (isFinished()) {
            return null;
        }
        long start = System.nanoTime();
        try {
            return fetchPage();
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    private Page fetchPage() throws IOException {
        if (currentShardContext == null) {
            LuceneScorer scorer = getCurrentOrLoadNextScorer();
            if (scorer == null) {
                return null;
            }
            Optional<SortAndFormats> sortAndFormats = scorer.shardContext().buildSort(sorts);
            if (sortAndFormats.isEmpty()) {
                throw new IllegalStateException("sorts must not be disabled in LuceneSearchAfterSortedSourceOperator");
            }
            shardScorer = scorer;
            currentShardContext = scorer.shardContext();
            currentShardId = currentShardContext.index();
            currentSort = sortAndFormats.get().sort;
            lastFieldDoc = null;
            docsEmitted = 0;
        }

        int batchSize = Math.min(maxPageSize, limit - docsEmitted);
        if (batchSize <= 0) {
            advanceToNextShard();
            return null;
        }

        // Integer.MAX_VALUE as totalHitsThreshold disables exact total-hit counting (not needed here).
        TopFieldCollectorManager manager = new TopFieldCollectorManager(currentSort, batchSize, lastFieldDoc, Integer.MAX_VALUE);
        TopFieldDocs results = (TopFieldDocs) currentShardContext.searcher().search(shardScorer.weight().getQuery(), manager);

        if (results.scoreDocs.length == 0) {
            advanceToNextShard();
            return null;
        }

        lastFieldDoc = (FieldDoc) results.scoreDocs[results.scoreDocs.length - 1];
        docsEmitted += results.scoreDocs.length;

        Page page = buildPage(currentShardId, currentShardContext, results.scoreDocs);

        if (docsEmitted >= limit) {
            advanceToNextShard();
        }

        return page;
    }

    /**
     * Marks the current shard as complete and resets state so the next {@link #fetchPage()} call
     * loads the following shard. Setting {@link #currentSlice} to {@code null} causes
     * {@link #getCurrentOrLoadNextScorer()} to call {@code sliceQueue.nextSlice(null)}, advancing
     * to the next shard's slice rather than continuing within the current one.
     */
    private void advanceToNextShard() {
        if (shardScorer != null) {
            shardScorer.markAsDone();
            shardScorer = null;
        }
        currentSlice = null;
        currentShardContext = null;
    }

    private Page buildPage(int shardId, ShardContext shardContext, ScoreDoc[] scoreDocs) {
        List<LeafReaderContext> leaves = shardContext.searcher().getTopReaderContext().leaves();
        IntVector shard = null;
        IntVector segments = null;
        IntVector docs = null;
        DocBlock docBlock = null;
        Page page = null;
        try (
            IntVector.Builder segmentBuilder = blockFactory.newIntVectorBuilder(scoreDocs.length);
            IntVector.Builder docBuilder = blockFactory.newIntVectorBuilder(scoreDocs.length)
        ) {
            for (ScoreDoc sd : scoreDocs) {
                int leafIdx = ReaderUtil.subIndex(sd.doc, leaves);
                segmentBuilder.appendInt(leafIdx);
                docBuilder.appendInt(sd.doc - leaves.get(leafIdx).docBase);
            }

            shardRowsEmitted[shardId] += scoreDocs.length;
            shard = blockFactory.newConstantIntBlockWith(shardId, scoreDocs.length).asVector();
            segments = segmentBuilder.build();
            docs = docBuilder.build();
            docBlock = new DocVector(refCounteds, shard, segments, docs, DocVector.config()).asBlock();
            shard = null;
            segments = null;
            docs = null;
            page = new Page(scoreDocs.length, docBlock);
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(shard, segments, docs, docBlock);
            }
        }
        return page;
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", limit = ").append(limit);
        String notPrettySorts = sorts.stream().map(Strings::toString).collect(Collectors.joining(","));
        sb.append(", sorts = [").append(notPrettySorts).append("]");
    }
}
