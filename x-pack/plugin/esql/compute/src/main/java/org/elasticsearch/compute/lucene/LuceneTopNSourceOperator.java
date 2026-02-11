/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Source operator that builds Pages out of the output of a TopFieldCollector (aka TopN).
 * <p>
 *     Makes {@link Page}s of the shape {@code (docBlock)} or {@code (docBlock, score)}.
 *     Lucene loads the sort keys, but we don't read them from lucene. Yet. We should.
 * </p>
 */
public final class LuceneTopNSourceOperator extends LuceneOperator {

    public static class Factory extends LuceneOperator.Factory {
        private final IndexedByShardId<? extends ShardContext> contexts;
        private final int maxPageSize;
        private final List<SortBuilder<?>> sorts;
        private final long estimatedPerRowSortSize;

        public Factory(
            IndexedByShardId<? extends ShardContext> contexts,
            Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
            DataPartitioning dataPartitioning,
            DataPartitioning.AutoStrategy autoStrategy,
            int taskConcurrency,
            int maxPageSize,
            int limit,
            List<SortBuilder<?>> sorts,
            long estimatedPerRowSortSize,
            boolean needsScore
        ) {
            super(
                contexts,
                queryFunction,
                dataPartitioning,
                dataPartitioning == DataPartitioning.AUTO ? autoStrategy.pickStrategy(limit) : query -> {
                    throw new UnsupportedOperationException("locked in " + dataPartitioning);
                },
                taskConcurrency,
                limit,
                needsScore,
                scoreModeFunction(sorts, needsScore)
            );
            this.contexts = contexts;
            this.maxPageSize = maxPageSize;
            this.sorts = sorts;
            this.estimatedPerRowSortSize = estimatedPerRowSortSize;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneTopNSourceOperator(
                contexts,
                driverContext,
                maxPageSize,
                sorts,
                estimatedPerRowSortSize,
                limit,
                sliceQueue,
                needsScore
            );
        }

        public int maxPageSize() {
            return maxPageSize;
        }

        @Override
        public String describe() {
            String notPrettySorts = sorts.stream().map(Strings::toString).collect(Collectors.joining(","));
            return "LuceneTopNSourceOperator[dataPartitioning = "
                + dataPartitioning
                + ", maxPageSize = "
                + maxPageSize
                + ", limit = "
                + limit
                + ", needsScore = "
                + needsScore
                + ", sorts = ["
                + notPrettySorts
                + "]]";
        }
    }

    // We use the same value as the INITIAL_INTERVAL from CancellableBulkScorer
    private static final int NUM_DOCS_INTERVAL = 1 << 12;

    private final DriverContext driverContext;
    private final List<SortBuilder<?>> sorts;
    private final long estimatedPerRowSortSize;
    private final int limit;
    private final boolean needsScore;

    /**
     * Collected docs. {@code null} until we're ready to {@link #emit()}.
     */
    private ScoreDoc[] topDocs;

    /**
     * The offset in {@link #topDocs} of the next page.
     */
    private int offset = 0;

    private PerShardCollector perShardCollector;

    public LuceneTopNSourceOperator(
        IndexedByShardId<? extends ShardContext> contexts,
        DriverContext driverContext,
        int maxPageSize,
        List<SortBuilder<?>> sorts,
        long estimatedPerRowSortSize,
        int limit,
        LuceneSliceQueue sliceQueue,
        boolean needsScore
    ) {
        super(contexts, driverContext.blockFactory(), maxPageSize, sliceQueue);
        this.driverContext = driverContext;
        this.sorts = sorts;
        this.estimatedPerRowSortSize = estimatedPerRowSortSize;
        this.limit = limit;
        this.needsScore = needsScore;
        driverContext.breaker().addEstimateBytesAndMaybeBreak(reserveSize(), "esql lucene topn");
    }

    @Override
    public boolean isFinished() {
        return doneCollecting && isEmitting() == false;
    }

    @Override
    public void finish() {
        doneCollecting = true;
        topDocs = null;
        assert isFinished();
    }

    @Override
    public Page getCheckedOutput() throws IOException {
        if (isFinished()) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (isEmitting()) {
                return emit();
            } else {
                return collect();
            }
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    private Page collect() throws IOException {
        assert doneCollecting == false;
        long start = System.nanoTime();

        var scorer = getCurrentOrLoadNextScorer();

        while (scorer != null) {
            if (scorer.tags().isEmpty() == false) {
                throw new UnsupportedOperationException("tags not supported by " + getClass());
            }

            try {
                if (perShardCollector == null || perShardCollector.shardContext.index() != scorer.shardContext().index()) {
                    // TODO: share the bottom between shardCollectors
                    perShardCollector = newPerShardCollector(scorer.shardContext(), sorts, needsScore, limit);
                }
                var leafCollector = perShardCollector.getLeafCollector(scorer.leafReaderContext());
                scorer.scoreNextRange(leafCollector, scorer.leafReaderContext().reader().getLiveDocs(), NUM_DOCS_INTERVAL);
            } catch (CollectionTerminatedException cte) {
                // Lucene terminated early the collection (doing topN for an index that's sorted and the topN uses the same sorting)
                scorer.markAsDone();
            }

            // check if the query has been cancelled.
            driverContext.checkForEarlyTermination();

            if (scorer.isDone()) {
                var nextScorer = getCurrentOrLoadNextScorer();
                if (nextScorer != null && nextScorer.shardContext().index() != scorer.shardContext().index()) {
                    startEmitting();
                    return emit();
                }
                scorer = nextScorer;
            }

            // When it takes a long time to start emitting pages we need to return back to the driver so we can update its status.
            // Even if this should almost never happen, we want to update the driver status even when a query runs "forever".
            if (System.nanoTime() - start > Driver.DEFAULT_STATUS_INTERVAL.getNanos()) {
                return null;
            }
        }

        doneCollecting = true;
        startEmitting();
        return emit();
    }

    private boolean isEmitting() {
        return topDocs != null;
    }

    private void startEmitting() {
        assert isEmitting() == false : "offset=" + offset + " score_docs=" + Arrays.toString(topDocs);
        offset = 0;
        if (perShardCollector != null) {
            /*
             * Important note for anyone who looks at this and has bright ideas:
             * There *is* a method in lucene to return topDocs with an offset
             * and a limit. So you'd *think* you can scroll the top docs there.
             * But you can't. It's expressly forbidden to call any of the `topDocs`
             * methods more than once. You *must* call `topDocs` once and use the
             * array.
             */
            topDocs = perShardCollector.collector.topDocs().scoreDocs;
            int shardId = perShardCollector.shardContext.index();
        } else {
            topDocs = new ScoreDoc[0];
        }
    }

    private void stopEmitting() {
        topDocs = null;
    }

    private Page emit() {
        if (offset >= topDocs.length) {
            stopEmitting();
            return null;
        }
        int size = Math.min(maxPageSize, topDocs.length - offset);
        IntVector shard = null;
        IntVector segments = null;
        IntVector docs = null;
        DocBlock docBlock = null;
        DoubleBlock scores = null;
        Page page = null;
        try (
            IntVector.Builder currentSegmentBuilder = blockFactory.newIntVectorFixedBuilder(size);
            IntVector.Builder currentDocsBuilder = blockFactory.newIntVectorFixedBuilder(size);
            DoubleVector.Builder currentScoresBuilder = scoreVectorOrNull(size);
        ) {
            int start = offset;
            offset += size;
            ShardContext shardContext = perShardCollector.shardContext;
            List<LeafReaderContext> leafContexts = shardContext.searcher().getLeafContexts();
            for (int i = start; i < offset; i++) {
                int doc = topDocs[i].doc;
                int segment = ReaderUtil.subIndex(doc, leafContexts);
                currentSegmentBuilder.appendInt(segment);
                currentDocsBuilder.appendInt(doc - leafContexts.get(segment).docBase); // the offset inside the segment
                if (currentScoresBuilder != null) {
                    float score = getScore(topDocs[i]);
                    currentScoresBuilder.appendDouble(score);
                }
                // Null the top doc so it can be GCed early, just in case.
                topDocs[i] = null;
            }

            int shardId = shardContext.index();
            shard = blockFactory.newConstantIntBlockWith(shardId, size).asVector();
            segments = currentSegmentBuilder.build();
            docs = currentDocsBuilder.build();
            docBlock = new DocVector(refCounteds, shard, segments, docs, DocVector.config()).asBlock();
            shard = null;
            segments = null;
            docs = null;
            if (currentScoresBuilder == null) {
                page = new Page(size, docBlock);
            } else {
                scores = currentScoresBuilder.build().asBlock();
                page = new Page(size, docBlock, scores);
            }
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(shard, segments, docs, docBlock, scores);
            }
        }
        return page;
    }

    private float getScore(ScoreDoc scoreDoc) {
        if (scoreDoc instanceof FieldDoc fieldDoc) {
            if (Float.isNaN(fieldDoc.score)) {
                if (sorts != null) {
                    return (Float) fieldDoc.fields[sorts.size() + 1];
                } else {
                    return (Float) fieldDoc.fields[0];
                }
            } else {
                return fieldDoc.score;
            }
        } else {
            return scoreDoc.score;
        }
    }

    private DoubleVector.Builder scoreVectorOrNull(int size) {
        if (needsScore) {
            return blockFactory.newDoubleVectorFixedBuilder(size);
        } else {
            return null;
        }
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", limit = ").append(limit);
        sb.append(", needsScore = ").append(needsScore);
        String notPrettySorts = sorts.stream().map(Strings::toString).collect(Collectors.joining(","));
        sb.append(", sorts = [").append(notPrettySorts).append("]");
    }

    @Override
    protected void additionalClose() {
        Releasables.close(() -> driverContext.breaker().addWithoutBreaking(-reserveSize()));
    }

    private long reserveSize() {
        long perRowSize = FIELD_DOC_SIZE + estimatedPerRowSortSize;
        return limit * perRowSize;
    }

    abstract static class PerShardCollector {
        private final ShardContext shardContext;
        private final TopDocsCollector<?> collector;

        private int leafIndex;
        private LeafCollector leafCollector;
        private Thread currentThread;

        PerShardCollector(ShardContext shardContext, TopDocsCollector<?> collector) {
            this.shardContext = shardContext;
            this.collector = collector;
        }

        LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
            if (currentThread != Thread.currentThread() || leafIndex != leafReaderContext.ord) {
                leafCollector = collector.getLeafCollector(leafReaderContext);
                leafIndex = leafReaderContext.ord;
                currentThread = Thread.currentThread();
            }
            return leafCollector;
        }
    }

    static final class NonScoringPerShardCollector extends PerShardCollector {
        NonScoringPerShardCollector(ShardContext shardContext, Sort sort, int limit) {
            // We don't use CollectorManager here as we don't retrieve the total hits and sort by score.
            super(shardContext, new TopFieldCollectorManager(sort, limit, null, 0).newCollector());
        }
    }

    static final class ScoringPerShardCollector extends PerShardCollector {
        ScoringPerShardCollector(ShardContext shardContext, TopDocsCollector<?> topDocsCollector) {
            super(shardContext, topDocsCollector);
        }
    }

    private static Function<ShardContext, ScoreMode> scoreModeFunction(List<SortBuilder<?>> sorts, boolean needsScore) {
        return ctx -> {
            try {
                // we create a collector with a limit of 1 to determine the appropriate score mode to use.
                return newPerShardCollector(ctx, sorts, needsScore, 1).collector.scoreMode();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static PerShardCollector newPerShardCollector(ShardContext context, List<SortBuilder<?>> sorts, boolean needsScore, int limit)
        throws IOException {
        Optional<SortAndFormats> sortAndFormats = context.buildSort(sorts);
        if (sortAndFormats.isEmpty()) {
            throw new IllegalStateException("sorts must not be disabled in TopN");
        }
        if (needsScore == false) {
            return new NonScoringPerShardCollector(context, sortAndFormats.get().sort, limit);
        }
        Sort sort = sortAndFormats.get().sort;
        if (Sort.RELEVANCE.equals(sort)) {
            // SORT _score DESC
            return new ScoringPerShardCollector(context, new TopScoreDocCollectorManager(limit, null, 0).newCollector());
        }

        // SORT ..., _score, ...
        var l = new ArrayList<>(Arrays.asList(sort.getSort()));
        l.add(SortField.FIELD_DOC);
        l.add(SortField.FIELD_SCORE);
        sort = new Sort(l.toArray(SortField[]::new));
        return new ScoringPerShardCollector(context, new TopFieldCollectorManager(sort, limit, null, 0).newCollector());
    }

    private static final int FIELD_DOC_SIZE = Math.toIntExact(RamUsageEstimator.shallowSizeOf(FieldDoc.class));
}
