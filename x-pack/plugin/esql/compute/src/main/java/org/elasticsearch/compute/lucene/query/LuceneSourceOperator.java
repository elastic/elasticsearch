/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Limiter;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.apache.lucene.search.ScoreMode.COMPLETE;
import static org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.DOC;
import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.SEGMENT;
import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.SHARD;

/**
 * Source operator that incrementally runs Lucene searches
 */
public class LuceneSourceOperator extends LuceneOperator {
    private static final Logger log = LogManager.getLogger(LuceneSourceOperator.class);

    private int remainingDocs;
    private final Limiter limiter;

    private final IntArrayPool docIdsPool;
    private int[] docIds;
    private int currentPagePos = 0;
    private DoubleVector.Builder scoreBuilder;
    private final LeafCollector leafCollector;
    private final int minPageSize;

    public static class Factory extends LuceneOperator.Factory {
        protected final IndexedByShardId<? extends RefCounted> refCounteds;
        protected final int maxPageSize;
        protected final Limiter limiter;

        public Factory(
            IndexedByShardId<? extends ShardContext> shardContexts,
            Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
            DataPartitioning dataPartitioning,
            DataPartitioning.AutoStrategy autoStrategy,
            int docThresholdForAutoStrategy,
            int taskConcurrency,
            int maxPageSize,
            int limit,
            boolean needsScore,
            LongSupplier directoryBytesRead,
            int minDocsPerSlice,
            QueryWarnings singleValueQueryWarnings
        ) {
            super(
                shardContexts,
                queryFunction,
                dataPartitioning,
                dataPartitioning == DataPartitioning.AUTO ? autoStrategy.pickStrategy(limit) : (ctx, q) -> {
                    throw new UnsupportedOperationException("locked in " + dataPartitioning);
                },
                docThresholdForAutoStrategy,
                taskConcurrency,
                limit,
                needsScore,
                shardContext -> needsScore ? COMPLETE : COMPLETE_NO_SCORES,
                directoryBytesRead,
                minDocsPerSlice,
                singleValueQueryWarnings
            );
            this.refCounteds = shardContexts;
            this.maxPageSize = maxPageSize;
            // TODO: use a single limiter for multiple stage execution
            this.limiter = limit == NO_LIMIT ? Limiter.NO_LIMIT : new Limiter(limit);
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneSourceOperator(
                refCounteds,
                driverContext,
                maxPageSize,
                sliceQueue,
                limit,
                limiter,
                needsScore,
                directoryBytesRead,
                singleValueQueryWarnings
            );
        }

        public int maxPageSize() {
            return maxPageSize;
        }

        @Override
        public String describe() {
            return "LuceneSourceOperator[dataPartitioning = "
                + dataPartitioning
                + ", maxPageSize = "
                + maxPageSize
                + ", limit = "
                + limit
                + ", needsScore = "
                + needsScore
                + "]";
        }

        /**
         * The {@link DataPartitioning.AutoStrategy#DEFAULT default} strategy, without a cost threshold: a no-limit scan
         * parallelizes with {@link PartitioningStrategy#DOC} unless costly, a limited scan keeps the low-overhead
         * {@link PartitioningStrategy#SHARD}. The production non-time-series source uses {@link #autoStrategy(long)}
         * instead, which also folds cheap no-limit scans onto {@link PartitioningStrategy#SEGMENT}.
         */
        public static BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy(int limit) {
            return limit == NO_LIMIT ? Factory::highSpeedAutoStrategy : Factory::lowOverheadAutoStrategy;
        }

        /**
         * Cost-aware {@link DataPartitioning#AUTO} strategy for the unsorted source. A no-limit scan (e.g. {@code STATS})
         * visits every matching doc, so it shares the {@link #autoPartitioning} rule with count and TopN: costly →
         * SEGMENT, cheap ({@code cost < minCostForDoc}) → SEGMENT, else DOC. A query with a {@code limit} early-terminates
         * after matching {@code N} docs — whether DOC pays off then depends on match density, which the cost can't see —
         * so that case is deferred and keeps the low-overhead {@link PartitioningStrategy#SHARD}.
         */
        public static DataPartitioning.AutoStrategy autoStrategy(long minCostForDoc) {
            return limit -> limit == NO_LIMIT
                ? (ctx, query) -> autoPartitioning(ctx, query, DOC, minCostForDoc, SEGMENT)
                : Factory::lowOverheadAutoStrategy;
        }

        /**
         * Use the {@link PartitioningStrategy#SHARD} strategy because
         * it has the lowest overhead. Used when there is a {@code limit} on the operator
         * because that's for cases like {@code FROM foo | LIMIT 10} or
         * {@code FROM foo | WHERE a == 1 | LIMIT 10} when the {@code WHERE} can be pushed
         * to Lucene. In those cases we're better off with the lowest overhead we can
         * manage - and that's {@link PartitioningStrategy#SHARD}.
         */
        private static PartitioningStrategy lowOverheadAutoStrategy(ShardContext ctx, Query query) {
            return SHARD;
        }

        /**
         * The no-limit branch of {@link #autoStrategy(int)}: parallelize the full scan with {@link PartitioningStrategy#DOC}
         * unless a clause is costly to build ({@link PartitioningStrategy#SEGMENT}); {@link MatchAllDocsQuery} → DOC,
         * {@link MatchNoDocsQuery} → SHARD. Unlike {@link #autoPartitioning} this applies no cost threshold.
         */
        private static PartitioningStrategy highSpeedAutoStrategy(ShardContext ctx, Query query) {
            Query unwrapped = unwrapQuery(query);
            if (unwrapped instanceof MatchAllDocsQuery) {
                return DOC;
            }
            if (unwrapped instanceof MatchNoDocsQuery) {
                return SHARD;
            }
            return containsCostlyClause(query) ? SEGMENT : DOC;
        }

        /**
         * Shared {@link DataPartitioning#AUTO} decision for the "cheap scorer, parallelize the scan" operators that visit
         * every matching doc: the no-limit unsorted source ({@code STATS}), the count ({@link LuceneCountOperator}) and
         * the field-sorted TopN ({@link LuceneTopNSourceOperator}). They all protect against the same two things:
         * <ul>
         *     <li>a costly-to-build clause (point range, multi-term) whose full-segment scorer a sub-segment DOC slice
         *         would rebuild → {@link PartitioningStrategy#SEGMENT};</li>
         *     <li>a query too cheap ({@code cost < minCostForDoc}) to amortize DOC's per-slice overhead → {@code cheap}.</li>
         * </ul>
         * Otherwise the scan is heavy enough to parallelize → {@link PartitioningStrategy#DOC}. The source's implicit-limit
         * case is <em>not</em> routed here: it early-terminates after {@code N} matches, so the cost isn't the right signal.
         *
         * @param matchAll outcome for a root {@link MatchAllDocsQuery}: SHARD when the match set needs no scan (count is
         *                 {@code maxDoc}), DOC when it must be fully scanned (a field sort, or a no-limit scan)
         * @param cheap    outcome below {@code minCostForDoc} (and for an empty {@link MatchNoDocsQuery})
         */
        static PartitioningStrategy autoPartitioning(
            ShardContext ctx,
            Query query,
            PartitioningStrategy matchAll,
            long minCostForDoc,
            PartitioningStrategy cheap
        ) {
            Query unwrapped = unwrapQuery(query);
            if (unwrapped instanceof MatchAllDocsQuery) {
                return matchAll;
            }
            if (unwrapped instanceof MatchNoDocsQuery) {
                return cheap;
            }
            if (containsCostlyClause(query)) {
                return SEGMENT;
            }
            try {
                return queryCost(ctx, query, minCostForDoc) < minCostForDoc ? cheap : DOC;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /**
         * Sum of the per-leaf {@link org.apache.lucene.search.ScorerSupplier#cost()} for {@code query}, short-circuiting
         * once it reaches {@code ceiling} (callers only need to know whether it clears the threshold). Tells a scan-heavy
         * query — a doc-values-only filter reports ~{@code maxDoc} — from a cheap indexed lookup that skips to its matches.
         */
        static long queryCost(ShardContext ctx, Query query, long ceiling) throws IOException {
            // Use an uncached weight so this cost probe doesn't count the query against the caching policy.
            // Query#createWeight requires a rewritten query (the slice queue rewrites before picking).
            assert query == ctx.searcher().rewrite(query) : "query must be rewritten before createWeight: " + query;
            Weight weight = query.createWeight(ctx.searcher(), COMPLETE_NO_SCORES, 1f);
            long cost = 0;
            for (LeafReaderContext leaf : ctx.searcher().getIndexReader().leaves()) {
                var scorerSupplier = weight.scorerSupplier(leaf);
                if (scorerSupplier != null) {
                    cost += scorerSupplier.cost();
                    if (cost >= ceiling) {
                        break;
                    }
                }
            }
            return cost;
        }

        /**
         * Walk the full query tree and return {@code true} if any sub-query is costly to build a
         * scorer for. Uses a {@link QueryVisitor}, so this handles arbitrary nesting:
         * {@code BooleanQuery} (incl. {@code MUST_NOT} clauses), {@code IndexOrDocValuesQuery},
         * {@code DisjunctionMaxQuery}, and any other compound structure that Lucene knows how to
         * walk via {@link Query#visit}.
         */
        static boolean containsCostlyClause(Query query) {
            boolean[] found = { false };
            query.visit(new QueryVisitor() {
                @Override
                public void consumeTerms(Query q, Term... terms) {
                    if (isCostlyToBuildScorer(q)) {
                        found[0] = true;
                    }
                }

                @Override
                public void consumeTermsMatching(Query q, String field, Supplier<ByteRunAutomaton> automaton) {
                    if (isCostlyToBuildScorer(q)) {
                        found[0] = true;
                    }
                }

                @Override
                public void visitLeaf(Query q) {
                    if (isCostlyToBuildScorer(q)) {
                        found[0] = true;
                    }
                }

                @Override
                public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                    if (isCostlyToBuildScorer(parent)) {
                        found[0] = true;
                    }
                    // Visit every branch including MUST_NOT — a costly negated clause still has to be
                    // resolved at scorer-build time, so it should still steer us to SEGMENT.
                    return this;
                }
            });
            return found[0];
        }

        // copied from UsageTrackingQueryCachingPolicy
        static boolean isCostlyToBuildScorer(Query query) {
            if (query instanceof MultiTermQuery || query instanceof PointRangeQuery || query instanceof PointInSetQuery) {
                return true;
            }
            final String clazzName = query.getClass().getSimpleName();
            if (clazzName.equals("MultiTermQueryConstantScoreBlendedWrapper") || clazzName.equals("MultiTermQueryConstantScoreWrapper")) {
                return true;
            }
            return false;
        }

        static Query unwrapQuery(Query query) {
            while (true) {
                switch (query) {
                    case BoostQuery q: {
                        query = q.getQuery();
                        break;
                    }
                    case ConstantScoreQuery q: {
                        query = q.getQuery();
                        break;
                    }
                    default:
                        return query;
                }
            }
        }
    }

    @SuppressWarnings("this-escape")
    public LuceneSourceOperator(
        IndexedByShardId<? extends RefCounted> refCounteds,
        DriverContext driverContext,
        int maxPageSize,
        LuceneSliceQueue sliceQueue,
        int limit,
        Limiter limiter,
        boolean needsScore,
        LongSupplier directoryBytesRead,
        QueryWarnings singleValueQueryWarnings
    ) {
        super(refCounteds, driverContext, maxPageSize, sliceQueue, directoryBytesRead, singleValueQueryWarnings);
        this.minPageSize = Math.max(1, maxPageSize / 2);
        this.remainingDocs = limit;
        this.limiter = limiter;
        int estimatedSize = Math.min(limit, maxPageSize);
        boolean success = false;
        try {
            if (needsScore) {
                scoreBuilder = blockFactory.newDoubleVectorBuilder(estimatedSize);
                this.leafCollector = new ScoringCollector();
            } else {
                scoreBuilder = null;
                this.leafCollector = new LimitingCollector();
            }
            this.docIdsPool = new IntArrayPool(blockFactory.breaker());
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    class LimitingCollector implements LeafCollector {
        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) throws IOException {
            if (remainingDocs > 0) {
                --remainingDocs;
                docIds[currentPagePos++] = doc;
            } else {
                throw new CollectionTerminatedException();
            }
        }
    }

    final class ScoringCollector extends LuceneSourceOperator.LimitingCollector {
        private Scorable scorable;

        @Override
        public void setScorer(Scorable scorer) {
            this.scorable = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            super.collect(doc);
            scoreBuilder.appendDouble(scorable.score());
        }
    }

    @Override
    public boolean isFinished() {
        return doneCollecting || limiter.remaining() == 0;
    }

    @Override
    public void finish() {
        doneCollecting = true;
    }

    @Override
    public Page getCheckedOutput() throws IOException {
        if (isFinished()) {
            return null;
        }
        long start = System.nanoTime();
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            if (scorer == null) {
                return null;
            }
            if (docIds == null) {
                docIds = docIdsPool.getOrAllocate(maxPageSize);
            }
            final int remainingDocsStart = remainingDocs = limiter.remaining();
            try {
                scorer.scoreNextRange(
                    leafCollector,
                    scorer.leafReaderContext().reader().getLiveDocs(),
                    // Note: if (maxPageSize - currentPagePos) is a small "remaining" interval, this could lead to slow collection with a
                    // highly selective filter. Having a large "enough" difference between max- and minPageSize (and thus currentPagePos)
                    // alleviates this issue.
                    maxPageSize - currentPagePos
                );
            } catch (CollectionTerminatedException ex) {
                // The leaf collector terminated the execution
                doneCollecting = true;
                scorer.markAsDone();
            }
            final int collectedDocs = remainingDocsStart - remainingDocs;
            final int discardedDocs = collectedDocs - limiter.tryAccumulateHits(collectedDocs);
            Page page = null;
            if (currentPagePos >= minPageSize || scorer.isDone() || (remainingDocs = limiter.remaining()) == 0) {
                IntVector shard = null;
                IntVector leaf = null;
                IntVector docs = null;
                int metadataBlocks = numMetadataBlocks();
                Block[] blocks = new Block[1 + metadataBlocks + scorer.tags().size()];
                currentPagePos -= discardedDocs;
                try {
                    int shardId = scorer.shardContext().index();
                    shard = blockFactory.newConstantIntVector(shardId, currentPagePos);
                    leaf = blockFactory.newConstantIntVector(scorer.leafReaderContext().ord, currentPagePos);
                    docs = buildDocsVector();
                    int b = 0;
                    blocks[b++] = new DocVector(refCounteds, shard, leaf, docs, DocVector.config().singleSegmentNonDecreasing(true))
                        .asBlock();
                    shard = null;
                    leaf = null;
                    docs = null;
                    buildMetadataBlocks(blocks, b, currentPagePos);
                    b += metadataBlocks;
                    for (Object e : scorer.tags()) {
                        blocks[b++] = BlockUtils.constantBlock(blockFactory, e, currentPagePos);
                    }
                    page = new Page(currentPagePos, blocks);
                    shardRowsEmitted[shardId] += page.getPositionCount();
                } finally {
                    if (page == null) {
                        Releasables.closeExpectNoException(shard, leaf, docs, Releasables.wrap(blocks));
                    }
                }
                currentPagePos = 0;
            }
            return page;
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    private IntVector buildDocsVector() {
        final var docIds = this.docIds;
        this.docIds = null;
        final var arrayPool = this.docIdsPool; // avoid holding reference to this
        final IntVector docs = blockFactory.newIntArrayVector(docIds, currentPagePos);
        docs.attachReleasable(() -> arrayPool.put(docIds));
        return docs;
    }

    private DoubleVector buildScoresVector(int upToPositions) {
        final DoubleVector scores = scoreBuilder.build();
        assert scores.getPositionCount() >= upToPositions : scores.getPositionCount() + " < " + upToPositions;
        if (scores.getPositionCount() == upToPositions) {
            return scores;
        }
        try (scores) {
            try (var slice = blockFactory.newDoubleVectorBuilder(upToPositions)) {
                for (int i = 0; i < upToPositions; i++) {
                    slice.appendDouble(scores.getDouble(i));
                }
                return slice.build();
            }
        }
    }

    protected int numMetadataBlocks() {
        return scoreBuilder != null ? 1 : 0;
    }

    protected void buildMetadataBlocks(Block[] blocks, int offset, int currentPagePos) {
        if (scoreBuilder != null) {
            blocks[offset] = buildScoresVector(currentPagePos).asBlock();
            scoreBuilder = blockFactory.newDoubleVectorBuilder(Math.min(remainingDocs, maxPageSize));
        }
    }

    @Override
    public void additionalClose() {
        Releasables.close(scoreBuilder, docIdsPool);
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", remainingDocs = ").append(remainingDocs);
    }

    private static class IntArrayPool implements Releasable {
        private int[] pool;
        private long usedBytes;
        private final CircuitBreaker breaker;

        IntArrayPool(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        int[] getOrAllocate(int size) {
            int[] arr = this.pool;
            this.pool = null;
            if (arr == null || arr.length < size) {
                final long newBytes = RamUsageEstimator.alignObjectSize(
                    (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * size
                );
                if (newBytes > usedBytes) {
                    breaker.addEstimateBytesAndMaybeBreak(newBytes - usedBytes, "int[]");
                    usedBytes = newBytes;
                }
                arr = new int[size];
            }
            return arr;
        }

        void put(int[] arr) {
            // the pool can be tolerated with races, where we just allocate a new array
            this.pool = arr;
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-usedBytes);
        }
    }
}
