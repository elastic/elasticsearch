/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.EsNumericRangeQuery;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

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

    private int currentPagePos = 0;
    private int remainingDocs;
    private final Limiter limiter;

    private IntVector.Builder docsBuilder;
    private DoubleVector.Builder scoreBuilder;
    private final LeafCollector leafCollector;
    private final int minPageSize;

    /** Pre-allocated mask buffer, sized to {@code maxPageSize}. Reused across calls. */
    private final boolean[] maskBuffer;
    private long bulkFilterLower;
    private long bulkFilterUpper;
    /** Bulk filter for the current leaf; refreshed when the leaf changes. */
    @Nullable
    private BlockLoader.OptionalBulkNumericFilter currentBulkFilter;
    private LeafReaderContext lastBulkFilterLeaf;

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
            boolean needsScore
        ) {
            super(
                shardContexts,
                queryFunction,
                dataPartitioning,
                dataPartitioning == DataPartitioning.AUTO ? autoStrategy.pickStrategy(limit) : q -> {
                    throw new UnsupportedOperationException("locked in " + dataPartitioning);
                },
                docThresholdForAutoStrategy,
                taskConcurrency,
                limit,
                needsScore,
                shardContext -> needsScore ? COMPLETE : COMPLETE_NO_SCORES
            );
            this.refCounteds = shardContexts;
            this.maxPageSize = maxPageSize;
            // TODO: use a single limiter for multiple stage execution
            this.limiter = limit == NO_LIMIT ? Limiter.NO_LIMIT : new Limiter(limit);
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneSourceOperator(refCounteds, driverContext.blockFactory(), maxPageSize, sliceQueue, limit, limiter, needsScore);
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
         * Pick a strategy for the {@link DataPartitioning#AUTO} partitioning.
         */
        public static Function<Query, PartitioningStrategy> autoStrategy(int limit) {
            return limit == NO_LIMIT ? Factory::highSpeedAutoStrategy : Factory::lowOverheadAutoStrategy;
        }

        /**
         * Use the {@link PartitioningStrategy#SHARD} strategy because
         * it has the lowest overhead. Used when there is a {@code limit} on the operator
         * because that's for cases like {@code FROM foo | LIMIT 10} or
         * {@code FROM foo | WHERE a == 1 | LIMIT 10} when the {@code WHERE} can be pushed
         * to Lucene. In those cases we're better off with the lowest overhead we can
         * manage - and that's {@link PartitioningStrategy#SHARD}.
         */
        private static PartitioningStrategy lowOverheadAutoStrategy(Query query) {
            return SHARD;
        }

        /**
         * Select the {@link PartitioningStrategy} based on the {@link Query}.
         * <ul>
         *     <li>
         *         If the {@linkplain Query} matches <strong>no</strong> documents then this will
         *         use the {@link PartitioningStrategy#SHARD} strategy so we minimize the overhead
         *         of finding nothing.
         *     </li>
         *     <li>
         *         If the {@linkplain Query} matches <strong>all</strong> documents then this will
         *         use the {@link PartitioningStrategy#DOC} strategy because the overhead of using
         *         that strategy for {@link MatchAllDocsQuery} is very low, and we need as many CPUs
         *         as we can get to process all the documents.
         *     </li>
         *     <li>
         *         Otherwise use the {@link PartitioningStrategy#SEGMENT} strategy because it's
         *         overhead is generally low.
         *     </li>
         * </ul>
         */
        private static PartitioningStrategy highSpeedAutoStrategy(Query query) {
            Query unwrapped = unwrapQuery(query);
            log.trace("highSpeedAutoStrategy {} {}", query, unwrapped);
            return switch (unwrapped) {
                case BooleanQuery bq -> highSpeedAutoStrategyForBoolean(bq);
                case MatchAllDocsQuery q -> DOC;
                case MatchNoDocsQuery q -> SHARD;
                case IndexOrDocValuesQuery q -> highSpeedAutoStrategy(q.getIndexQuery());
                default -> costlyToBuildScorer(unwrapped) ? SEGMENT : DOC;
            };
        }

        // copied from UsageTrackingQueryCachingPolicy
        private static boolean costlyToBuildScorer(Query query) {
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

        /**
         * Select the {@link PartitioningStrategy} for a {@link BooleanQuery}.
         */
        private static PartitioningStrategy highSpeedAutoStrategyForBoolean(BooleanQuery query) {
            for (BooleanClause c : query) {
                var strategy = highSpeedAutoStrategy(c.query());
                if (strategy != DOC) {
                    return strategy;
                }
            }
            return DOC;
        }
    }

    @SuppressWarnings("this-escape")
    public LuceneSourceOperator(
        IndexedByShardId<? extends RefCounted> refCounteds,
        BlockFactory blockFactory,
        int maxPageSize,
        LuceneSliceQueue sliceQueue,
        int limit,
        Limiter limiter,
        boolean needsScore
    ) {
        super(refCounteds, blockFactory, maxPageSize, sliceQueue);
        this.minPageSize = Math.max(1, maxPageSize / 2);
        this.remainingDocs = limit;
        this.limiter = limiter;
        this.maskBuffer = new boolean[maxPageSize];
        int estimatedSize = Math.min(limit, maxPageSize);
        boolean success = false;
        try {
            this.docsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            if (needsScore) {
                scoreBuilder = blockFactory.newDoubleVectorBuilder(estimatedSize);
                this.leafCollector = new ScoringCollector();
            } else {
                scoreBuilder = null;
                this.leafCollector = new LimitingCollector();
            }
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
                docsBuilder.appendInt(doc);
                currentPagePos++;
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

    /**
     * Represent a contiguous range of doc IDs as a {@link BlockLoader.Docs} for bulk filter evaluation.
     */
    private record SequentialDocs(int start, int count) implements BlockLoader.Docs {
        @Override
        public int get(int i) {
            return start + i;
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }

    /**
     * Updates {@link #currentBulkFilter} when the active leaf changes.
     * Detects {@link EsNumericRangeQuery} on the scorer's query and routes
     * to the bulk path when the field's doc values support it.
     */
    private void maybeUpdateBulkFilter(LuceneScorer scorer) throws IOException {
        final LeafReaderContext leaf = scorer.leafReaderContext();
        if (leaf == lastBulkFilterLeaf) {
            return;
        }
        lastBulkFilterLeaf = leaf;
        currentBulkFilter = null;
        if (scorer.query() instanceof EsNumericRangeQuery rangeQuery) {
            bulkFilterLower = rangeQuery.lowerValue();
            bulkFilterUpper = rangeQuery.upperValue();
            final SortedNumericDocValues dv = leaf.reader().getSortedNumericDocValues(rangeQuery.field());
            currentBulkFilter = (dv instanceof BlockLoader.OptionalBulkNumericFilter f) ? f : null;
        }
    }

    /**
     * Collects matching doc IDs into {@link #docsBuilder} using the bulk numeric filter,
     * bypassing Lucene's per-doc {@link org.apache.lucene.search.BulkScorer} machinery entirely.
     */
    private void collectBulkFiltered(LuceneScorer scorer) throws IOException {
        final int chunkSize = Math.min(scorer.maxPosition() - scorer.position(), maxPageSize - currentPagePos);
        if (chunkSize <= 0) {
            return;
        }
        final int start = scorer.position();
        currentBulkFilter.tryBulkRangeFilter(new SequentialDocs(start, chunkSize), bulkFilterLower, bulkFilterUpper, maskBuffer);
        final Bits liveDocs = scorer.leafReaderContext().reader().getLiveDocs();
        for (int i = 0; i < chunkSize; i++) {
            if (maskBuffer[i] && (liveDocs == null || liveDocs.get(start + i))) {
                if (remainingDocs > 0) {
                    --remainingDocs;
                    docsBuilder.appendInt(start + i);
                    currentPagePos++;
                } else {
                    doneCollecting = true;
                    scorer.markAsDone();
                    // scorer.position is now NO_MORE_DOCS; skip advanceTo
                    return;
                }
            }
        }
        scorer.advanceTo(start + chunkSize);
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
            maybeUpdateBulkFilter(scorer);
            final int remainingDocsStart = remainingDocs = limiter.remaining();
            if (currentBulkFilter != null) {
                collectBulkFiltered(scorer);
            } else {
                try {
                    scorer.scoreNextRange(
                        leafCollector,
                        scorer.leafReaderContext().reader().getLiveDocs(),
                        // Note: if (maxPageSize - currentPagePos) is a small "remaining" interval, this could lead to slow collection with
                        // a
                        // highly selective filter. Having a large "enough" difference between max- and minPageSize (and thus
                        // currentPagePos)
                        // alleviates this issue.
                        maxPageSize - currentPagePos
                    );
                } catch (CollectionTerminatedException ex) {
                    // The leaf collector terminated the execution
                    doneCollecting = true;
                    scorer.markAsDone();
                }
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
                    docs = buildDocsVector(currentPagePos);
                    docsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
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

    private IntVector buildDocsVector(int upToPositions) {
        final IntVector docs = docsBuilder.build();
        assert docs.getPositionCount() >= upToPositions : docs.getPositionCount() + " < " + upToPositions;
        if (docs.getPositionCount() == upToPositions) {
            return docs;
        }
        try (docs) {
            try (var slice = blockFactory.newIntVectorFixedBuilder(upToPositions)) {
                for (int i = 0; i < upToPositions; i++) {
                    slice.appendInt(docs.getInt(i));
                }
                return slice.build();
            }
        }
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
        Releasables.close(docsBuilder, scoreBuilder);
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", remainingDocs = ").append(remainingDocs);
    }
}
