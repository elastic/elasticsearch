/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Base class for evaluating a Lucene query at the compute engine and providing a Block as a result.
 * Subclasses can override methods to decide what type of {@link Block} should be returned, and how to add results to it
 * based on documents on the Page matching the query or not.
 * See {@link LuceneQueryExpressionEvaluator} for an example of how to use this class and {@link LuceneQueryScoreEvaluator} for
 * examples of subclasses that provide different types of scoring results for different ESQL constructs.
 * It's much faster to push queries to the {@link LuceneSourceOperator} or the like, but sometimes this isn't possible. So
 * this class is here to save the day.
 */
public abstract class LuceneQueryEvaluator<T extends Vector.Builder> implements Releasable {

    public record ShardConfig(Query query, IndexSearcher searcher) {}

    private final BlockFactory blockFactory;
    private final ShardConfig[] shards;

    private final List<ShardState> perShardState;

    protected LuceneQueryEvaluator(BlockFactory blockFactory, ShardConfig[] shards) {
        this.blockFactory = blockFactory;
        this.shards = shards;
        this.perShardState = new ArrayList<>(Collections.nCopies(shards.length, null));
    }

    public Block executeQuery(Page page) {
        // Lucene based operators retrieve DocVectors as first block
        Block block = page.getBlock(0);
        assert block instanceof DocBlock : "LuceneQueryExpressionEvaluator expects DocBlock as input";
        DocVector docs = (DocVector) block.asVector();
        try {
            if (docs.singleSegmentNonDecreasing()) {
                return evalSingleSegmentNonDecreasing(docs).asBlock();
            } else {
                return evalSlow(docs).asBlock();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Evaluate {@link DocVector#singleSegmentNonDecreasing()} documents.
     * <p>
     *     ESQL receives documents in DocVector, and they can be in one of two
     *     states. Either the DocVector contains documents from a single segment
     *     non-decreasing order, or it doesn't. that first case is much more like
     *     how Lucene likes to process documents. and it's much more common. So we
     *     optimize for it.
     * <p>
     *     Vectors that are {@link DocVector#singleSegmentNonDecreasing()}
     *     represent many documents from a single Lucene segment. In Elasticsearch
     *     terms that's a segment in a single shard. And the document ids are in
     *     non-decreasing order. Probably just {@code 0, 1, 2, 3, 4, 5...}.
     *     But maybe something like {@code 0, 5, 6, 10, 10, 10}. Both of those are
     *     very like how lucene "natively" processes documents and this optimizes
     *     those accesses.
     * </p>
     * <p>
     *     If the documents are literally {@code 0, 1, ... n} then we use
     *     {@link BulkScorer#score(LeafCollector, Bits, int, int)} which processes
     *     a whole range. This should be quite common in the case where we don't
     *     have deleted documents because that's the order that
     *     {@link LuceneSourceOperator} produces them.
     * </p>
     * <p>
     *     If there are gaps in the sequence we use {@link Scorer} calls to
     *     score the sequence. This'll be less fast but isn't going be particularly
     *     common.
     * </p>
     */
    private Vector evalSingleSegmentNonDecreasing(DocVector docs) throws IOException {
        ShardState shardState = shardState(docs.shards().getInt(0));
        SegmentState segmentState = shardState.segmentState(docs.segments().getInt(0));
        int min = docs.docs().getInt(0);
        int max = docs.docs().getInt(docs.getPositionCount() - 1);
        int length = max - min + 1;
        try (T scoreBuilder = createVectorBuilder(blockFactory, length)) {
            if (length == docs.getPositionCount() && length > 1) {
                return segmentState.scoreDense(scoreBuilder, min, max);
            }
            return segmentState.scoreSparse(scoreBuilder, docs.docs());
        }
    }

    /**
     * Evaluate non-{@link DocVector#singleSegmentNonDecreasing()} documents. See
     * {@link #evalSingleSegmentNonDecreasing} for the meaning of
     * {@link DocVector#singleSegmentNonDecreasing()} and how we can efficiently
     * evaluate those segments.
     * <p>
     *     This processes the worst case blocks of documents. These can be from any
     *     number of shards and any number of segments and in any order. We do this
     *     by iterating the docs in {@code shard ASC, segment ASC, doc ASC} order.
     *     So, that's segment by segment, docs ascending. We build a boolean block
     *     out of that. Then we <strong>sort</strong> that to put the booleans in
     *     the order that the {@link DocVector} came in.
     * </p>
     */
    private Vector evalSlow(DocVector docs) throws IOException {
        int[] map = docs.shardSegmentDocMapForwards();
        // Clear any state flags from the previous run
        int prevShard = -1;
        int prevSegment = -1;
        SegmentState segmentState = null;
        try (T scoreBuilder = createVectorBuilder(blockFactory, docs.getPositionCount())) {
            for (int i = 0; i < docs.getPositionCount(); i++) {
                int shard = docs.shards().getInt(docs.shards().getInt(map[i]));
                int segment = docs.segments().getInt(map[i]);
                if (segmentState == null || prevShard != shard || prevSegment != segment) {
                    segmentState = shardState(shard).segmentState(segment);
                    segmentState.initScorer(docs.docs().getInt(map[i]));
                    prevShard = shard;
                    prevSegment = segment;
                }
                if (segmentState.noMatch) {
                    appendNoMatch(scoreBuilder);
                } else {
                    segmentState.scoreSingleDocWithScorer(scoreBuilder, docs.docs().getInt(map[i]));
                }
            }
            try (Vector outOfOrder = scoreBuilder.build()) {
                return outOfOrder.filter(docs.shardSegmentDocMapBackwards());
            }
        }
    }

    @Override
    public void close() {}

    private ShardState shardState(int shard) throws IOException {
        ShardState shardState = perShardState.get(shard);
        if (shardState != null) {
            return shardState;
        }
        shardState = new ShardState(shards[shard]);
        perShardState.set(shard, shardState);
        return shardState;
    }

    /**
     * Contains shard search related information, like the weight and index searcher
     */
    private class ShardState {
        private final Weight weight;
        private final IndexSearcher searcher;
        private final List<SegmentState> perSegmentState;

        ShardState(ShardConfig config) throws IOException {
            // At this point, only the QueryBuilder has been rewritten into the query, but not the query itself.
            // The query needs to be rewritten before creating the Weight so it can be transformed into the final Query to execute.
            Query rewritten = config.searcher.rewrite(config.query);
            weight = config.searcher.createWeight(rewritten, scoreMode(), 1.0f);
            searcher = config.searcher;
            perSegmentState = new ArrayList<>(Collections.nCopies(searcher.getLeafContexts().size(), null));
        }

        SegmentState segmentState(int segment) throws IOException {
            SegmentState segmentState = perSegmentState.get(segment);
            if (segmentState != null) {
                return segmentState;
            }
            segmentState = new SegmentState(weight, searcher.getLeafContexts().get(segment));
            perSegmentState.set(segment, segmentState);
            return segmentState;
        }
    }

    /**
     * Contains segment search related information, like the leaf reader context and bulk scorer
     */
    private class SegmentState {
        private final Weight weight;
        private final LeafReaderContext ctx;

        /**
         * Lazily initialed {@link Scorer} for this. {@code null} here means uninitialized
         * <strong>or</strong> that {@link #noMatch} is true.
         */
        private Scorer scorer;

        /**
         * Thread that initialized the {@link #scorer}.
         */
        private Thread scorerThread;

        /**
         * Lazily initialed {@link BulkScorer} for this. {@code null} here means uninitialized
         * <strong>or</strong> that {@link #noMatch} is true.
         */
        private BulkScorer bulkScorer;

        /**
         * Thread that initialized the {@link #bulkScorer}.
         */
        private Thread bulkScorerThread;

        /**
         * Set to {@code true} if, in the process of building a {@link Scorer} or {@link BulkScorer},
         * the {@link Weight} tells us there aren't any matches.
         */
        private boolean noMatch;

        private SegmentState(Weight weight, LeafReaderContext ctx) {
            this.weight = weight;
            this.ctx = ctx;
        }

        /**
         * Score a range using the {@link BulkScorer}. This should be faster
         * than using {@link #scoreSparse} for dense doc ids.
         */
        Vector scoreDense(T scoreBuilder, int min, int max) throws IOException {
            if (noMatch) {
                return createNoMatchVector(blockFactory, max - min + 1);
            }
            if (bulkScorer == null ||  // The bulkScorer wasn't initialized
                Thread.currentThread() != bulkScorerThread // The bulkScorer was initialized on a different thread
            ) {
                bulkScorerThread = Thread.currentThread();
                bulkScorer = weight.bulkScorer(ctx);
                if (bulkScorer == null) {
                    noMatch = true;
                    return createNoMatchVector(blockFactory, max - min + 1);
                }
            }
            try (
                DenseCollector<T> collector = new DenseCollector<>(
                    min,
                    max,
                    scoreBuilder,
                    LuceneQueryEvaluator.this::appendNoMatch,
                    LuceneQueryEvaluator.this::appendMatch
                )
            ) {
                bulkScorer.score(collector, ctx.reader().getLiveDocs(), min, max + 1);
                return collector.build();
            }
        }

        /**
         * Score a vector of doc ids using {@link Scorer}. If you have a dense range of
         * doc ids it'd be faster to use {@link #scoreDense}.
         */
        Vector scoreSparse(T scoreBuilder, IntVector docs) throws IOException {
            initScorer(docs.getInt(0));
            if (noMatch) {
                return createNoMatchVector(blockFactory, docs.getPositionCount());
            }
            for (int i = 0; i < docs.getPositionCount(); i++) {
                scoreSingleDocWithScorer(scoreBuilder, docs.getInt(i));
            }
            return scoreBuilder.build();
        }

        private void initScorer(int minDocId) throws IOException {
            if (noMatch) {
                return;
            }
            if (scorer == null || // Scorer not initialized
                scorerThread != Thread.currentThread() || // Scorer initialized on a different thread
                scorer.iterator().docID() > minDocId // The previous block came "after" this one
            ) {
                scorerThread = Thread.currentThread();
                scorer = weight.scorer(ctx);
                if (scorer == null) {
                    noMatch = true;
                }
            }
        }

        private void scoreSingleDocWithScorer(T builder, int doc) throws IOException {
            if (scorer.iterator().docID() == doc) {
                appendMatch(builder, scorer);
            } else if (scorer.iterator().docID() > doc) {
                appendNoMatch(builder);
            } else {
                if (scorer.iterator().advance(doc) == doc) {
                    appendMatch(builder, scorer);
                } else {
                    appendNoMatch(builder);
                }
            }
        }
    }

    /**
     * Collects matching information for dense range of doc ids. This assumes that
     * doc ids are sent to {@link LeafCollector#collect(int)} in ascending order
     * which isn't documented, but @jpountz swears is true.
     */
    static class DenseCollector<U extends Vector.Builder> implements LeafCollector, Releasable {
        private final U scoreBuilder;
        private final int max;
        private final Consumer<U> appendNoMatch;
        private final CheckedBiConsumer<U, Scorable, IOException> appendMatch;

        private Scorable scorer;
        int next;

        DenseCollector(
            int min,
            int max,
            U scoreBuilder,
            Consumer<U> appendNoMatch,
            CheckedBiConsumer<U, Scorable, IOException> appendMatch
        ) {
            this.scoreBuilder = scoreBuilder;
            this.max = max;
            next = min;
            this.appendNoMatch = appendNoMatch;
            this.appendMatch = appendMatch;
        }

        @Override
        public void setScorer(Scorable scorable) {
            this.scorer = scorable;
        }

        @Override
        public void collect(int doc) throws IOException {
            while (next++ < doc) {
                appendNoMatch.accept(scoreBuilder);
            }
            appendMatch.accept(scoreBuilder, scorer);
        }

        public Vector build() {
            return scoreBuilder.build();
        }

        @Override
        public void finish() {
            while (next++ <= max) {
                appendNoMatch.accept(scoreBuilder);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(scoreBuilder);
        }
    }

    /**
     * Returns the score mode to use on searches
     */
    protected abstract ScoreMode scoreMode();

    /**
     * Creates a vector where all positions correspond to elements that don't match the query
     */
    protected abstract Vector createNoMatchVector(BlockFactory blockFactory, int size);

    /**
     * Creates the corresponding vector builder to store the results of evaluating the query
     */
    protected abstract T createVectorBuilder(BlockFactory blockFactory, int size);

    /**
     * Appends a matching result to a builder created by @link createVectorBuilder}
     */
    protected abstract void appendMatch(T builder, Scorable scorer) throws IOException;

    /**
     * Appends a non matching result to a builder created by @link createVectorBuilder}
     */
    protected abstract void appendNoMatch(T builder);

}
