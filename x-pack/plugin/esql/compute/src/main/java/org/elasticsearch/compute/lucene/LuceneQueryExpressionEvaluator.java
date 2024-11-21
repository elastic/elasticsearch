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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * {@link EvalOperator.ExpressionEvaluator} to run a Lucene {@link Query} during
 * the compute engine's normal execution, yielding matches/does not match into
 * a {@link BooleanVector}. It's much faster to push these to the
 * {@link LuceneSourceOperator} or the like, but sometimes this isn't possible. So
 * this evaluator is here to save the day.
 */
public class LuceneQueryExpressionEvaluator implements EvalOperator.ExpressionEvaluator {
    public record ShardConfig(Query query, IndexSearcher searcher) {}

    private final BlockFactory blockFactory;
    private final ShardConfig[] shards;
    private final int docChannel;

    private ShardState[] perShardState = EMPTY_SHARD_STATES;

    public LuceneQueryExpressionEvaluator(BlockFactory blockFactory, ShardConfig[] shards, int docChannel) {
        this.blockFactory = blockFactory;
        this.shards = shards;
        this.docChannel = docChannel;
    }

    @Override
    public Block eval(Page page) {
        DocVector docs = page.<DocBlock>getBlock(docChannel).asVector();
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
    private BooleanVector evalSingleSegmentNonDecreasing(DocVector docs) throws IOException {
        ShardState shardState = shardState(docs.shards().getInt(0));
        SegmentState segmentState = shardState.segmentState(docs.segments().getInt(0));
        int min = docs.docs().getInt(0);
        int max = docs.docs().getInt(docs.getPositionCount() - 1);
        int length = max - min + 1;
        if (length == docs.getPositionCount() && length > 1) {
            return segmentState.scoreDense(min, max);
        }
        return segmentState.scoreSparse(docs.docs());
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
    private BooleanVector evalSlow(DocVector docs) throws IOException {
        int[] map = docs.shardSegmentDocMapForwards();
        // Clear any state flags from the previous run
        int prevShard = -1;
        int prevSegment = -1;
        SegmentState segmentState = null;
        try (BooleanVector.Builder builder = blockFactory.newBooleanVectorFixedBuilder(docs.getPositionCount())) {
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
                    builder.appendBoolean(false);
                } else {
                    segmentState.scoreSingleDocWithScorer(builder, docs.docs().getInt(map[i]));
                }
            }
            try (BooleanVector outOfOrder = builder.build()) {
                return outOfOrder.filter(docs.shardSegmentDocMapBackwards());
            }
        }
    }

    @Override
    public void close() {

    }

    private ShardState shardState(int shard) throws IOException {
        if (shard >= perShardState.length) {
            perShardState = ArrayUtil.grow(perShardState, shard + 1);
        } else if (perShardState[shard] != null) {
            return perShardState[shard];
        }
        perShardState[shard] = new ShardState(shards[shard]);
        return perShardState[shard];
    }

    private class ShardState {
        private final Weight weight;
        private final IndexSearcher searcher;
        private SegmentState[] perSegmentState = EMPTY_SEGMENT_STATES;

        ShardState(ShardConfig config) throws IOException {
            weight = config.searcher.createWeight(config.query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
            searcher = config.searcher;
        }

        SegmentState segmentState(int segment) throws IOException {
            if (segment >= perSegmentState.length) {
                perSegmentState = ArrayUtil.grow(perSegmentState, segment + 1);
            } else if (perSegmentState[segment] != null) {
                return perSegmentState[segment];
            }
            perSegmentState[segment] = new SegmentState(weight, searcher.getLeafContexts().get(segment));
            return perSegmentState[segment];
        }
    }

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
        BooleanVector scoreDense(int min, int max) throws IOException {
            int length = max - min + 1;
            if (noMatch) {
                return blockFactory.newConstantBooleanVector(false, length);
            }
            if (bulkScorer == null ||  // The bulkScorer wasn't initialized
                Thread.currentThread() != bulkScorerThread // The bulkScorer was initialized on a different thread
            ) {
                bulkScorerThread = Thread.currentThread();
                bulkScorer = weight.bulkScorer(ctx);
                if (bulkScorer == null) {
                    noMatch = true;
                    return blockFactory.newConstantBooleanVector(false, length);
                }
            }
            try (DenseCollector collector = new DenseCollector(blockFactory, min, max)) {
                bulkScorer.score(collector, ctx.reader().getLiveDocs(), min, max + 1);
                return collector.build();
            }
        }

        /**
         * Score a vector of doc ids using {@link Scorer}. If you have a dense range of
         * doc ids it'd be faster to use {@link #scoreDense}.
         */
        BooleanVector scoreSparse(IntVector docs) throws IOException {
            initScorer(docs.getInt(0));
            if (noMatch) {
                return blockFactory.newConstantBooleanVector(false, docs.getPositionCount());
            }
            try (BooleanVector.Builder builder = blockFactory.newBooleanVectorFixedBuilder(docs.getPositionCount())) {
                for (int i = 0; i < docs.getPositionCount(); i++) {
                    scoreSingleDocWithScorer(builder, docs.getInt(i));
                }
                return builder.build();
            }
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

        private void scoreSingleDocWithScorer(BooleanVector.Builder builder, int doc) throws IOException {
            if (scorer.iterator().docID() == doc) {
                builder.appendBoolean(true);
            } else if (scorer.iterator().docID() > doc) {
                builder.appendBoolean(false);
            } else {
                builder.appendBoolean(scorer.iterator().advance(doc) == doc);
            }
        }
    }

    private static final ShardState[] EMPTY_SHARD_STATES = new ShardState[0];
    private static final SegmentState[] EMPTY_SEGMENT_STATES = new SegmentState[0];

    /**
     * Collects matching information for dense range of doc ids. This assumes that
     * doc ids are sent to {@link LeafCollector#collect(int)} in ascending order
     * which isn't documented, but @jpountz swears is true.
     */
    static class DenseCollector implements LeafCollector, Releasable {
        private final BooleanVector.FixedBuilder builder;
        private final int max;

        int next;

        DenseCollector(BlockFactory blockFactory, int min, int max) {
            this.builder = blockFactory.newBooleanVectorFixedBuilder(max - min + 1);
            this.max = max;
            next = min;
        }

        @Override
        public void setScorer(Scorable scorable) {}

        @Override
        public void collect(int doc) {
            while (next++ < doc) {
                builder.appendBoolean(false);
            }
            builder.appendBoolean(true);
        }

        public BooleanVector build() {
            return builder.build();
        }

        @Override
        public void finish() {
            while (next++ <= max) {
                builder.appendBoolean(false);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(builder);
        }
    }
}
