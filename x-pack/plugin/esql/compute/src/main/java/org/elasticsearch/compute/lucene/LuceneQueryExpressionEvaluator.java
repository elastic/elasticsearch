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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;

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

    public static final float NO_MATCH_SCORE = -1.0F;

    public record ShardConfig(Query query, IndexSearcher searcher) {}

    private final BlockFactory blockFactory;
    private final ShardConfig[] shards;
    private final boolean usesScoring;

    private ShardState[] perShardState = EMPTY_SHARD_STATES;
    private DoubleVector scoreVector;

    public LuceneQueryExpressionEvaluator(BlockFactory blockFactory, ShardConfig[] shards, boolean usesScoring) {
        this.blockFactory = blockFactory;
        this.shards = shards;
        this.usesScoring = usesScoring;
    }

    @Override
    public Block eval(Page page) {
        // Lucene based operators retrieve DocVectors as first block
        Block block = page.getBlock(0);
        assert block instanceof DocBlock : "LuceneQueryExpressionEvaluator expects DocBlock as input";
        DocVector docs = (DocVector) block.asVector();
        try {
            final Tuple<BooleanVector, DoubleVector> evalResult;
            if (docs.singleSegmentNonDecreasing()) {
                evalResult = evalSingleSegmentNonDecreasing(docs);
            } else {
                evalResult = evalSlow(docs);
            }
            // Cache the score vector for later use
            scoreVector = evalResult.v2();
            return evalResult.v1().asBlock();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public DoubleBlock score(Page page, BlockFactory blockFactory) {
        assert scoreVector != null : "eval() should be invoked before calling score()";
        return scoreVector.asBlock();
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
    private Tuple<BooleanVector, DoubleVector> evalSingleSegmentNonDecreasing(DocVector docs) throws IOException {
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
    private Tuple<BooleanVector, DoubleVector> evalSlow(DocVector docs) throws IOException {
        int[] map = docs.shardSegmentDocMapForwards();
        // Clear any state flags from the previous run
        int prevShard = -1;
        int prevSegment = -1;
        SegmentState segmentState = null;
        try (
            BooleanVector.Builder builder = blockFactory.newBooleanVectorFixedBuilder(docs.getPositionCount());
            DoubleVector.Builder scoreBuilder = blockFactory.newDoubleVectorFixedBuilder(docs.getPositionCount())
        ) {
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
                    scoreBuilder.appendDouble(NO_MATCH_SCORE);
                } else {
                    segmentState.scoreSingleDocWithScorer(builder, scoreBuilder, docs.docs().getInt(map[i]));
                }
            }
            try (BooleanVector outOfOrder = builder.build(); DoubleVector outOfOrderScores = scoreBuilder.build()) {
                return Tuple.tuple(
                    outOfOrder.filter(docs.shardSegmentDocMapBackwards()),
                    outOfOrderScores.filter(docs.shardSegmentDocMapBackwards())
                );
            }
        }
    }

    @Override
    public void close() {}

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
            float boost = usesScoring ? 1.0f : 0.0f;
            ScoreMode scoreMode = usesScoring ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
            weight = config.searcher.createWeight(config.query, scoreMode, boost);
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
        Tuple<BooleanVector, DoubleVector> scoreDense(int min, int max) throws IOException {
            int length = max - min + 1;
            if (noMatch) {
                return Tuple.tuple(
                    blockFactory.newConstantBooleanVector(false, length),
                    blockFactory.newConstantDoubleVector(NO_MATCH_SCORE, length)
                );
            }
            if (bulkScorer == null ||  // The bulkScorer wasn't initialized
                Thread.currentThread() != bulkScorerThread // The bulkScorer was initialized on a different thread
            ) {
                bulkScorerThread = Thread.currentThread();
                bulkScorer = weight.bulkScorer(ctx);
                if (bulkScorer == null) {
                    noMatch = true;
                    return Tuple.tuple(
                        blockFactory.newConstantBooleanVector(false, length),
                        blockFactory.newConstantDoubleVector(NO_MATCH_SCORE, length)
                    );
                }
            }

            final DenseCollector collector;
            if (usesScoring) {
                collector = new ScoringDenseCollector(blockFactory, min, max);
            } else {
                collector = new DenseCollector(blockFactory, min, max);
            }
            try (collector) {
                bulkScorer.score(collector, ctx.reader().getLiveDocs(), min, max + 1);
                return Tuple.tuple(collector.buildMatchVector(), collector.buildScoreVector());
            }
        }

        /**
         * Score a vector of doc ids using {@link Scorer}. If you have a dense range of
         * doc ids it'd be faster to use {@link #scoreDense}.
         */
        Tuple<BooleanVector, DoubleVector> scoreSparse(IntVector docs) throws IOException {
            initScorer(docs.getInt(0));
            if (noMatch) {
                return Tuple.tuple(
                    blockFactory.newConstantBooleanVector(false, docs.getPositionCount()),
                    blockFactory.newConstantDoubleVector(NO_MATCH_SCORE, docs.getPositionCount())
                );
            }
            try (
                BooleanVector.Builder builder = blockFactory.newBooleanVectorFixedBuilder(docs.getPositionCount());
                DoubleVector.Builder scoreBuilder = blockFactory.newDoubleVectorFixedBuilder(docs.getPositionCount())
            ) {
                for (int i = 0; i < docs.getPositionCount(); i++) {
                    scoreSingleDocWithScorer(builder, scoreBuilder, docs.getInt(i));
                }
                return Tuple.tuple(builder.build(), scoreBuilder.build());
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

        private void scoreSingleDocWithScorer(BooleanVector.Builder builder, DoubleVector.Builder scoreBuilder, int doc)
            throws IOException {
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
        private final BooleanVector.FixedBuilder matchBuilder;
        private final int max;

        int next;

        DenseCollector(BlockFactory blockFactory, int min, int max) {
            this.matchBuilder = blockFactory.newBooleanVectorFixedBuilder(max - min + 1);
            this.max = max;
            next = min;
        }

        @Override
        public void setScorer(Scorable scorable) {}

        @Override
        public void collect(int doc) throws IOException {
            while (next++ < doc) {
                appendNoMatch();
            }
            appendMatch();
        }

        protected void appendMatch() throws IOException {
            matchBuilder.appendBoolean(true);
        }

        protected void appendNoMatch() {
            matchBuilder.appendBoolean(false);
        }

        public BooleanVector buildMatchVector() {
            return matchBuilder.build();
        }

        public DoubleVector buildScoreVector() {
            return null;
        }

        @Override
        public void finish() {
            while (next++ <= max) {
                appendNoMatch();
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(matchBuilder);
        }
    }

    static class ScoringDenseCollector extends DenseCollector {
        private final DoubleVector.FixedBuilder scoreBuilder;
        private Scorable scorable;

        ScoringDenseCollector(BlockFactory blockFactory, int min, int max) {
            super(blockFactory, min, max);
            this.scoreBuilder = blockFactory.newDoubleVectorFixedBuilder(max - min + 1);
        }

        @Override
        public void setScorer(Scorable scorable) {
            this.scorable = scorable;
        }

        @Override
        protected void appendMatch() throws IOException {
            super.appendMatch();
            scoreBuilder.appendDouble(scorable.score());
        }

        @Override
        protected void appendNoMatch() {
            super.appendNoMatch();
            scoreBuilder.appendDouble(NO_MATCH_SCORE);
        }

        @Override
        public DoubleVector buildScoreVector() {
            return scoreBuilder.build();
        }

        @Override
        public void close() {
            super.close();
            Releasables.closeExpectNoException(scoreBuilder);
        }
    }

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final ShardConfig[] shardConfigs;
        private final boolean usesScoring;

        public Factory(ShardConfig[] shardConfigs, boolean usesScoring) {
            this.shardConfigs = shardConfigs;
            this.usesScoring = usesScoring;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new LuceneQueryExpressionEvaluator(context.blockFactory(), shardConfigs, usesScoring);
        }
    }
}
