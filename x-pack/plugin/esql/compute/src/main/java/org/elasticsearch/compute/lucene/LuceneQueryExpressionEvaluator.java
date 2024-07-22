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

    private BooleanVector evalSlow(DocVector docs) throws IOException {
        int[] map = docs.shardSegmentDocMapForwards();
        // Clear any state flags from the previous run
        for (ShardState shardState : perShardState) {
            if (shardState == null) {
                continue;
            }
            for (SegmentState segmentState : shardState.perSegmentState) {
                if (segmentState == null) {
                    continue;
                }
                segmentState.initializedForCurrentBlock = false;
            }
        }
        try (BooleanVector.Builder builder = blockFactory.newBooleanVectorFixedBuilder(docs.getPositionCount())) {
            for (int i = 0; i < docs.getPositionCount(); i++) {
                ShardState shardState = shardState(docs.shards().getInt(docs.shards().getInt(map[i])));
                SegmentState segmentState = shardState.segmentState(docs.segments().getInt(map[i]));
                if (segmentState.initializedForCurrentBlock == false) {
                    segmentState.initScorer(docs.docs().getInt(map[i]));
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
         * Lazily initialed {@link BulkScorer} for this. {@code null} here means uninitialized
         * <strong>or</strong> that {@link #noMatch} is true.
         */
        private BulkScorer bulkScorer;

        /**
         * Set to {@code true} if, in the process of building a {@link Scorer} or {@link BulkScorer},
         * the {@link Weight} tells us there aren't any matches.
         */
        private boolean noMatch;

        private boolean initializedForCurrentBlock;

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
            if (bulkScorer == null) {
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
            if (noMatch) {
                return blockFactory.newConstantBooleanVector(false, docs.getPositionCount());
            }
            if (scorer == null || scorer.iterator().docID() > docs.getInt(0)) {
                // The previous block might have been beyond this one, reset the scorer and try again.
                scorer = weight.scorer(ctx);
                if (scorer == null) {
                    noMatch = true;
                    return blockFactory.newConstantBooleanVector(false, docs.getPositionCount());
                }
            }
            try (BooleanVector.Builder builder = blockFactory.newBooleanVectorFixedBuilder(docs.getPositionCount())) {
                for (int i = 0; i < docs.getPositionCount(); i++) {
                    scoreSingleDocWithScorer(builder, docs.getInt(i));
                }
                return builder.build();
            }
        }

        private void initScorer(int minDocId) throws IOException {
            if (scorer == null || scorer.iterator().docID() > minDocId) {
                // The previous block might have been beyond this one, reset the scorer and try again.
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
