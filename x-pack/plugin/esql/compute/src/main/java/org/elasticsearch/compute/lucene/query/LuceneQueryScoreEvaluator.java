/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;

import java.io.IOException;

/**
 * {@link ExpressionEvaluator} to run a Lucene {@link Query} during
 * the compute engine's normal execution, yielding the corresponding scores into
 * a {@link DoubleVector}.
 * Elements that don't match will have a score of {@link #NO_MATCH_SCORE}.
 * @see LuceneQueryExpressionEvaluator
 */
public class LuceneQueryScoreEvaluator extends LuceneQueryEvaluator<DoubleBlock.Builder> implements ExpressionEvaluator {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LuceneQueryScoreEvaluator.class);

    public static final double NO_MATCH_SCORE = 0.0;

    LuceneQueryScoreEvaluator(BlockFactory blockFactory, IndexedByShardId<ShardConfig> shards) {
        super(blockFactory, shards);
    }

    @Override
    public DoubleBlock eval(Page page) {
        return (DoubleBlock) executeQuery(page);
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    protected ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    protected DoubleBlock createNoMatchBlock(BlockFactory blockFactory, int size) {
        return blockFactory.newConstantDoubleBlockWith(NO_MATCH_SCORE, size);
    }

    @Override
    protected DoubleBlock.Builder createBlockBuilder(BlockFactory blockFactory, int size) {
        return blockFactory.newDoubleBlockBuilder(size);
    }

    @Override
    protected void appendNoMatch(DoubleBlock.Builder builder) {
        builder.appendDouble(NO_MATCH_SCORE);
    }

    @Override
    protected void appendMatch(DoubleBlock.Builder builder, Scorable scorer) throws IOException {
        builder.appendDouble(scorer.score());
    }

    public record Factory(IndexedByShardId<ShardConfig> shardConfigs) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new LuceneQueryScoreEvaluator(context.blockFactory(), shardConfigs);
        }
    }
}
