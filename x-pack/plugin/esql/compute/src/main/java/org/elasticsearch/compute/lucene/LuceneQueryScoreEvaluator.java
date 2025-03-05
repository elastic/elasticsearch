/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.ScoreOperator;

import java.io.IOException;

/**
 * {@link EvalOperator.ExpressionEvaluator} to run a Lucene {@link Query} during
 * the compute engine's normal execution, yielding matches/does not match into
 * a {@link BooleanVector}. It's much faster to push these to the
 * {@link LuceneSourceOperator} or the like, but sometimes this isn't possible. So
 * this evaluator is here to save the day.
 */
public class LuceneQueryScoreEvaluator extends LuceneQueryEvaluator<DoubleVector.Builder> implements ScoreOperator.ExpressionScorer {

    public static final double NO_MATCH_SCORE = 0.0;

    LuceneQueryScoreEvaluator(BlockFactory blockFactory, ShardConfig[] shards) {
        super(blockFactory, shards);
    }

    @Override
    public DoubleBlock score(Page page) {
        return (DoubleBlock) executeQuery(page);
    }

    @Override
    protected ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    protected Vector createNoMatchVector(BlockFactory blockFactory, int size) {
        return blockFactory.newConstantDoubleVector(NO_MATCH_SCORE, size);
    }

    @Override
    protected DoubleVector.Builder createBuilder(BlockFactory blockFactory, int size) {
        return blockFactory.newDoubleVectorFixedBuilder(size);
    }

    @Override
    protected void appendNoMatch(DoubleVector.Builder builder) {
        builder.appendDouble(NO_MATCH_SCORE);
    }

    @Override
    protected void appendMatch(DoubleVector.Builder builder, Scorable scorer) throws IOException {
        builder.appendDouble(scorer.score());
    }

    public record Factory(ShardConfig[] shardConfigs) implements ScoreOperator.ExpressionScorer.Factory {
        @Override
        public ScoreOperator.ExpressionScorer get(DriverContext context) {
            return new LuceneQueryScoreEvaluator(context.blockFactory(), shardConfigs);
        }
    }
}
