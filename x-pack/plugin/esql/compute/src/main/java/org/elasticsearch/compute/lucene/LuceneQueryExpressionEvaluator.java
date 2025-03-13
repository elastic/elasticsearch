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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

import java.io.IOException;

/**
 * {@link EvalOperator.ExpressionEvaluator} to run a Lucene {@link Query} during
 * the compute engine's normal execution, yielding matches/does not match into
 * a {@link BooleanVector}.
 * @see LuceneQueryScoreEvaluator
 */
public class LuceneQueryExpressionEvaluator extends LuceneQueryEvaluator<BooleanVector.Builder>
    implements
        EvalOperator.ExpressionEvaluator {

    LuceneQueryExpressionEvaluator(BlockFactory blockFactory, ShardConfig[] shards) {
        super(blockFactory, shards);
    }

    @Override
    public Block eval(Page page) {
        return executeQuery(page);
    }

    @Override
    protected ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    protected Vector createNoMatchVector(BlockFactory blockFactory, int size) {
        return blockFactory.newConstantBooleanVector(false, size);
    }

    @Override
    protected BooleanVector.Builder createVectorBuilder(BlockFactory blockFactory, int size) {
        return blockFactory.newBooleanVectorFixedBuilder(size);
    }

    @Override
    protected void appendNoMatch(BooleanVector.Builder builder) {
        builder.appendBoolean(false);
    }

    @Override
    protected void appendMatch(BooleanVector.Builder builder, Scorable scorer) throws IOException {
        builder.appendBoolean(true);
    }

    public record Factory(ShardConfig[] shardConfigs) implements EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new LuceneQueryExpressionEvaluator(context.blockFactory(), shardConfigs);
        }
    }
}
