/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

import java.util.function.BiFunction;

/**
 * {@link EvalOperator.ExpressionEvaluator} to run a Lucene {@link Query} during
 * the compute engine's normal execution, yielding matches/does not match into
 * a {@link BooleanVector}. It's much faster to push these to the
 * {@link LuceneSourceOperator} or the like, but sometimes this isn't possible. So
 * this evaluator is here to save the day.
 */
public class LuceneQueryExpressionEvaluator extends LuceneQueryEvaluator implements EvalOperator.ExpressionEvaluator {

    public static final double NO_MATCH_SCORE = 0.0;

    private LuceneQueryExpressionEvaluator(
        BlockFactory blockFactory,
        ShardConfig[] shards,
        BiFunction<BlockFactory, Integer, ScoreVectorBuilder> scoreVectorBuilderSupplier
    ) {
        super(blockFactory, shards, scoreVectorBuilderSupplier);
    }

    @Override
    public Block eval(Page page) {
        return executeQuery(page);
    }

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final ShardConfig[] shardConfigs;

        public Factory(ShardConfig[] shardConfigs) {
            this.shardConfigs = shardConfigs;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new LuceneQueryExpressionEvaluator(context.blockFactory(), shardConfigs, BooleanScoreVectorBuilder::new);
        }
    }

    private static class BooleanScoreVectorBuilder implements ScoreVectorBuilder {

        private final BlockFactory blockFactory;
        private final int size;

        private BooleanVector.Builder builder;

        BooleanScoreVectorBuilder(BlockFactory blockFactory, int size) {
            this.blockFactory = blockFactory;
            this.size = size;
        }

        @Override
        public Vector createNoMatchVector() {
            return blockFactory.newConstantBooleanVector(false, size);
        }

        @Override
        public void initVector() {
            builder = blockFactory.newBooleanVectorBuilder(size);
        }

        @Override
        public void appendNoMatch() {
            assert builder != null : "appendNoMatch called before initVector";
            builder.appendBoolean(false);
        }

        @Override
        public void appendMatch(Scorable scorer) {
            assert builder != null : "appendMatch called before initVector";
            builder.appendBoolean(true);
        }

        @Override
        public Vector build() {
            assert builder != null : "build called before initVector";
            return builder.build();
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(builder);
        }
    }
}
