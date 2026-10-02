/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.score;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

/**
 * Maps an expression tree into an {@link ExpressionEvaluator.Factory} that evaluates its scores.
 */
public class ScoreMapper {

    public static ExpressionEvaluator.Factory toScorer(
        Expression expression,
        IndexedByShardId<? extends ShardContext> shardContexts,
        EvaluatorMapper.ToEvaluator toEvaluator
    ) {
        if (expression instanceof ExpressionScoreMapper mapper && mapper.contributesToScore()) {
            return mapper.toScorer(new ExpressionScoreMapper.ToScorer() {
                @Override
                public ExpressionEvaluator.Factory toScorer(Expression expression) {
                    return ScoreMapper.toScorer(expression, shardContexts, toEvaluator);
                }

                @Override
                public EvaluatorMapper.ToEvaluator toEvaluator() {
                    return toEvaluator;
                }

                @Override
                public IndexedByShardId<? extends ShardContext> shardContexts() {
                    return shardContexts;
                }
            });
        }

        return new DefaultScoreMapper();
    }

    public static class DefaultScoreMapper implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext driverContext) {
            return new ExpressionEvaluator() {
                @Override
                public DoubleBlock eval(Page page) {
                    return driverContext.blockFactory().newConstantDoubleBlockWith(0.0, page.getPositionCount());
                }

                @Override
                public long baseRamBytesUsed() {
                    return 0;
                }

                @Override
                public void close() {}
            };
        }
    }
}
