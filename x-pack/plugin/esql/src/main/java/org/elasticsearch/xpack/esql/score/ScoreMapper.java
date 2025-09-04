/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.score;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.ScoreOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

import java.util.List;

/**
 * Maps an expression tree into ExpressionScorer.Factory, so scores can be evaluated for an expression tree.
 */
public class ScoreMapper {

    public static ScoreOperator.ExpressionScorer.Factory toScorer(Expression expression, List<ShardContext> shardContexts) {
        if (expression instanceof ExpressionScoreMapper mapper) {
            return mapper.toScorer(new ExpressionScoreMapper.ToScorer() {
                @Override
                public ScoreOperator.ExpressionScorer.Factory toScorer(Expression expression) {
                    return ScoreMapper.toScorer(expression, shardContexts);
                }

                @Override
                public List<ShardContext> shardContexts() {
                    return shardContexts;
                }
            });
        }

        return page -> new DefaultScoreMapper().get(page);
    }

    public static class DefaultScoreMapper implements ScoreOperator.ExpressionScorer.Factory {
        @Override
        public ScoreOperator.ExpressionScorer get(DriverContext driverContext) {
            return new ScoreOperator.ExpressionScorer() {
                @Override
                public DoubleBlock score(Page page) {
                    return driverContext.blockFactory().newConstantDoubleBlockWith(0.0, page.getPositionCount());
                }

                @Override
                public void close() {}
            };
        }
    }
}
