/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.score;

import org.elasticsearch.compute.operator.ScoreOperator.ExpressionScorer;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;

import java.util.List;

/**
 * Maps expressions that have a mapping to an {@link ExpressionScorer}. Allows for transforming expressions into their corresponding scores.
 */
public interface ExpressionScoreMapper {
    interface ToScorer {
        ExpressionScorer.Factory toScorer(Expression expression);

        default List<EsPhysicalOperationProviders.ShardContext> shardContexts() {
            throw new UnsupportedOperationException("Shard contexts should only be needed for scoring operations");
        }
    }

    ExpressionScorer.Factory toScorer(ToScorer toScorer);
}
