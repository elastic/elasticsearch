/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.score;

import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;

/**
 * Allows for transforming expressions into their corresponding scores.
 * Maps expressions that contribute to the score to an {@link ExpressionEvaluator} that evaluates the scores.
 */
public interface ExpressionScoreMapper {
    interface ToScorer {
        ExpressionEvaluator.Factory toScorer(Expression expression);

        ExpressionEvaluator.Factory toEvaluator(Expression expression);

        default IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts() {
            throw new UnsupportedOperationException("Shard contexts should only be needed for scoring operations");
        }
    }

    ExpressionEvaluator.Factory toScorer(ToScorer toScorer);

    default boolean contributesToScore() {
        return true;
    }
}
