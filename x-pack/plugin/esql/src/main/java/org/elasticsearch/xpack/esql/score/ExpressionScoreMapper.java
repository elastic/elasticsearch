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
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;

/**
 * Allows for transforming expressions into their corresponding scores: expressions that contribute to the score map
 * to an {@link ExpressionEvaluator} that evaluates a {@link org.elasticsearch.compute.data.DoubleBlock} of per-row
 * scores.
 */
public interface ExpressionScoreMapper {
    interface ToScorer {
        ExpressionEvaluator.Factory toScorer(Expression expression);

        /**
         * The regular (non-scoring) evaluator context, used by scorers that need to evaluate their inputs
         * (e.g. the field values of a runtime full-text function) or resolve analyzers by name.
         */
        EvaluatorMapper.ToEvaluator toEvaluator();

        default IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts() {
            throw new UnsupportedOperationException("Shard contexts should only be needed for scoring operations");
        }
    }

    ExpressionEvaluator.Factory toScorer(ToScorer toScorer);

    default boolean contributesToScore() {
        return true;
    }
}
