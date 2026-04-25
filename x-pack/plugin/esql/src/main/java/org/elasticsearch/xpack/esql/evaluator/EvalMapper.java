/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.planner.Layout;

public final class EvalMapper {
    private EvalMapper() {}

    public static ExpressionEvaluator.Factory toEvaluator(FoldContext foldCtx, Expression exp, Layout layout) {
        return toEvaluator(foldCtx, exp, layout, EmptyIndexedByShardId.instance());
    }

    /**
     * Provides an ExpressionEvaluator factory to evaluate an expression.
     *
     * @param foldCtx the fold context for folding expressions
     * @param exp the expression to generate an evaluator for
     * @param layout the mapping from attributes to channels
     * @param shardContexts the shard contexts, needed to generate queries for expressions that couldn't be pushed down to Lucene
     */
    public static ExpressionEvaluator.Factory toEvaluator(
        FoldContext foldCtx,
        Expression exp,
        Layout layout,
        IndexedByShardId<? extends ShardContext> shardContexts
    ) {
        if (exp instanceof EvaluatorMapper m) {
            return m.toEvaluator(new EvaluatorMapper.ToEvaluator() {
                @Override
                public ExpressionEvaluator.Factory apply(Expression expression) {
                    return toEvaluator(foldCtx, expression, layout, shardContexts);
                }

                @Override
                public FoldContext foldCtx() {
                    return foldCtx;
                }

                @Override
                public IndexedByShardId<? extends ShardContext> shardContexts() {
                    return shardContexts;
                }
            });
        }
        if (exp instanceof Attribute attr) {
            return new LoadFromPageEvaluator.Factory(layout.get(attr.id()).channel());
        }
        throw new QlIllegalArgumentException("Unsupported expression [{}]", exp);
    }
}
