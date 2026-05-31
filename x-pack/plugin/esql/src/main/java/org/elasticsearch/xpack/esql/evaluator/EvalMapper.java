/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;

public final class EvalMapper {
    private EvalMapper() {}

    public static ExpressionEvaluator.Factory toEvaluator(FoldContext foldCtx, Expression exp, Layout layout) {
        return toEvaluator(foldCtx, exp, layout, EmptyIndexedByShardId.instance());
    }

    /**
     * Provides an ExpressionEvaluator factory to evaluate an expression that does not require shard contexts
     * but may reference functions (e.g. {@code TOP_SNIPPETS}) that need the node-level {@link AnalysisRegistry}.
     */
    public static ExpressionEvaluator.Factory toEvaluator(
        FoldContext foldCtx,
        Expression exp,
        Layout layout,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        return toEvaluator(foldCtx, exp, layout, EmptyIndexedByShardId.instance(), analysisRegistry);
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
        return toEvaluator(foldCtx, exp, layout, shardContexts, null);
    }

    /**
     * Provides an ExpressionEvaluator factory to evaluate an expression.
     *
     * @param foldCtx the fold context for folding expressions
     * @param exp the expression to generate an evaluator for
     * @param layout the mapping from attributes to channels
     * @param shardContexts the shard contexts, needed to generate queries for expressions that couldn't be pushed down to Lucene
     * @param analysisRegistry the node-level analysis registry for resolving analyzers by name, may be {@code null}
     */
    public static ExpressionEvaluator.Factory toEvaluator(
        FoldContext foldCtx,
        Expression exp,
        Layout layout,
        IndexedByShardId<? extends ShardContext> shardContexts,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        if (exp instanceof EvaluatorMapper m) {
            return m.toEvaluator(new EvaluatorMapper.ToEvaluator() {
                @Override
                public ExpressionEvaluator.Factory apply(Expression expression) {
                    return toEvaluator(foldCtx, expression, layout, shardContexts, analysisRegistry);
                }

                @Override
                public FoldContext foldCtx() {
                    return foldCtx;
                }

                @Override
                public IndexedByShardId<? extends ShardContext> shardContexts() {
                    return shardContexts;
                }

                @Override
                public Analyzer getAnalyzer(String name) {
                    if (analysisRegistry == null) {
                        throw new InvalidArgumentException("'analyzer' option cannot be resolved without an analysis registry");
                    }
                    Analyzer analyzer;
                    try {
                        analyzer = analysisRegistry.getAnalyzer(name);
                    } catch (IOException e) {
                        throw new InvalidArgumentException("failed to load analyzer [{}]", e, name);
                    }
                    if (analyzer == null) {
                        throw new InvalidArgumentException("'analyzer' must be a registered analyzer, found [{}]", name);
                    }
                    return analyzer;
                }
            });
        }
        if (exp instanceof Attribute attr) {
            return new LoadFromPageEvaluator.Factory(layout.get(attr.id()).channel());
        }
        throw new QlIllegalArgumentException("Unsupported expression [{}]", exp);
    }
}
