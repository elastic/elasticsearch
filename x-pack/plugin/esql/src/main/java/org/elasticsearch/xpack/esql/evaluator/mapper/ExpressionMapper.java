/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.util.ReflectionUtils;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.List;

public abstract class ExpressionMapper<E extends Expression> {
    public final Class<E> typeToken;

    public ExpressionMapper() {
        typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());
    }

    public abstract ExpressionEvaluator.Factory map(FoldContext foldCtx, E expression, Layout layout, List<ShardContext> shardContexts);
}
