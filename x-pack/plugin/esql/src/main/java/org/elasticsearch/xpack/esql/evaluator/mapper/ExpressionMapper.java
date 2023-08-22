/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

import java.util.function.Supplier;

public abstract class ExpressionMapper<E extends Expression> {
    public final Class<E> typeToken;

    public ExpressionMapper() {
        typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());
    }

    public abstract Supplier<EvalOperator.ExpressionEvaluator> map(E expression, Layout layout);
}
