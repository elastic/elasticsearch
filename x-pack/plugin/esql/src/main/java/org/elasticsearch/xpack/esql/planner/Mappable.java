/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Expressions that have a mapping to an {@link EvalOperator.ExpressionEvaluator}.
 */
public interface Mappable {
    Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator);
}
