/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

/**
 * Expressions that have a mapping to an {@link EvalOperator.ExpressionEvaluator}.
 */
public interface Mappable {
    Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator);

    /**
     * Fold using {@link #toEvaluator} so you don't need a "by hand"
     * implementation of fold. The evaluator that it makes is "funny"
     * in that it'll always call {@link Expression#fold}, but that's
     * good enough.
     */
    default Object fold() {
        return toJavaObject(toEvaluator(e -> () -> p -> fromArrayRow(e.fold())[0]).get().eval(new Page(1)), 0);
    }
}
