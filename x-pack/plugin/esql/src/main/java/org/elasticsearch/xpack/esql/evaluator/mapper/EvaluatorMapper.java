/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.ThrowingDriverContext;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.function.Function;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

/**
 * Expressions that have a mapping to an {@link ExpressionEvaluator}.
 */
public interface EvaluatorMapper {
    ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator);

    /**
     * Fold using {@link #toEvaluator} so you don't need a "by hand"
     * implementation of fold. The evaluator that it makes is "funny"
     * in that it'll always call {@link Expression#fold}, but that's
     * good enough.
     */
    default Object fold() {
        return toJavaObject(toEvaluator(e -> driverContext -> new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                return fromArrayRow(e.fold())[0];
            }

            @Override
            public void close() {}
        }).get(new ThrowingDriverContext() {
            @Override
            public CircuitBreaker breaker() {
                // TODO maybe this should have a small fixed limit?
                return new NoopCircuitBreaker(CircuitBreaker.REQUEST);
            }
        }).eval(new Page(1)), 0);
    }
}
