/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.planner.Layout;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

/**
 * Expressions that have a mapping to an {@link ExpressionEvaluator}.
 */
public interface EvaluatorMapper {
    interface ToEvaluator {
        ExpressionEvaluator.Factory apply(Expression expression);
    }

    /**
     * <p>
     * Note for implementors:
     * If you are implementing this function, you should call the passed-in
     * lambda on your children, after doing any other manipulation (casting,
     * etc.) necessary.
     * </p>
     * <p>
     * Note for Callers:
     * If you are attempting to call this method, and you have an
     * {@link Expression} and a {@link org.elasticsearch.xpack.esql.planner.Layout},
     * you likely want to call {@link org.elasticsearch.xpack.esql.evaluator.EvalMapper#toEvaluator(Expression, Layout)}
     * instead.  On the other hand, if you already have something that
     * looks like the parameter for this method, you should call this method
     * with that function.
     * </p>
     * <p>
     * Build an {@link ExpressionEvaluator.Factory} for the tree of
     * expressions rooted at this node. This is only guaranteed to return
     * a sensible evaluator if this node has a valid type. If this node
     * is a subclass of {@link Expression} then "valid type" means that
     * {@link Expression#typeResolved} returns a non-error resolution.
     * If {@linkplain Expression#typeResolved} returns an error then
     * this method may throw. Or return an evaluator that produces
     * garbage. Or return an evaluator that throws when run.
     * </p>
     */
    ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator);

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
                return fromArrayRow(driverContext.blockFactory(), e.fold())[0];
            }

            @Override
            public void close() {}
        }).get(DriverContext.getLocalDriver()).eval(new Page(1)), 0);
    }
}
