/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

public class FoldNull extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public FoldNull() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(Expression e, LogicalOptimizerContext ctx) {
        if (e instanceof NamedExpression) {
            // Never replace NamedExpression with a literal null, because the name gets lost.
            return e;
        }
        if (e instanceof AggregateFunction agg) {
            // AggregateMapper cannot handle aggregate functions with literal values.
            // Aggregates over null inputs are instead replaced with a literal by ReplaceStatsFilteredOrNullAggWithEval.
            // Convert an aggregate null filter into a false if possible.
            if (isNull(agg.filter())) {
                return agg.withFilter(Literal.of(agg.filter(), false));
            } else {
                return agg;
            }
        }
        if (e instanceof In in) {
            // Instead of special-casing `In`, this could benefit from a marker
            // interface `FirstNullIsNull` or similar (comparable to `AnyNullIsNull`).
            // See also: https://github.com/elastic/elasticsearch/issues/159848
            if (isNull(in.value())) {
                return Literal.of(in, null);
            }
        }
        if (isNull(e)) {
            return Literal.of(e, null);
        }
        return e;
    }

    private static boolean isNull(Expression e) {
        return Expressions.isGuaranteedNull(e) || e instanceof AnyNullIsNull && e.children().stream().anyMatch(FoldNull::isNull);
    }
}
