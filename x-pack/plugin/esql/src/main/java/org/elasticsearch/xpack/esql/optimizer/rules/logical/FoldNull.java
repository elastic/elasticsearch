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
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

public class FoldNull extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public FoldNull() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(Expression e, LogicalOptimizerContext ctx) {
        if (e instanceof AggregateFunction agg) {
            // AggregateMapper cannot handle aggregate functions with literal values.
            // Aggregates over null inputs are instead replaced with a literal by ReplaceStatsFilteredOrNullAggWithEval.
            // Convert an aggregate null filter into a false if possible.
            if (Expressions.isGuaranteedNull(agg.filter())) {
                e = agg.withFilter(Literal.of(agg.filter(), false));
            }
        }
        if (e instanceof EvaluatorMapper
            && (Expressions.isGuaranteedNull(e)
                || (e instanceof AnyNullIsNull && e.children().stream().anyMatch(Expressions::isGuaranteedNull)))) {
            return Literal.of(e, null);
        }
        return e;
    }
}
