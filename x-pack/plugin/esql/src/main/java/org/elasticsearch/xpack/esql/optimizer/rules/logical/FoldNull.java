/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

public class FoldNull extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public FoldNull() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(Expression e, LogicalOptimizerContext ctx) {
        Expression result = tryReplaceIsNullIsNotNull(e);

        // convert an aggregate null filter into a false
        // perform this early to prevent the rule from converting the null filter into nullifying the whole expression
        // P.S. this could be done inside the Aggregate but this place better centralizes the logic
        if (e instanceof AggregateFunction agg) {
            if (Expressions.isGuaranteedNull(agg.filter())) {
                return agg.withFilter(Literal.of(agg.filter(), false));
            }
        }

        if (result != e) {
            return result;
        } else if (e instanceof In in) {
            if (Expressions.isGuaranteedNull(in.value())) {
                return Literal.of(in, null);
            }
        } else if (e instanceof Alias == false && e.nullable() == Nullability.TRUE
        // Non-evaluatable functions stay as a STATS grouping (It isn't moved to an early EVAL like other groupings),
        // so folding it to null would currently break the plan, as we don't create an attribute/channel for that null value.
            && e instanceof GroupingFunction.NonEvaluatableGroupingFunction == false
            && Expressions.anyMatch(e.children(), Expressions::isGuaranteedNull)) {
                return Literal.of(e, null);
            }
        return e;
    }

    protected Expression tryReplaceIsNullIsNotNull(Expression e) {
        return e;
    }
}
