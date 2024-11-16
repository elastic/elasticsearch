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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;

public class FoldNull extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public FoldNull() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(Expression e) {
        Expression result = tryReplaceIsNullIsNotNull(e);

        // convert an aggregate null filter into a false
        // perform this early to prevent the rule from converting the null filter into nullifying the whole expression
        // P.S. this could be done inside the Aggregate but this place better centralizes the logic
        if (e instanceof AggregateFunction agg) {
            if (Expressions.isNull(agg.filter())) {
                return agg.withFilter(Literal.of(agg.filter(), false));
            }
        }

        if (result != e) {
            return result;
        } else if (e instanceof In in) {
            if (Expressions.isNull(in.value())) {
                return Literal.of(in, null);
            }
        } else if (e instanceof Alias == false
            && e.nullable() == Nullability.TRUE
            && Expressions.anyMatch(e.children(), Expressions::isNull)) {
                return Literal.of(e, null);
            }
        return e;
    }

    protected Expression tryReplaceIsNullIsNotNull(Expression e) {
        return e;
    }
}
