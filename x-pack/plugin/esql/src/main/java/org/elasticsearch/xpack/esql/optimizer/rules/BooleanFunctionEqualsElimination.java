/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;

/**
 * This rule must always be placed after {@link LiteralsOnTheRight}
 * since it looks at TRUE/FALSE literals' existence on the right hand-side of the {@link Equals}/{@link NotEquals} expressions.
 */
public final class BooleanFunctionEqualsElimination extends OptimizerRules.OptimizerExpressionRule<BinaryComparison> {

    public BooleanFunctionEqualsElimination() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(BinaryComparison bc) {
        if ((bc instanceof Equals || bc instanceof NotEquals) && bc.left() instanceof Function) {
            // for expression "==" or "!=" TRUE/FALSE, return the expression itself or its negated variant

            // TODO: Replace use of QL Not with ESQL Not
            if (TRUE.equals(bc.right())) {
                return bc instanceof Equals ? bc.left() : new Not(bc.left().source(), bc.left());
            }
            if (FALSE.equals(bc.right())) {
                return bc instanceof Equals ? new Not(bc.left().source(), bc.left()) : bc.left();
            }
        }

        return bc;
    }
}
