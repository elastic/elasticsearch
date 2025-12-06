/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

/**
 * Replace {@link SurrogateExpression}s with their {@link SurrogateExpression#surrogate surrogates}.
 */
public final class SubstituteSurrogateExpressions extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public SubstituteSurrogateExpressions() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected Expression rule(Expression e, LogicalOptimizerContext ctx) {
        return rule(e);
    }

    /**
     * Perform the actual substitution.
     */
    public static Expression rule(Expression e) {
        if (e instanceof SurrogateExpression s) {
            Expression surrogate = s.surrogate();
            if (surrogate != null) {
                return surrogate;
            }
        }
        return e;
    }
}
