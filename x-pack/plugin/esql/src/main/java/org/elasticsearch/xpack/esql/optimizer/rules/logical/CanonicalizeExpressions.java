/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

public class CanonicalizeExpressions extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public CanonicalizeExpressions() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected Expression rule(Expression expression, LogicalOptimizerContext ctx) {
        Expression canonical = expression.canonical();
        return canonical == expression ? expression : canonical;
    }
}
