/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

public final class ConstantFolding extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public ConstantFolding() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public Expression rule(Expression e, LogicalOptimizerContext ctx) {
        return e.foldable() ? Literal.of(ctx.foldCtx(), e) : e;
    }
}
