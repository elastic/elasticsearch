/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;

public final class ConstantFolding extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public ConstantFolding() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public Expression rule(Expression e) {
        return e.foldable() ? Literal.of(e) : e;
    }
}
