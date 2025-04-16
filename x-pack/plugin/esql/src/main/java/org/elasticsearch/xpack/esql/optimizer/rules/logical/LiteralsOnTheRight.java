/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

public final class LiteralsOnTheRight extends OptimizerRules.OptimizerExpressionRule<BinaryOperator<?, ?, ?, ?>> {

    public LiteralsOnTheRight() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public BinaryOperator<?, ?, ?, ?> rule(BinaryOperator<?, ?, ?, ?> be, LogicalOptimizerContext ctx) {
        return be.left() instanceof Literal && (be.right() instanceof Literal) == false ? be.swapLeftAndRight() : be;
    }
}
