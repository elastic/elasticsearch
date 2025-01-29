/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.OptimizerExpressionRule;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection;

/**
 * This rule should not be needed - the substitute infrastructure should be enough.
 */
public class SubstituteFilteredExpression extends OptimizerExpressionRule<FilteredExpression> {
    public SubstituteFilteredExpression() {
        super(TransformDirection.UP);
    }

    @Override
    protected Expression rule(FilteredExpression filteredExpression, LogicalOptimizerContext ctx) {
        return filteredExpression.surrogate();
    }
}
