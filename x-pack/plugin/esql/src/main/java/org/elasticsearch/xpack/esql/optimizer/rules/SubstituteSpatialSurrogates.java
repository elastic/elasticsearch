/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;

/**
 * Currently this works similarly to SurrogateExpression, leaving the logic inside the expressions,
 * so each can decide for itself whether or not to change to a surrogate expression.
 * But what is actually being done is similar to LiteralsOnTheRight. We can consider in the future moving
 * this in either direction, reducing the number of rules, but for now,
 * it's a separate rule to reduce the risk of unintended interactions with other rules.
 */
public final class SubstituteSpatialSurrogates extends OptimizerRules.OptimizerExpressionRule<SpatialRelatesFunction> {

    public SubstituteSpatialSurrogates() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected SpatialRelatesFunction rule(SpatialRelatesFunction function) {
        return function.surrogate();
    }
}
