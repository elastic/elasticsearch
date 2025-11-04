/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

/**
 * Ensures that vector similarity functions are in their canonical form, with literals to the right.
 */
public class CanonicalizeVectorSimilarityFunctions extends OptimizerRules.OptimizerExpressionRule<VectorSimilarityFunction> {

    public CanonicalizeVectorSimilarityFunctions() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected Expression rule(VectorSimilarityFunction vectorSimilarityFunction, LogicalOptimizerContext ctx) {
        return vectorSimilarityFunction.canonical();
    }
}
