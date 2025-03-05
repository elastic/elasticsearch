/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

import static org.elasticsearch.test.ESTestCase.randomBoolean;

/**
 * An {@link EvalOperator.ExpressionEvaluator} that evaluates to a constant boolean value.
 */
public record ConstantBooleanExpressionEvaluator(BlockFactory factory, boolean value) implements EvalOperator.ExpressionEvaluator {
    public static EvalOperator.ExpressionEvaluator.Factory factory(boolean value) {
        return ctx -> new ConstantBooleanExpressionEvaluator(ctx.blockFactory(), value);
    }

    @Override
    public Block eval(Page page) {
        if (randomBoolean()) {
            return factory.newConstantBooleanVector(value, page.getPositionCount()).asBlock();
        }
        try (BooleanVector.Builder builder = factory.newBooleanVectorFixedBuilder(page.getPositionCount())) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                builder.appendBoolean(value);
            }
            return builder.build().asBlock();
        }
    }

    @Override
    public void close() {}

}
