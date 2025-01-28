/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

import static org.elasticsearch.compute.lucene.LuceneQueryExpressionScoringEvaluator.SCORE_FOR_FALSE;

public class BooleanToScoringExpressionEvaluator implements EvalOperator.ExpressionEvaluator {

    private final EvalOperator.ExpressionEvaluator inner;
    private final BlockFactory blockFactory;

    public BooleanToScoringExpressionEvaluator(EvalOperator.ExpressionEvaluator inner, BlockFactory blockFactory) {
        this.inner = inner;
        this.blockFactory = blockFactory;
    }

    @Override
    public Block eval(Page page) {
        Block innerBlock = inner.eval(page);
        if ((innerBlock == null) || innerBlock.areAllValuesNull()) {
            return innerBlock;
        }

        if (innerBlock instanceof BooleanBlock == false) {
            throw new IllegalArgumentException("Unexpected block type: " + innerBlock.getClass());
        }

        BooleanBlock booleanBlock = (BooleanBlock) innerBlock;
        int positionCount = booleanBlock.getPositionCount();
        DoubleVector.FixedBuilder builder = blockFactory.newDoubleVectorFixedBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            boolean value = booleanBlock.getBoolean(i);
            if (value) {
                builder.appendDouble(1.0);
            } else {
                builder.appendDouble(SCORE_FOR_FALSE);
            }
        }

        return builder.build().asBlock();
    }

    @Override
    public void close() {
        inner.close();
    }

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {

        private final EvalOperator.ExpressionEvaluator.Factory innerFactory;

        public Factory(EvalOperator.ExpressionEvaluator.Factory innerFactory) {
            this.innerFactory = innerFactory;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new BooleanToScoringExpressionEvaluator(innerFactory.get(context), context.blockFactory());
        }
    }
}
