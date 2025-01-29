/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

import static org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator.SCORE_FOR_FALSE;

/**
 * Converts a {@link BooleanBlock} to a {@link DoubleBlock} if needed. This is needed to convert boolean expressions
 * into scores that can be mixed together with other scoring expressions, or just for boolean expressions to be scored.
 * Scores are converted using 1.0 for true and {* @link LuceneQueryExpressionEvaluator#SCORE_FOR_FALSE} for false.
 */
public class BooleanToScoringExpressionEvaluator implements EvalOperator.ExpressionEvaluator {

    public static final double SCORE_FOR_TRUE = 0.0;

    private final EvalOperator.ExpressionEvaluator inner;
    private final DriverContext driverContext;

    public BooleanToScoringExpressionEvaluator(EvalOperator.ExpressionEvaluator inner, DriverContext driverContext) {
        this.inner = inner;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        Block innerBlock = inner.eval(page);
        if ((innerBlock == null) || innerBlock.areAllValuesNull()) {
            return innerBlock;
        }

        if (innerBlock instanceof DoubleBlock) {
            // It's already a scoring block - no need to convert
            return innerBlock;
        }

        if (innerBlock instanceof BooleanBlock == false) {
            throw new IllegalArgumentException("Unexpected block type: " + innerBlock.getClass());
        }

        return eval((BooleanBlock) innerBlock);
    }

    private DoubleBlock eval(BooleanBlock booleanBlock) {
        DoubleBlock.Builder builder = null;
        try {
            int positionCount = booleanBlock.getPositionCount();
            builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount);
            for (int i = 0; i < positionCount; i++) {
                boolean value = booleanBlock.getBoolean(i);
                if (value) {
                    builder.appendDouble(SCORE_FOR_TRUE);
                } else {
                    builder.appendDouble(SCORE_FOR_FALSE);
                }
            }

            return builder.build();
        } finally {
            Releasables.closeExpectNoException(builder, booleanBlock);
        }
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(inner);
    }

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {

        private final EvalOperator.ExpressionEvaluator.Factory innerFactory;

        public Factory(EvalOperator.ExpressionEvaluator.Factory innerFactory) {
            this.innerFactory = innerFactory;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new BooleanToScoringExpressionEvaluator(innerFactory.get(context), context);
        }
    }
}
