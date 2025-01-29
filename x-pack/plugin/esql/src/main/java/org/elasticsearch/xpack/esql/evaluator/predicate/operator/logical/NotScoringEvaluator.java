/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.BooleanToScoringExpressionEvaluator;

import static org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator.SCORE_FOR_FALSE;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Not} when scores are used.
 * It returns 0.0 for false and {@link org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator#SCORE_FOR_FALSE} for true.
 */
public final class NotScoringEvaluator implements EvalOperator.ExpressionEvaluator {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator v;

    private final DriverContext driverContext;

    private Warnings warnings;

    public NotScoringEvaluator(Source source, EvalOperator.ExpressionEvaluator v, DriverContext driverContext) {
        this.source = source;
        this.v = v;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        try (DoubleBlock vBlock = (DoubleBlock) v.eval(page)) {
            DoubleVector vVector = vBlock.asVector();
            if (vVector == null) {
                return eval(page.getPositionCount(), vBlock);
            }
            return eval(page.getPositionCount(), vVector).asBlock();
        }
    }

    public DoubleBlock eval(int positionCount, DoubleBlock vBlock) {
        try (DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
            position: for (int p = 0; p < positionCount; p++) {
                if (vBlock.isNull(p)) {
                    result.appendNull();
                    continue position;
                }
                if (vBlock.getValueCount(p) != 1) {
                    if (vBlock.getValueCount(p) > 1) {
                        warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                    }
                    result.appendNull();
                    continue position;
                }
                result.appendDouble(evaluate(vBlock.getDouble(vBlock.getFirstValueIndex(p))));
            }
            return result.build();
        }
    }

    private static Double evaluate(Double v) {
        return v == SCORE_FOR_FALSE ? 0.0 : SCORE_FOR_FALSE;
    }

    public DoubleVector eval(int positionCount, DoubleVector vVector) {
        try (DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
            position: for (int p = 0; p < positionCount; p++) {
                result.appendDouble(evaluate(vVector.getDouble(p)));
            }
            return result.build();
        }
    }

    @Override
    public String toString() {
        return "NotEvaluator[" + "v=" + v + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(v);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = Warnings.createWarnings(
                driverContext.warningsMode(),
                source.source().getLineNumber(),
                source.source().getColumnNumber(),
                source.text()
            );
        }
        return warnings;
    }

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;

        private final EvalOperator.ExpressionEvaluator.Factory v;

        public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory v) {
            this.source = source;
            this.v = v;
        }

        @Override
        public NotScoringEvaluator get(DriverContext context) {
            return new NotScoringEvaluator(source, new BooleanToScoringExpressionEvaluator(v.get(context), context), context);
        }

        @Override
        public String toString() {
            return "NotEvaluator[" + "v=" + v + "]";
        }
    }
}
