/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.JitConstantSpinner;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.lang.reflect.InvocationTargetException;

public abstract class DivDoublesByConstantJitEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DivDoublesByConstantJitEvaluator.class);

    private final Source source;
    private final ExpressionEvaluator lhs;
    private final DriverContext driverContext;
    private Warnings warnings;

    protected DivDoublesByConstantJitEvaluator(Source source, ExpressionEvaluator lhs, DriverContext driverContext) {
        this.source = source;
        this.lhs = lhs;
        this.driverContext = driverContext;
    }

    protected abstract double rhs();

    @Override
    public final Block eval(Page page) {
        try (DoubleBlock lhsBlock = (DoubleBlock) lhs.eval(page)) {
            DoubleVector lhsVector = lhsBlock.asVector();
            if (lhsVector == null) {
                return eval(page.getPositionCount(), lhsBlock);
            }
            return eval(page.getPositionCount(), lhsVector);
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + lhs.baseRamBytesUsed();
    }

    public final DoubleBlock eval(int positionCount, DoubleBlock lhsBlock) {
        try (DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
            position: for (int p = 0; p < positionCount; p++) {
                switch (lhsBlock.getValueCount(p)) {
                    case 0:
                        result.appendNull();
                        continue position;
                    case 1:
                        break;
                    default:
                        warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                        result.appendNull();
                        continue position;
                }
                double lhsVal = lhsBlock.getDouble(lhsBlock.getFirstValueIndex(p));
                try {
                    result.appendDouble(Div.processDoublesByConstant(lhsVal, rhs()));
                } catch (ArithmeticException e) {
                    warnings().registerException(e);
                    result.appendNull();
                }
            }
            return result.build();
        }
    }

    public final DoubleBlock eval(int positionCount, DoubleVector lhsVector) {
        try (DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                double lhsVal = lhsVector.getDouble(p);
                try {
                    result.appendDouble(Div.processDoublesByConstant(lhsVal, rhs()));
                } catch (ArithmeticException e) {
                    warnings().registerException(e);
                    result.appendNull();
                }
            }
            return result.build();
        }
    }

    @Override
    public final String toString() {
        return "DivDoublesByConstantJitEvaluator[lhs=" + lhs + ", rhs=" + rhs() + "]";
    }

    @Override
    public final void close() {
        Releasables.closeExpectNoException(lhs);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
        }
        return warnings;
    }

    public static final class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final ExpressionEvaluator.Factory lhs;
        private final double rhs;

        public Factory(Source source, ExpressionEvaluator.Factory lhs, double rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DivDoublesByConstantJitEvaluator get(DriverContext context) {
            Class<? extends DivDoublesByConstantJitEvaluator> spunClass = JitConstantSpinner.doubleConstantSubclass(
                DivDoublesByConstantJitEvaluator.class,
                "rhs",
                rhs
            );
            try {
                return spunClass.getDeclaredConstructor(Source.class, ExpressionEvaluator.class, DriverContext.class)
                    .newInstance(source, lhs.get(context), context);
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new IllegalStateException("failed to construct spun evaluator for divisor " + rhs, e);
            }
        }

        @Override
        public String toString() {
            return "DivDoublesByConstantJitEvaluator[lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }
}
