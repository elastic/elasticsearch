/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.JitConstantSpinner;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.lang.reflect.InvocationTargetException;

/**
 * Constant-RHS fast path for {@code Div} on {@code long} arguments, JIT-folding shape.
 * See {@link ModLongsByConstantJitEvaluator} for the design rationale and PR #148678
 * for the investigation that motivated this shape.
 */
public abstract class DivLongsByConstantJitEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DivLongsByConstantJitEvaluator.class);

    private final Source source;
    private final ExpressionEvaluator lhs;
    private final DriverContext driverContext;
    private Warnings warnings;

    protected DivLongsByConstantJitEvaluator(Source source, ExpressionEvaluator lhs, DriverContext driverContext) {
        this.source = source;
        this.lhs = lhs;
        this.driverContext = driverContext;
    }

    protected abstract long rhs();

    @Override
    public final Block eval(Page page) {
        try (LongBlock lhsBlock = (LongBlock) lhs.eval(page)) {
            LongVector lhsVector = lhsBlock.asVector();
            if (lhsVector == null) {
                return eval(page.getPositionCount(), lhsBlock);
            }
            return eval(page.getPositionCount(), lhsVector).asBlock();
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + lhs.baseRamBytesUsed();
    }

    public final LongBlock eval(int positionCount, LongBlock lhsBlock) {
        try (LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
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
                long lhsVal = lhsBlock.getLong(lhsBlock.getFirstValueIndex(p));
                result.appendLong(Div.processLongsByConstant(lhsVal, rhs()));
            }
            return result.build();
        }
    }

    public final LongVector eval(int positionCount, LongVector lhsVector) {
        try (LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                long lhsVal = lhsVector.getLong(p);
                result.appendLong(p, Div.processLongsByConstant(lhsVal, rhs()));
            }
            return result.build();
        }
    }

    @Override
    public final String toString() {
        return "DivLongsByConstantJitEvaluator[lhs=" + lhs + ", rhs=" + rhs() + "]";
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
        private final long rhs;

        public Factory(Source source, ExpressionEvaluator.Factory lhs, long rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DivLongsByConstantJitEvaluator get(DriverContext context) {
            Class<? extends DivLongsByConstantJitEvaluator> spunClass = JitConstantSpinner.longConstantSubclass(
                DivLongsByConstantJitEvaluator.class,
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
            return "DivLongsByConstantJitEvaluator[lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }
}
