/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

import java.util.Arrays;
import java.util.BitSet;

/**
 * This class is generated. Edit {@code X-In.java.st} instead.
 */
public class InBooleanEvaluator implements EvalOperator.ExpressionEvaluator {
    private final Warnings warnings;

    private final EvalOperator.ExpressionEvaluator lhs;

    private final EvalOperator.ExpressionEvaluator[] rhs;

    private final DriverContext driverContext;

    public InBooleanEvaluator(
        Source source,
        EvalOperator.ExpressionEvaluator lhs,
        EvalOperator.ExpressionEvaluator[] rhs,
        DriverContext driverContext
    ) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.driverContext = driverContext;
        this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }

    @Override
    public Block eval(Page page) {
        try (BooleanBlock lhsBlock = (BooleanBlock) lhs.eval(page)) {
            BooleanBlock[] rhsBlocks = new BooleanBlock[rhs.length];
            try (Releasable rhsRelease = Releasables.wrap(rhsBlocks)) {
                for (int i = 0; i < rhsBlocks.length; i++) {
                    rhsBlocks[i] = (BooleanBlock) rhs[i].eval(page);
                }
                BooleanVector lhsVector = lhsBlock.asVector();
                if (lhsVector == null) {
                    return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
                }
                BooleanVector[] rhsVectors = new BooleanVector[rhs.length];
                for (int i = 0; i < rhsBlocks.length; i++) {
                    rhsVectors[i] = rhsBlocks[i].asVector();
                    if (rhsVectors[i] == null) {
                        return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
                    }
                }
                return eval(page.getPositionCount(), lhsVector, rhsVectors);
            }
        }
    }

    public BooleanBlock eval(int positionCount, BooleanBlock lhsBlock, BooleanBlock[] rhsBlocks) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            boolean[] rhsValues = new boolean[rhs.length];
            BitSet nulls = new BitSet(rhs.length);
            BitSet mvs = new BitSet(rhs.length);
            for (int p = 0; p < positionCount; p++) {
                if (lhsBlock.isNull(p)) {
                    result.appendNull();
                    continue;
                }
                if (lhsBlock.getValueCount(p) != 1) {
                    if (lhsBlock.getValueCount(p) > 1) {
                        warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                    }
                    result.appendNull();
                    continue;
                }
                // unpack rhsBlocks into rhsValues
                nulls.clear();
                mvs.clear();
                for (int i = 0; i < rhsBlocks.length; i++) {
                    if (rhsBlocks[i].isNull(p)) {
                        nulls.set(i);
                        continue;
                    }
                    if (rhsBlocks[i].getValueCount(p) > 1) {
                        mvs.set(i);
                        warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                        continue;
                    }
                    int o = rhsBlocks[i].getFirstValueIndex(p);
                    rhsValues[i] = rhsBlocks[i].getBoolean(o);
                }
                if (nulls.cardinality() == rhsBlocks.length || mvs.cardinality() == rhsBlocks.length) {
                    result.appendNull();
                    continue;
                }
                try {
                    In.process(result, nulls, mvs, lhsBlock.getBoolean(lhsBlock.getFirstValueIndex(p)), rhsValues);
                } catch (IllegalArgumentException e) {
                    warnings.registerException(e);
                    result.appendNull();
                }
            }
            return result.build();
        }
    }

    public BooleanBlock eval(int positionCount, BooleanVector lhsVector, BooleanVector[] rhsVectors) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            boolean[] rhsValues = new boolean[rhs.length];
            for (int p = 0; p < positionCount; p++) {
                // unpack rhsVectors into rhsValues
                for (int i = 0; i < rhsVectors.length; i++) {
                    rhsValues[i] = rhsVectors[i].getBoolean(p);
                }
                try {
                    In.process(result, null, null, lhsVector.getBoolean(p), rhsValues);
                } catch (IllegalArgumentException e) {
                    warnings.registerException(e);
                    result.appendNull();
                }
            }
            return result.build();
        }
    }

    @Override
    public String toString() {
        return "InBooleanEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lhs, () -> Releasables.close(rhs));
    }

    static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory lhs;
        private final EvalOperator.ExpressionEvaluator.Factory[] rhs;

        Factory(Source source, EvalOperator.ExpressionEvaluator.Factory lhs, EvalOperator.ExpressionEvaluator.Factory[] rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public InBooleanEvaluator get(DriverContext context) {
            EvalOperator.ExpressionEvaluator[] rhs = Arrays.stream(this.rhs)
                .map(a -> a.get(context))
                .toArray(EvalOperator.ExpressionEvaluator[]::new);
            return new InBooleanEvaluator(source, lhs.get(context), rhs, context);
        }

        @Override
        public String toString() {
            return "InBooleanEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
        }
    }
}
