/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;
import java.util.BitSet;
// end generated imports

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link In}.
 * This class is generated. Edit {@code X-InEvaluator.java.st} instead.
 */
public class InMillisNanosEvaluator implements EvalOperator.ExpressionEvaluator {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator lhs;

    private final EvalOperator.ExpressionEvaluator[] rhs;

    private final DriverContext driverContext;

    private Warnings warnings;

    public InMillisNanosEvaluator(
        Source source,
        EvalOperator.ExpressionEvaluator lhs,
        EvalOperator.ExpressionEvaluator[] rhs,
        DriverContext driverContext
    ) {
        this.source = source;
        this.lhs = lhs;
        this.rhs = rhs;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        try (LongBlock lhsBlock = (LongBlock) lhs.eval(page)) {
            LongBlock[] rhsBlocks = new LongBlock[rhs.length];
            try (Releasable rhsRelease = Releasables.wrap(rhsBlocks)) {
                for (int i = 0; i < rhsBlocks.length; i++) {
                    rhsBlocks[i] = (LongBlock) rhs[i].eval(page);
                }
                LongVector lhsVector = lhsBlock.asVector();
                if (lhsVector == null) {
                    return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
                }
                LongVector[] rhsVectors = new LongVector[rhs.length];
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

    private BooleanBlock eval(int positionCount, LongBlock lhsBlock, LongBlock[] rhsBlocks) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            long[] rhsValues = new long[rhs.length];
            BitSet nulls = new BitSet(rhs.length);
            BitSet mvs = new BitSet(rhs.length);
            boolean foundMatch;
            for (int p = 0; p < positionCount; p++) {
                if (lhsBlock.isNull(p)) {
                    result.appendNull();
                    continue;
                }
                if (lhsBlock.getValueCount(p) != 1) {
                    if (lhsBlock.getValueCount(p) > 1) {
                        warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
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
                        warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                        continue;
                    }
                    int o = rhsBlocks[i].getFirstValueIndex(p);
                    rhsValues[i] = rhsBlocks[i].getLong(o);
                }
                if (nulls.cardinality() == rhsBlocks.length || mvs.cardinality() == rhsBlocks.length) {
                    result.appendNull();
                    continue;
                }
                foundMatch = In.process(nulls, mvs, lhsBlock.getLong(lhsBlock.getFirstValueIndex(p)), rhsValues);
                if (foundMatch) {
                    result.appendBoolean(true);
                } else {
                    if (nulls.cardinality() > 0) {
                        result.appendNull();
                    } else {
                        result.appendBoolean(false);
                    }
                }
            }
            return result.build();
        }
    }

    private BooleanBlock eval(int positionCount, LongVector lhsVector, LongVector[] rhsVectors) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            long[] rhsValues = new long[rhs.length];
            for (int p = 0; p < positionCount; p++) {
                // unpack rhsVectors into rhsValues
                for (int i = 0; i < rhsVectors.length; i++) {
                    rhsValues[i] = rhsVectors[i].getLong(p);
                }
                result.appendBoolean(In.processMillisNanos(null, null, lhsVector.getLong(p), rhsValues));
            }
            return result.build();
        }
    }

    @Override
    public String toString() {
        return "InMillisNanosEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lhs, () -> Releasables.close(rhs));
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
        public InMillisNanosEvaluator get(DriverContext context) {
            EvalOperator.ExpressionEvaluator[] rhs = Arrays.stream(this.rhs)
                .map(a -> a.get(context))
                .toArray(EvalOperator.ExpressionEvaluator[]::new);
            return new InMillisNanosEvaluator(source, lhs.get(context), rhs, context);
        }

        @Override
        public String toString() {
            return "InMillisNanosEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
        }
    }
}
