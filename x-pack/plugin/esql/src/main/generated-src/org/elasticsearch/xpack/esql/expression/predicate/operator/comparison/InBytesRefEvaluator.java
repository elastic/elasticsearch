/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link In}.
 * This class is generated. Edit {@code InEvaluator.java.st} instead.
 */
public class InBytesRefEvaluator implements EvalOperator.ExpressionEvaluator {
    private final Warnings warnings;

    private final EvalOperator.ExpressionEvaluator lhs;

    private final EvalOperator.ExpressionEvaluator[] rhs;

    private final DriverContext driverContext;

    public InBytesRefEvaluator(
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
        try (BytesRefBlock lhsBlock = (BytesRefBlock) lhs.eval(page)) {
            BytesRefBlock[] rhsBlocks = new BytesRefBlock[rhs.length];
            try (Releasable rhsRelease = Releasables.wrap(rhsBlocks)) {
                for (int i = 0; i < rhsBlocks.length; i++) {
                    rhsBlocks[i] = (BytesRefBlock) rhs[i].eval(page);
                }
                BytesRefVector lhsVector = lhsBlock.asVector();
                if (lhsVector == null) {
                    return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
                }
                BytesRefVector[] rhsVectors = new BytesRefVector[rhs.length];
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

    private BooleanBlock eval(int positionCount, BytesRefBlock lhsBlock, BytesRefBlock[] rhsBlocks) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            BytesRef[] rhsValues = new BytesRef[rhs.length];
            BytesRef lhsScratch = new BytesRef();
            BytesRef[] rhsScratch = new BytesRef[rhs.length];
            for (int i = 0; i < rhs.length; i++) {
                rhsScratch[i] = new BytesRef();
            }
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
                    rhsValues[i] = rhsBlocks[i].getBytesRef(o, rhsScratch[i]);
                }
                if (nulls.cardinality() == rhsBlocks.length || mvs.cardinality() == rhsBlocks.length) {
                    result.appendNull();
                    continue;
                }
                foundMatch = In.process(nulls, mvs, lhsBlock.getBytesRef(lhsBlock.getFirstValueIndex(p), lhsScratch), rhsValues);
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

    private BooleanBlock eval(int positionCount, BytesRefVector lhsVector, BytesRefVector[] rhsVectors) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            BytesRef[] rhsValues = new BytesRef[rhs.length];
            BytesRef lhsScratch = new BytesRef();
            BytesRef[] rhsScratch = new BytesRef[rhs.length];
            for (int i = 0; i < rhs.length; i++) {
                rhsScratch[i] = new BytesRef();
            }
            for (int p = 0; p < positionCount; p++) {
                // unpack rhsVectors into rhsValues
                for (int i = 0; i < rhsVectors.length; i++) {
                    rhsValues[i] = rhsVectors[i].getBytesRef(p, rhsScratch[i]);
                }
                result.appendBoolean(In.process(null, null, lhsVector.getBytesRef(p, lhsScratch), rhsValues));
            }
            return result.build();
        }
    }

    @Override
    public String toString() {
        return "InBytesRefEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
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
        public InBytesRefEvaluator get(DriverContext context) {
            EvalOperator.ExpressionEvaluator[] rhs = Arrays.stream(this.rhs)
                .map(a -> a.get(context))
                .toArray(EvalOperator.ExpressionEvaluator[]::new);
            return new InBytesRefEvaluator(source, lhs.get(context), rhs, context);
        }

        @Override
        public String toString() {
            return "InBytesRefEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
        }
    }
}
