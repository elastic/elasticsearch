/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.In;

import org.apache.lucene.util.ArrayUtil;
$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
$if(int)$
import org.elasticsearch.compute.data.IntBlock;
$elseif(long)$
import org.elasticsearch.compute.data.LongBlock;
$else$
import org.elasticsearch.compute.data.$Type$Block;
$endif$
import java.util.Arrays;

/**
 * This class is generated. Edit {@code X-In.java.st} instead.
 */
public class In$Type$Evaluator implements EvalOperator.ExpressionEvaluator {
    private final Warnings warnings;

    private final EvalOperator.ExpressionEvaluator lhs;

    private final EvalOperator.ExpressionEvaluator[] rhs;

    private final DriverContext driverContext;

    public In$Type$Evaluator(Source source, EvalOperator.ExpressionEvaluator lhs,
        EvalOperator.ExpressionEvaluator[] rhs, DriverContext driverContext) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.driverContext = driverContext;
        this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }

    @Override
    public Block eval(Page page) {
        try ($Type$Block lhsBlock = ($Type$Block) lhs.eval(page)) {
            $Type$Block[] rhsBlocks = new $Type$Block[rhs.length];
            try (Releasable rhsRelease = Releasables.wrap(rhsBlocks)) {
                for (int i = 0; i < rhsBlocks.length; i++) {
                    rhsBlocks[i] = ($Type$Block)rhs[i].eval(page);
                }
                $Type$Vector lhsVector = lhsBlock.asVector();
                if (lhsVector == null) {
                    return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
                }
                $Type$Vector[] rhsVectors = new $Type$Vector[rhs.length];
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


    public BooleanBlock eval(int positionCount, $Type$Block lhsBlock, $Type$Block[] rhsBlocks) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            $Type$[] rhsValues = new $Type$[rhs.length];
            BitSet nulls = new BitSet(rhs.length);
$if(BytesRef)$
            BytesRef lhsScratch = new BytesRef();
            BytesRef[] rhsScratch = new BytesRef[rhs.length];
            for (int i = 0; i < rhs.length; i++) {
                rhsScratch[i] = new BytesRef();
            }
$endif$
            position: for (int p = 0; p < positionCount; p++) {
                if (lhsBlock.isNull(p)) {
                    result.appendNull();
                    continue position;
                }
                if (lhsBlock.getValueCount(p) != 1) {
                    if (lhsBlock.getValueCount(p) > 1) {
                        warnings.registerException(
                            new IllegalArgumentException("single-value function encountered multi-value"));
                    }
                    result.appendNull();
                    continue position;
                }
                // unpack rhsBlocks into rhsValues
                for (int i = 0; i < rhsBlocks.length; i++) {
                    nulls.clear();
                    arrayArgs: for (int i = 0; i < rhsBlocks.length; i++) {
                        if (rhsBlocks[i].isNull(p)) {
                            nulls.set(i);
                            continue arrayArgs;
                        }
                        int o = rhsBlocks[i].getFirstValueIndex(p);
$if(BytesRef)$
                        rhsValues[i] = rhsBlocks[i].getBytesRef(o, rhsScratch[i]);
$else$
                        rhsValues[i] = rhsBlocks[i].get$Type$(o);
$endif$
                    }
                }
                if (nulls.cardinality() == rhsBlocks.length) {
                    result.appendNull();
                    continue position;
                }
                try {
$if(BytesRef)$
                    In.process(result, nulls, lhsBlock.getBytesRef(lhsBlock.getFirstValueIndex(p), lhsScratch), rhsValues);
$else$
                    In.process(result, nulls, lhsBlock.get$Type$(lhsBlock.getFirstValueIndex(p)), rhsValues);
$endif$
                } catch (IllegalArgumentException e) {
                    warnings.registerException(e);
                    result.appendNull();
                }
            }
            return result.build();
        }
    }

    public BooleanBlock eval(int positionCount, $Type$Vector lhsVector, $Type$Vector[] rhsVectors) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            $Type$[] rhsValues = new $Type$[rhs.length];
            BitSet nulls = new BitSet(rhs.length);
$if(BytesRef)$
            BytesRef lhsScratch = new BytesRef();
            BytesRef[] rhsScratch = new BytesRef[rhs.length];
            for (int i = 0; i < rhs.length; i++) {
                rhsScratch[i] = new BytesRef();
            }
$endif$
            position: for (int p = 0; p < positionCount; p++) {
                // unpack rhsVectors into rhsValues
                nulls.clear();
                arrayArgs: for (int i = 0; i < rhsVectors.length; i++) {
                    if (rhsVectors[i].isNull(p)) {
                        nulls.set(i);
                        continue arrayArgs;
                    }
$if(BytesRef)$
                    rhsValues[i] = rhsVectors[i].getBytesRef(p, rhsScratch[i]);
$else$
                    rhsValues[i] = rhsVectors[i].get$Type$(p);
$endif$
                }
                if (nulls.cardinality() == rhsVectors.length) {
                    result.appendNull();
                    continue position;
                }
                try {
$if(BytesRef)$
                    In.process(result, nulls, lhsVector.getBytesRef(lhsBlock.getFirstValueIndex(p), lhsScratch), rhsValues);
$else$
                    In.process(result, nulls, lhsVector.get$Type$(p), rhsValues);
$endif$
                } catch (IllegalArgumentException e) {
                    warnings.registerException(e);
                    result.appendNull();
                }
                return result.build();
            }
        }
    }

    @Override
    public String toString() {
        return "In$Type$Evaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lhs, () -> Releasables.close(rhs));
    }

    static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory lhs;
        private final EvalOperator.ExpressionEvaluator.Factory[] rhs;

        public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory lhs,
            EvalOperator.ExpressionEvaluator.Factory[] rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public In$Type$Evaluator get(DriverContext context) {
          EvalOperator.ExpressionEvaluator[] rhs =
              Arrays.stream(this.rhs).map(a -> a.get(context)).toArray(EvalOperator.ExpressionEvaluator[]::new);
          return new In$Type$Evaluator(source, lhs.get(context), rhs, context);
        }

        @Override
        public String toString() {
            return "In$Type$Evaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
        }
    }
}
