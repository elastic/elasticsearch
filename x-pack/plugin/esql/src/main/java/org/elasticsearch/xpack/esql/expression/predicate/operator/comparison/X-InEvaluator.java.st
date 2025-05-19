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
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
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
public class In$Name$Evaluator implements EvalOperator.ExpressionEvaluator {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator lhs;

    private final EvalOperator.ExpressionEvaluator[] rhs;

    private final DriverContext driverContext;

    private Warnings warnings;

    public In$Name$Evaluator(
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
        try ($Type$Block lhsBlock = ($Type$Block) lhs.eval(page)) {
            $Type$Block[] rhsBlocks = new $Type$Block[rhs.length];
            try (Releasable rhsRelease = Releasables.wrap(rhsBlocks)) {
                for (int i = 0; i < rhsBlocks.length; i++) {
                    rhsBlocks[i] = ($Type$Block) rhs[i].eval(page);
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

    private BooleanBlock eval(int positionCount, $Type$Block lhsBlock, $Type$Block[] rhsBlocks) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
$if(int)$
            int[] rhsValues = new int[rhs.length];
$elseif(long)$
            long[] rhsValues = new long[rhs.length];
$elseif(double)$
            double[] rhsValues = new double[rhs.length];
$elseif(boolean)$
            boolean hasTrue = false;
            boolean hasFalse = false;
$elseif(BytesRef)$
            $Type$[] rhsValues = new $Type$[rhs.length];
            BytesRef lhsScratch = new BytesRef();
            BytesRef[] rhsScratch = new BytesRef[rhs.length];
            for (int i = 0; i < rhs.length; i++) {
                rhsScratch[i] = new BytesRef();
            }
$endif$
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
$if(boolean)$
                hasTrue = false;
                hasFalse = false;
$endif$
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
$if(boolean)$
                    if (hasTrue && hasFalse) {
                        continue;
                    }
$endif$
                    int o = rhsBlocks[i].getFirstValueIndex(p);
$if(BytesRef)$
                    rhsValues[i] = rhsBlocks[i].getBytesRef(o, rhsScratch[i]);
$elseif(boolean)$
                    if (rhsBlocks[i].getBoolean(o)) {
                        hasTrue = true;
                    } else {
                        hasFalse = true;
                    }
$else$
                    rhsValues[i] = rhsBlocks[i].get$Type$(o);
$endif$
                }
                if (nulls.cardinality() == rhsBlocks.length || mvs.cardinality() == rhsBlocks.length) {
                    result.appendNull();
                    continue;
                }
$if(boolean)$
                foundMatch = lhsBlock.getBoolean(lhsBlock.getFirstValueIndex(p)) ? hasTrue : hasFalse;
$elseif(BytesRef)$
                foundMatch = In.process(nulls, mvs, lhsBlock.getBytesRef(lhsBlock.getFirstValueIndex(p), lhsScratch), rhsValues);
$elseif(NanosMillis)$
                // Even though these are longs, they're in different units and need special handling
                foundMatch = In.processNanosMilils(nulls, mvs, lhsBlock.get$Type$(lhsBlock.getFirstValueIndex(p)), rhsValues);
$else$
                foundMatch = In.process(nulls, mvs, lhsBlock.get$Type$(lhsBlock.getFirstValueIndex(p)), rhsValues);
$endif$
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

    private BooleanBlock eval(int positionCount, $Type$Vector lhsVector, $Type$Vector[] rhsVectors) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
$if(int)$
            int[] rhsValues = new int[rhs.length];
$elseif(long)$
            long[] rhsValues = new long[rhs.length];
$elseif(double)$
            double[] rhsValues = new double[rhs.length];
$elseif(boolean)$
            boolean hasTrue = false;
            boolean hasFalse = false;
$elseif(BytesRef)$
            $Type$[] rhsValues = new $Type$[rhs.length];
            BytesRef lhsScratch = new BytesRef();
            BytesRef[] rhsScratch = new BytesRef[rhs.length];
            for (int i = 0; i < rhs.length; i++) {
                rhsScratch[i] = new BytesRef();
            }
$endif$
            for (int p = 0; p < positionCount; p++) {
                // unpack rhsVectors into rhsValues
$if(boolean)$
                hasTrue = false;
                hasFalse = false;
$endif$
                for (int i = 0; i < rhsVectors.length; i++) {
$if(boolean)$
                    if (hasTrue && hasFalse) {
                        continue;
                    }
$endif$
$if(BytesRef)$
                    rhsValues[i] = rhsVectors[i].getBytesRef(p, rhsScratch[i]);
$elseif(boolean)$
                    if (rhsVectors[i].getBoolean(p)) {
                        hasTrue = true;
                    } else {
                        hasFalse = true;
                    }
$else$
                    rhsValues[i] = rhsVectors[i].get$Type$(p);
$endif$
                }
$if(BytesRef)$
                result.appendBoolean(In.process(null, null, lhsVector.getBytesRef(p, lhsScratch), rhsValues));
$elseif(boolean)$
                result.appendBoolean(lhsVector.getBoolean(p) ? hasTrue : hasFalse);
$elseif(nanosMillis)$
                result.appendBoolean(In.processNanosMillis(null, null, lhsVector.get$Type$(p), rhsValues));
$elseif(millisNanos)$
                result.appendBoolean(In.processMillisNanos(null, null, lhsVector.get$Type$(p), rhsValues));
$else$
                result.appendBoolean(In.process(null, null, lhsVector.get$Type$(p), rhsValues));
$endif$
            }
            return result.build();
        }
    }

    @Override
    public String toString() {
        return "In$Name$Evaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
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
        public In$Name$Evaluator get(DriverContext context) {
            EvalOperator.ExpressionEvaluator[] rhs = Arrays.stream(this.rhs)
                .map(a -> a.get(context))
                .toArray(EvalOperator.ExpressionEvaluator[]::new);
            return new In$Name$Evaluator(source, lhs.get(context), rhs, context);
        }

        @Override
        public String toString() {
            return "In$Name$Evaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
        }
    }
}
