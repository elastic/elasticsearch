/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

// begin generated imports
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;
import java.util.BitSet;
// end generated imports

/**
 * {@link ExpressionEvaluator} implementation for {@link In}.
 * This class is generated. Edit {@code X-InEvaluator.java.st} instead.
 */
public class InLongRangeEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(InLongRangeEvaluator.class);

    private final Source source;

    private final ExpressionEvaluator lhs;

    private final ExpressionEvaluator[] rhs;

    private final DriverContext driverContext;

    private Warnings warnings;

    public InLongRangeEvaluator(Source source, ExpressionEvaluator lhs, ExpressionEvaluator[] rhs, DriverContext driverContext) {
        this.source = source;
        this.lhs = lhs;
        this.rhs = rhs;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        try (LongRangeBlock lhsBlock = (LongRangeBlock) lhs.eval(page)) {
            LongRangeBlock[] rhsBlocks = new LongRangeBlock[rhs.length];
            try (Releasable rhsRelease = Releasables.wrap(rhsBlocks)) {
                for (int i = 0; i < rhsBlocks.length; i++) {
                    rhsBlocks[i] = (LongRangeBlock) rhs[i].eval(page);
                }
                return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
            }
        }
    }

    private BooleanBlock eval(int positionCount, LongRangeBlock lhsBlock, LongRangeBlock[] rhsBlocks) {
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            LongRangeBlockBuilder.LongRange lhsScratch = new LongRangeBlockBuilder.LongRange();
            LongRangeBlockBuilder.LongRange[] rhsScratch = new LongRangeBlockBuilder.LongRange[rhs.length];
            for (int i = 0; i < rhs.length; i++) {
                rhsScratch[i] = new LongRangeBlockBuilder.LongRange();
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
                    rhsScratch[i] = rhsBlocks[i].getLongRange(o, rhsScratch[i]);
                }
                if (nulls.cardinality() == rhsBlocks.length || mvs.cardinality() == rhsBlocks.length) {
                    result.appendNull();
                    continue;
                }
                foundMatch = In.processLongRange(nulls, mvs, lhsBlock.getLongRange(lhsBlock.getFirstValueIndex(p), lhsScratch), rhsScratch);
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

    @Override
    public String toString() {
        return "InLongRangeEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
    }

    @Override
    public long baseRamBytesUsed() {
        long baseRamBytesUsed = BASE_RAM_BYTES_USED;
        baseRamBytesUsed += lhs.baseRamBytesUsed();
        for (ExpressionEvaluator r : rhs) {
            baseRamBytesUsed += r.baseRamBytesUsed();
        }
        return baseRamBytesUsed;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lhs, () -> Releasables.close(rhs));
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
        }
        return warnings;
    }

    static class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final ExpressionEvaluator.Factory lhs;
        private final ExpressionEvaluator.Factory[] rhs;

        Factory(Source source, ExpressionEvaluator.Factory lhs, ExpressionEvaluator.Factory[] rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public InLongRangeEvaluator get(DriverContext context) {
            ExpressionEvaluator[] rhs = Arrays.stream(this.rhs).map(a -> a.get(context)).toArray(ExpressionEvaluator[]::new);
            return new InLongRangeEvaluator(source, lhs.get(context), rhs, context);
        }

        @Override
        public String toString() {
            return "InLongRangeEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
        }
    }
}
