/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Evaluator for InRange(date, date_range) -> boolean.
 * Returns true if the date is within the date_range (inclusive bounds).
 */
public class InRangeEvaluator implements EvalOperator.ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(InRangeEvaluator.class);

    private final Source source;
    private final EvalOperator.ExpressionEvaluator date;
    private final EvalOperator.ExpressionEvaluator range;
    private final DriverContext driverContext;

    public InRangeEvaluator(
        Source source,
        EvalOperator.ExpressionEvaluator date,
        EvalOperator.ExpressionEvaluator range,
        DriverContext driverContext
    ) {
        this.source = source;
        this.date = date;
        this.range = range;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(org.elasticsearch.compute.data.Page page) {
        try (Block dateBlock = date.eval(page); Block rangeBlock = range.eval(page)) {
            return eval(dateBlock, rangeBlock);
        }
    }

    private Block eval(Block dateBlock, Block rangeBlock) {
        LongBlock dateValues = (LongBlock) dateBlock;
        LongRangeBlock rangeValues = (LongRangeBlock) rangeBlock;
        int positionCount = dateValues.getPositionCount();

        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (dateValues.isNull(p) || rangeValues.isNull(p)) {
                    result.appendNull();
                    continue;
                }

                // Get the date value (millis or nanos)
                long dateValue = dateValues.getLong(dateValues.getFirstValueIndex(p));

                // Get the range bounds from the LongRangeBlock
                long rangeFrom = rangeValues.getFromBlock().getLong(p);
                long rangeTo = rangeValues.getToBlock().getLong(p);

                // Check if date is within range (inclusive)
                boolean inRange = dateValue >= rangeFrom && dateValue <= rangeTo;
                result.appendBoolean(inRange);
            }
            return result.build();
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + date.baseRamBytesUsed() + range.baseRamBytesUsed();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(date, range);
    }

    @Override
    public String toString() {
        return "InRangeEvaluator[date=" + date + ", range=" + range + "]";
    }

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory date;
        private final EvalOperator.ExpressionEvaluator.Factory range;

        public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory date, EvalOperator.ExpressionEvaluator.Factory range) {
            this.source = source;
            this.date = date;
            this.range = range;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new InRangeEvaluator(source, date.get(context), range.get(context), context);
        }

        @Override
        public String toString() {
            return "InRangeEvaluator[date=" + date + ", range=" + range + "]";
        }
    }
}
