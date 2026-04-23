/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Evaluator for RangeMin(date_range) -> date.
 * Returns the minimum (start) value of a date_range.
 */
public class RangeMinEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RangeMinEvaluator.class);

    @SuppressWarnings("unused")
    private final Source source;
    private final ExpressionEvaluator range;
    private final DriverContext driverContext;

    public RangeMinEvaluator(Source source, ExpressionEvaluator range, DriverContext driverContext) {
        this.source = source;
        this.range = range;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(org.elasticsearch.compute.data.Page page) {
        try (Block rangeBlock = range.eval(page)) {
            return eval(rangeBlock);
        }
    }

    private Block eval(Block rangeBlock) {
        LongRangeBlock rangeValues = (LongRangeBlock) rangeBlock;
        int positionCount = rangeValues.getPositionCount();

        try (LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (rangeValues.isNull(p)) {
                    result.appendNull();
                    continue;
                }

                LongBlock fromBlock = rangeValues.getFromBlock();
                int first = rangeValues.getFirstValueIndex(p);
                int valueCount = rangeValues.getValueCount(p);
                long minValue = fromBlock.getLong(first);
                for (int i = 1; i < valueCount; i++) {
                    minValue = Math.min(minValue, fromBlock.getLong(first + i));
                }
                result.appendLong(minValue);
            }
            return result.build();
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + range.baseRamBytesUsed();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(range);
    }

    @Override
    public String toString() {
        return "RangeMinEvaluator[range=" + range + "]";
    }

    public static class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final ExpressionEvaluator.Factory range;

        public Factory(Source source, ExpressionEvaluator.Factory range) {
            this.source = source;
            this.range = range;
        }

        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new RangeMinEvaluator(source, range.get(context), context);
        }

        @Override
        public String toString() {
            return "RangeMinEvaluator[range=" + range + "]";
        }
    }
}
