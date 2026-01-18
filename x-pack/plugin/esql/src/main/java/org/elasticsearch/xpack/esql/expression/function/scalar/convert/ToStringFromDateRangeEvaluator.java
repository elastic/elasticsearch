/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

public class ToStringFromDateRangeEvaluator extends AbstractConvertFunction.AbstractEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToStringFromDateRangeEvaluator.class);

    private final EvalOperator.ExpressionEvaluator field;

    private final DateFormatter formatter;

    public ToStringFromDateRangeEvaluator(
        Source source,
        EvalOperator.ExpressionEvaluator field,
        DateFormatter formatter,
        DriverContext driverContext
    ) {
        super(driverContext, source);
        this.field = field;
        this.formatter = formatter;
    }

    @Override
    protected EvalOperator.ExpressionEvaluator next() {
        return field;
    }

    @Override
    protected Block evalVector(Vector v) {
        return evalBlock(v.asBlock());
    }

    private BytesRef evalValue(LongRangeBlock block, int idx) {
        return new BytesRef(
            (EsqlDataTypeConverter.dateTimeToString(block.getFromBlock().getLong(idx), this.formatter)
                + ".."
                + EsqlDataTypeConverter.dateTimeToString(block.getToBlock().getLong(idx), this.formatter))
        );
    }

    @Override
    public Block evalBlock(Block b) {
        var block = (LongRangeBlock) b;
        int positionCount = block.getPositionCount();
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (block.isNull(p)) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(evalValue(block, p));
                }
            }
            return builder.build();
        }
    }

    @Override
    public String toString() {
        return "ToStringFromDateRangeEvaluator[field=" + field + ", formatter=" + formatter + ']';
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + field.baseRamBytesUsed();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(field);
    }

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory field;
        private final DateFormatter formatter;

        public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field, DateFormatter formatter) {
            this.source = source;
            this.field = field;
            this.formatter = formatter;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new ToStringFromDateRangeEvaluator(source, field.get(context), formatter, context);
        }

        @Override
        public String toString() {
            return "ToStringFromDateRangeEvaluator[field=" + field + ", formatter=" + formatter + "]";
        }
    }
}
