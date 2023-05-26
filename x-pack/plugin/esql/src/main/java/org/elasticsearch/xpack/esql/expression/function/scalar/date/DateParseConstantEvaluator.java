/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * Not generated because it has to handle parse exceptions and return null values
 */
public final class DateParseConstantEvaluator implements EvalOperator.ExpressionEvaluator {
    private final EvalOperator.ExpressionEvaluator val;

    private final DateFormatter formatter;

    public DateParseConstantEvaluator(EvalOperator.ExpressionEvaluator val, DateFormatter formatter) {
        this.val = val;
        this.formatter = formatter;
    }

    @Override
    public Block eval(Page page) {
        Block valUncastBlock = val.eval(page);
        if (valUncastBlock.areAllValuesNull()) {
            return Block.constantNullBlock(page.getPositionCount());
        }
        BytesRefBlock valBlock = (BytesRefBlock) valUncastBlock;
        return eval(page.getPositionCount(), valBlock, formatter);
    }

    public LongBlock eval(int positionCount, BytesRefBlock valBlock, DateFormatter formatter) {
        LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
        BytesRef valScratch = new BytesRef();
        position: for (int p = 0; p < positionCount; p++) {
            if (valBlock.isNull(p) || valBlock.getValueCount(p) != 1) {
                result.appendNull();
                continue position;
            }
            try {
                result.appendLong(DateParse.process(valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch), formatter));
            } catch (IllegalArgumentException e) {
                result.appendNull();
            }
        }
        return result.build();
    }

    @Override
    public String toString() {
        return "DateTimeParseConstantEvaluator[" + "val=" + val + ", formatter=" + formatter + "]";
    }
}
