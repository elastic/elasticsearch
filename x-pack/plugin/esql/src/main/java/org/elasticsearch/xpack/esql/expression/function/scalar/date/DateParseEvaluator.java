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
import org.elasticsearch.xpack.ql.expression.Expression;

import java.time.ZoneId;
import java.time.format.DateTimeParseException;

/**
 * Not generated because it has to handle parse exceptions and return null values
 */
public final class DateParseEvaluator implements EvalOperator.ExpressionEvaluator {
    private final EvalOperator.ExpressionEvaluator val;

    private final EvalOperator.ExpressionEvaluator formatter;

    private final ZoneId zoneId;

    public DateParseEvaluator(EvalOperator.ExpressionEvaluator val, EvalOperator.ExpressionEvaluator formatter, ZoneId zoneId) {
        this.val = val;
        this.formatter = formatter;
        this.zoneId = zoneId;
    }

    static Long fold(Expression val, Expression formatter, ZoneId zoneId) {
        Object valVal = val.fold();
        if (valVal == null) {
            return null;
        }
        Object formatterVal = formatter.fold();
        if (formatterVal == null) {
            return null;
        }
        try {
            return DateParse.process((BytesRef) valVal, (BytesRef) formatterVal, zoneId);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    static Long fold(Expression val, DateFormatter formatter) {
        Object valVal = val.fold();
        if (valVal == null) {
            return null;
        }
        try {
            return DateParse.process((BytesRef) valVal, formatter);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    @Override
    public Block eval(Page page) {
        Block valUncastBlock = val.eval(page);
        if (valUncastBlock.areAllValuesNull()) {
            return Block.constantNullBlock(page.getPositionCount());
        }
        BytesRefBlock valBlock = (BytesRefBlock) valUncastBlock;
        Block formatterUncastBlock = formatter.eval(page);
        if (formatterUncastBlock.areAllValuesNull()) {
            return Block.constantNullBlock(page.getPositionCount());
        }
        BytesRefBlock formatterBlock = (BytesRefBlock) formatterUncastBlock;
        return eval(page.getPositionCount(), valBlock, formatterBlock, zoneId);
    }

    public LongBlock eval(int positionCount, BytesRefBlock valBlock, BytesRefBlock formatterBlock, ZoneId zoneId) {
        LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
        BytesRef valScratch = new BytesRef();
        BytesRef formatterScratch = new BytesRef();
        position: for (int p = 0; p < positionCount; p++) {
            if (valBlock.isNull(p) || valBlock.getValueCount(p) != 1) {
                result.appendNull();
                continue position;
            }
            if (formatterBlock.isNull(p) || formatterBlock.getValueCount(p) != 1) {
                result.appendNull();
                continue position;
            }
            try {
                result.appendLong(
                    DateParse.process(
                        valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch),
                        formatterBlock.getBytesRef(formatterBlock.getFirstValueIndex(p), formatterScratch),
                        zoneId
                    )
                );
            } catch (DateTimeParseException e) {
                result.appendNull();
            }
        }
        return result.build();
    }

    @Override
    public String toString() {
        return "DateParseEvaluator[" + "val=" + val + ", formatter=" + formatter + ", zoneId=" + zoneId + "]";
    }
}
