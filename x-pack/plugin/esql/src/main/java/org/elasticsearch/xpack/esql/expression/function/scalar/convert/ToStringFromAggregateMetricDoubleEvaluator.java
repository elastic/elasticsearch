/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.tree.Source;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.aggregateMetricDoubleBlockToString;

public class ToStringFromAggregateMetricDoubleEvaluator extends AbstractConvertFunction.AbstractEvaluator {
    public ToStringFromAggregateMetricDoubleEvaluator(EvalOperator.ExpressionEvaluator field, Source source, DriverContext driverContext) {
        super(driverContext, field, source);
    }

    @Override
    protected String name() {
        return "ToStringFromAggregateMetricDouble";
    }

    @Override
    protected Block evalVector(Vector v) {
        return evalBlock(v.asBlock());
    }

    private static BytesRef evalValue(CompositeBlock compositeBlock, int index) {
        return new BytesRef(aggregateMetricDoubleBlockToString(compositeBlock, index));
    }

    @Override
    public Block evalBlock(Block b) {
        CompositeBlock block = (CompositeBlock) b;
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

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory field;

        public Factory(EvalOperator.ExpressionEvaluator.Factory field, Source source) {
            this.field = field;
            this.source = source;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new ToStringFromAggregateMetricDoubleEvaluator(field.get(context), source, context);
        }
    }
}
