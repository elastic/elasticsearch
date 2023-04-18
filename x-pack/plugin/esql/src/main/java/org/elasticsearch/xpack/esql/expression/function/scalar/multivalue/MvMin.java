/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

/**
 * Reduce a multivalued field to a single valued field containing the minimum value.
 */
public class MvMin extends AbstractMultivalueFunction {
    public MvMin(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Object foldMultivalued(List<?> l) {
        DataType type = field().dataType();
        if (type == DataTypes.INTEGER) {
            return l.stream().mapToInt(o -> (int) o).min().getAsInt();
        }
        throw new UnsupportedOperationException();
    }

    @Override
    protected Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval) {
        DataType type = field().dataType();
        if (type == DataTypes.INTEGER) {
            return () -> new IntEvaluator(fieldEval.get());
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMin(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMin::new, field());
    }

    private static class IntEvaluator extends AbstractEvaluator {
        private IntEvaluator(EvalOperator.ExpressionEvaluator field) {
            super(field);
        }

        @Override
        protected String name() {
            return "MvMin";
        }

        @Override
        protected Block evalWithNulls(Block fieldVal) {
            IntBlock v = (IntBlock) fieldVal;
            int positionCount = v.getPositionCount();
            IntBlock.Builder builder = IntBlock.newBlockBuilder(positionCount);
            for (int p = 0; p < positionCount; p++) {
                if (v.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = v.getValueCount(p);
                if (v.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                int first = v.getFirstValueIndex(p);
                int value = v.getInt(first);
                int end = first + valueCount;
                for (int i = first + 1; i < end; i++) {
                    value = Math.min(value, v.getInt(i));
                }
                builder.appendInt(value);
            }
            return builder.build();
        }

        @Override
        protected Block evalWithoutNulls(Block fieldVal) {
            IntBlock v = (IntBlock) fieldVal;
            int positionCount = v.getPositionCount();
            int[] values = new int[positionCount];
            for (int p = 0; p < positionCount; p++) {
                int first = v.getFirstValueIndex(p);
                int value = v.getInt(first);
                int valueCount = v.getValueCount(p);
                int end = first + valueCount;
                for (int i = first + 1; i < end; i++) {
                    value = Math.min(value, v.getInt(i));
                }
                values[p] = value;
            }
            return new IntArrayVector(values, positionCount).asBlock();
        }

    }
}
