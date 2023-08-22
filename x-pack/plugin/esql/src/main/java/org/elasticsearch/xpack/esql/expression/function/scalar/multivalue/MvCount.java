/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Reduce a multivalued field to a single valued field containing the minimum value.
 */
public class MvCount extends AbstractMultivalueFunction {
    public MvCount(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), EsqlDataTypes::isRepresentable, sourceText(), null, "representable");
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    @Override
    protected Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval) {
        return () -> new Evaluator(fieldEval.get());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvCount(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvCount::new, field());
    }

    private static class Evaluator extends AbstractEvaluator {
        protected Evaluator(EvalOperator.ExpressionEvaluator field) {
            super(field);
        }

        @Override
        protected String name() {
            return "MvCount";
        }

        @Override
        protected Block evalNullable(Block fieldVal) {
            IntBlock.Builder builder = IntBlock.newBlockBuilder(fieldVal.getPositionCount());
            for (int p = 0; p < fieldVal.getPositionCount(); p++) {
                int valueCount = fieldVal.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                builder.appendInt(valueCount);
            }
            return builder.build();
        }

        @Override
        protected Vector evalNotNullable(Block fieldVal) {
            int[] values = new int[fieldVal.getPositionCount()];
            for (int p = 0; p < fieldVal.getPositionCount(); p++) {
                values[p] = fieldVal.getValueCount(p);
            }
            return new IntArrayVector(values, values.length);
        }

        @Override
        protected Block evalSingleValuedNullable(Block fieldVal) {
            return evalNullable(fieldVal);
        }

        @Override
        protected Vector evalSingleValuedNotNullable(Block fieldVal) {
            return new ConstantIntVector(1, fieldVal.getPositionCount());
        }
    }
}
