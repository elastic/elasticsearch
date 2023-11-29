/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Reduce a multivalued field to a single valued field containing the count of values.
 */
public class MvCount extends AbstractMultivalueFunction {
    @FunctionInfo(
        returnType = "integer",
        description = "Reduce a multivalued field to a single valued field containing the count of values."
    )
    public MvCount(
        Source source,
        @Param(
            name = "v",
            type = { "unsigned_long", "date", "boolean", "double", "ip", "text", "integer", "keyword", "version", "long" }
        ) Expression v
    ) {
        super(source, v);
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
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return new EvaluatorFactory(fieldEval);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvCount(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvCount::new, field());
    }

    private record EvaluatorFactory(ExpressionEvaluator.Factory field) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(context, field.get(context));
        }

        @Override
        public String toString() {
            return "MvCount[field=" + field + ']';
        }
    }

    private static class Evaluator extends AbstractEvaluator {
        private final DriverContext driverContext;

        protected Evaluator(DriverContext driverContext, EvalOperator.ExpressionEvaluator field) {
            super(field);
            this.driverContext = driverContext;
        }

        @Override
        protected String name() {
            return "MvCount";
        }

        @Override
        protected Block evalNullable(Block block) {
            try (var builder = IntBlock.newBlockBuilder(block.getPositionCount(), driverContext.blockFactory())) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    int valueCount = block.getValueCount(p);
                    if (valueCount == 0) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendInt(valueCount);
                }
                return builder.build();
            }
        }

        @Override
        protected Block evalNotNullable(Block block) {
            try (var builder = IntVector.newVectorFixedBuilder(block.getPositionCount(), driverContext.blockFactory())) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    builder.appendInt(block.getValueCount(p));
                }
                return builder.build().asBlock();
            }
        }

        @Override
        protected Block evalSingleValuedNullable(Block ref) {
            return evalNullable(ref);
        }

        @Override
        protected Block evalSingleValuedNotNullable(Block ref) {
            return driverContext.blockFactory().newConstantIntBlockWith(1, ref.getPositionCount());
        }
    }
}
