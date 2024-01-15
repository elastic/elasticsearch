/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

/**
 * True if the provided value is a single value, null if it is null, false otherwise.
 */
public class IsSv extends AbstractMultivalueFunction {
    @FunctionInfo(returnType = "integer", description = "True if the provided value is a single value, false otherwise..")
    public IsSv(
        Source source,
        // TODO: add unrepresentable types as well? They are fine.
        @Param(
            name = "v",
            type = {
                "boolean",
                "cartesian_point",
                "date",
                "double",
                "geo_point",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" }
        ) Expression v
    ) {
        super(source, v);
    }

    // TODO: test type resolution with all data types
    @Override
    protected TypeResolution resolveFieldType() {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return new EvaluatorFactory(fieldEval);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IsSv(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IsSv::new, field());
    }

    private record EvaluatorFactory(ExpressionEvaluator.Factory field) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(context, field.get(context));
        }

        @Override
        public String toString() {
            return "IsSv[field=" + field + ']';
        }
    }

    // TODO test all 4 evaluation methods for all block types (randomized?)
    private static class Evaluator extends AbstractEvaluator {
        protected Evaluator(DriverContext driverContext, ExpressionEvaluator field) {
            super(driverContext, field);
        }

        @Override
        protected String name() {
            return "IsSv";
        }

        @Override
        protected Block evalNullable(Block block) {
            try (var builder = driverContext.blockFactory().newBooleanBlockBuilder(block.getPositionCount())) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    int valueCount = block.getValueCount(p);
                    if (valueCount == 0) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendBoolean(valueCount == 1);
                }
                return builder.build();
            }
        }

        @Override
        protected Block evalNotNullable(Block block) {
            try (var builder = driverContext.blockFactory().newBooleanVectorFixedBuilder(block.getPositionCount())) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    builder.appendBoolean(block.getValueCount(p) == 1);
                }
                return builder.build().asBlock();
            }
        }

        @Override
        protected Block evalSingleValuedNullable(Block block) {
            try (var builder = driverContext.blockFactory().newBooleanBlockBuilder(block.getPositionCount())) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    if (block.isNull(p)) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendBoolean(true);
                }
                return builder.build();
            }
        }

        @Override
        protected Block evalSingleValuedNotNullable(Block block) {
            return driverContext.blockFactory().newConstantBooleanBlockWith(true, block.getPositionCount());
        }
    }
}
