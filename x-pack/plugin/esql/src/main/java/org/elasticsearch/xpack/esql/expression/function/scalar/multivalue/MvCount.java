/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
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
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "double",
                "geo_point",
                "geo_shape",
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
        protected Evaluator(DriverContext driverContext, EvalOperator.ExpressionEvaluator field) {
            super(driverContext, field);
        }

        @Override
        protected String name() {
            return "MvCount";
        }

        @Override
        protected Block evalNullable(Block block) {
            try (var builder = driverContext.blockFactory().newIntBlockBuilder(block.getPositionCount())) {
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
            try (var builder = driverContext.blockFactory().newIntVectorFixedBuilder(block.getPositionCount())) {
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
