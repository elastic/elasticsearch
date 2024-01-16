/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Map all single-values to a constant output value.
 * Handle nulls and multi-values like other scalar functions, mapping to null and raising a warning for multi-values.
 */
public class ConstantScalarFunction extends ScalarFunction implements EvaluatorMapper {
    public ConstantScalarFunction(Source source, Expression field, Literal value) {
        super(source, List.of(field, value));
        assert isSupportedDataType(value.dataType());
    }

    // Support for other types (esp. keyword) can be added once needed.
    // TODO: implement other types than int
    // TODO: test
    // - evaluation for all data types
    // - evaluation for all cases of mv/sv nullable/non-nullable
    // - creation of correct warning headers
    private static final Set<DataType> supportedTypes = Set.of(DataTypes.BOOLEAN, DataTypes.INTEGER, DataTypes.LONG, DataTypes.DOUBLE);

    private static boolean isSupportedDataType(DataType dataType) {
        return supportedTypes.contains(dataType);
    }

    Expression field() {
        return children().get(1);
    }

    Literal value() {
        return (Literal) children().get(1);
    }

    @Override
    public final ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    protected NodeInfo<ConstantScalarFunction> info() {
        return NodeInfo.create(this, ConstantScalarFunction::new, field(), value());
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return value().dataType();
    }

    @Override
    public ConstantScalarFunction replaceChildren(List<Expression> newChildren) {
        return new ConstantScalarFunction(source(), newChildren.get(0), (Literal) newChildren.get(1));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return new EvaluatorFactory(source(), toEvaluator.apply(field()), value());
    }

    @Override
    public final Object fold() {
        throw new UnsupportedOperationException("TODO");
    }

    private record EvaluatorFactory(Source source, ExpressionEvaluator.Factory field, Literal value)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            DataType dataType = value.dataType();
            if (dataType == DataTypes.INTEGER) {
                int value = (Integer) this.value.value();
                return new IntConstantEvaluator(source, context, field.get(context), value);
            }

            throw new UnsupportedOperationException("Data type [" + dataType.typeName() + "] not supported by ConstantScalarFunction");
        }

        @Override
        public String toString() {
            return "ConstantScalarFunction[value=" + value.value() + ", field=" + field + "]";
        }
    }

    private abstract static class ConstantScalarFunctionEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
        protected Warnings warnings;

        protected ConstantScalarFunctionEvaluator(Source source, DriverContext driverContext, EvalOperator.ExpressionEvaluator field) {
            super(driverContext, field);
            warnings = new Warnings(source);
        }

        protected static String evaluatorName(Object value) {
            return "ConstantScalarFunction[value=" + value + ']';
        }

        @Override
        protected Block evalNotNullable(Block block) {
            return evalNullable(block);
        }

        @Override
        protected Block evalSingleValuedNullable(Block block) {
            return evalNullable(block);
        }
    }

    private static class IntConstantEvaluator extends ConstantScalarFunctionEvaluator {
        private final int value;

        protected IntConstantEvaluator(Source source, DriverContext driverContext, EvalOperator.ExpressionEvaluator field, int value) {
            super(source, driverContext, field);
            this.value = value;
        }

        @Override
        protected String name() {
            return evaluatorName(value);
        }

        @Override
        protected Block evalNullable(Block block) {
            try (var builder = driverContext.blockFactory().newIntBlockBuilder(block.getPositionCount())) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    if (block.getValueCount(p) != 1) {
                        if (block.getValueCount(p) > 1) {
                            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                        }
                        builder.appendNull();
                        continue;
                    }
                    builder.appendInt(value);
                }
                return builder.build();
            }
        }

        @Override
        protected Block evalSingleValuedNotNullable(Block block) {
            return driverContext.blockFactory().newConstantIntBlockWith(value, block.getPositionCount());
        }
    }
}
