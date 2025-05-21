/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ES|QL function that mimics the behavior of Math.copySign(double magnitude, double sign).
 * Returns a value with the magnitude of the first argument and the sign of the second argument.
 */
public class CopySign extends EsqlScalarFunction {

    public static final String NAME = "copy_sign";
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, NAME, CopySign::new);

    private interface CopySignFactoryProvider {
        EvalOperator.ExpressionEvaluator.Factory create(
            Source source,
            EvalOperator.ExpressionEvaluator.Factory magnitude,
            EvalOperator.ExpressionEvaluator.Factory sign
        );
    }

    private static final Map<DataType, CopySignFactoryProvider> FACTORY_PROVIDERS = Map.of(
        DataType.FLOAT, CopySignFloatEvaluator.Factory::new,
        DataType.DOUBLE, CopySignDoubleEvaluator.Factory::new
    );

    private DataType dataType;

    @FunctionInfo(
        description = "Returns a value with the magnitude of the first argument and the sign of the second argument. "
            + "This function is similar to Java's Math.copySign(double magnitude, double sign).",
        returnType = { "double", "float" }
    )
    public CopySign(
        Source source,
        @Param(
            name = "magnitude",
            type = { "double", "float" },
            description = "The expression providing the magnitude of the result. Must be a numeric type."
        ) Expression magnitude,
        @Param(
            name = "sign",
            type = { "double", "float" },
            description = "The expression providing the sign of the result. Must be a numeric type."
        ) Expression sign
    ) {
        super(source, Arrays.asList(magnitude, sign));
    }

    private CopySign(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteable(children().get(1));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, CopySign::new, children().get(0), children().get(1));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 2) {
            throw new EsqlIllegalArgumentException("Function [{}] expects exactly two arguments, got [{}]", NAME, newChildren.size());
        }
        return new CopySign(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public DataType dataType() {

        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    public TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var magnitude = children().get(0);
        var sign = children().get(1);
        if (magnitude.dataType().isNumeric() == false) {
            return new TypeResolution("Magnitude must be a numeric type");
        }
        if (sign.dataType().isNumeric() == false) {
            return new TypeResolution("Sign must be a numeric type");
        }
        // TODO(pabloem): Looks like we don't currently support generalizing types
        // when two types are incompatible (e.g. copySign(int, long) -> long instead of
        // copySign(int, long) => ERROR).
        dataType = magnitude.dataType();
        TypeResolution resolution = TypeResolutions.isType(
            magnitude,
            t -> t == dataType,
            sourceText(),
            TypeResolutions.ParamOrdinal.fromIndex(1),
            magnitude.dataType().typeName()
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var dataType = dataType();
        if (FACTORY_PROVIDERS.containsKey(dataType)) {
            return FACTORY_PROVIDERS.get(dataType).create(source(), toEvaluator.apply(children().get(0)), toEvaluator.apply(children().get(1)));
        } else {
            throw new EsqlIllegalArgumentException("Unsupported data type [{}] for function [{}]", dataType, NAME);
        }
    }

    @Evaluator(extraName = "Float")
    static float processFloat(float magnitude, float sign) {
        return Math.copySign(magnitude, sign);
    }

    @Evaluator(extraName = "Double")
    static double processDouble(double magnitude, double sign) {
        return Math.copySign(magnitude, sign);
    }
}
