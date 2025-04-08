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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;

public class CopySign extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "CopySign", CopySign::new);

    private final Expression magnitude;
    private final Expression sign;

    @FunctionInfo(
        returnType = { "double" },
        description = "Returns the first argument with the sign of the second argument.",
        examples = @Example(file = "math", tag = "CopySign")
    )
    public CopySign(
        Source source,
        @Param(
            name = "magnitude",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression magnitude,
        @Param(
            name = "sign",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression sign
    ) {
        super(source, Arrays.asList(magnitude, sign));
        this.magnitude = magnitude;
        this.sign = sign;
    }

    private CopySign(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(magnitude);
        out.writeNamedWriteable(sign);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isNumeric(magnitude, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isNumeric(sign, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return magnitude.foldable() && sign.foldable();
    }

    @Evaluator(warnExceptions = { ArithmeticException.class })
    static double process(double magnitude, double sign) {
        return NumericUtils.asFiniteNumber(Math.copySign(magnitude, sign));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new CopySign(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, CopySign::new, magnitude(), sign());
    }

    public Expression magnitude() {
        return magnitude;
    }

    public Expression sign() {
        return sign;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var magnitudeEval = Cast.cast(source(), magnitude.dataType(), DataType.DOUBLE, toEvaluator.apply(magnitude));
        var signEval = Cast.cast(source(), sign.dataType(), DataType.DOUBLE, toEvaluator.apply(sign));
        return new CopySignEvaluator.Factory(source(), magnitudeEval, signEval);
    }
}
