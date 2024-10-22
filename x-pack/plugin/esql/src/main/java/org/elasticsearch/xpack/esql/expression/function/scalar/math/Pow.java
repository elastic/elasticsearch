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

public class Pow extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Pow", Pow::new);

    private final Expression base;
    private final Expression exponent;

    @FunctionInfo(
        returnType = "double",
        description = "Returns the value of `base` raised to the power of `exponent`.",
        note = "It is still possible to overflow a double result here; in that case, null will be returned.",
        examples = { @Example(file = "math", tag = "powDI"), @Example(file = "math", tag = "powID-sqrt", description = """
            The exponent can be a fraction, which is similar to performing a root.
            For example, the exponent of `0.5` will give the square root of the base:"""), }
    )
    public Pow(
        Source source,
        @Param(
            name = "base",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression for the base. If `null`, the function returns `null`."
        ) Expression base,
        @Param(
            name = "exponent",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression for the exponent. If `null`, the function returns `null`."
        ) Expression exponent
    ) {
        super(source, Arrays.asList(base, exponent));
        this.base = base;
        this.exponent = exponent;
    }

    private Pow(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(base);
        out.writeNamedWriteable(exponent);
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

        TypeResolution resolution = isNumeric(base, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isNumeric(exponent, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return base.foldable() && exponent.foldable();
    }

    @Evaluator(warnExceptions = { ArithmeticException.class })
    static double process(double base, double exponent) {
        return NumericUtils.asFiniteNumber(Math.pow(base, exponent));
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Pow(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Pow::new, base(), exponent());
    }

    public Expression base() {
        return base;
    }

    public Expression exponent() {
        return exponent;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var baseEval = Cast.cast(source(), base.dataType(), DataType.DOUBLE, toEvaluator.apply(base));
        var expEval = Cast.cast(source(), exponent.dataType(), DataType.DOUBLE, toEvaluator.apply(exponent));
        return new PowEvaluator.Factory(source(), baseEval, expEval);
    }
}
