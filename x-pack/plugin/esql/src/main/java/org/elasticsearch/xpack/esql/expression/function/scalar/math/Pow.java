/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.NumericUtils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Pow extends EsqlScalarFunction implements OptionalArgument {

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
        return DataTypes.DOUBLE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var baseEval = Cast.cast(source(), base.dataType(), DataTypes.DOUBLE, toEvaluator.apply(base));
        var expEval = Cast.cast(source(), exponent.dataType(), DataTypes.DOUBLE, toEvaluator.apply(exponent));
        return new PowEvaluator.Factory(source(), baseEval, expEval);
    }
}
