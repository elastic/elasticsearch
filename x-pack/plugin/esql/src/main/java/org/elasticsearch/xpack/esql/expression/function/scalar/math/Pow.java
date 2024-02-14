/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
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
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Pow extends EsqlScalarFunction implements OptionalArgument {

    private final Expression base, exponent;
    private final DataType dataType;

    @FunctionInfo(returnType = "double", description = "Returns the value of a base raised to the power of an exponent.")
    public Pow(
        Source source,
        @Param(name = "base", type = { "double", "integer", "long", "unsigned_long" }) Expression base,
        @Param(name = "exponent", type = { "double", "integer", "long", "unsigned_long" }) Expression exponent
    ) {
        super(source, Arrays.asList(base, exponent));
        this.base = base;
        this.exponent = exponent;
        this.dataType = DataTypes.DOUBLE;
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
        return dataType;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var baseEval = Cast.cast(source(), base.dataType(), DataTypes.DOUBLE, toEvaluator.apply(base));
        var expEval = Cast.cast(source(), exponent.dataType(), DataTypes.DOUBLE, toEvaluator.apply(exponent));
        return new PowEvaluator.Factory(source(), baseEval, expEval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, exponent);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Pow other = (Pow) obj;
        return Objects.equals(other.base, base) && Objects.equals(other.exponent, exponent);
    }
}
