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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Log extends EsqlScalarFunction implements OptionalArgument {

    private final Expression base, value;

    @FunctionInfo(returnType = "double", description = "Returns the logarithm of a value to a base.")
    public Log(
        Source source,
        @Param(name = "base", type = { "integer", "unsigned_long", "long", "double" }, optional = true) Expression base,
        @Param(name = "value", type = { "integer", "unsigned_long", "long", "double" }) Expression value
    ) {
        super(source, value != null ? Arrays.asList(base, value) : Arrays.asList(base));
        this.value = value != null ? value : base;
        this.base = value != null ? base : null;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (base != null) {
            TypeResolution resolution = isNumeric(base, sourceText(), FIRST);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return isNumeric(value, sourceText(), base != null ? SECOND : FIRST);
    }

    @Override
    public boolean foldable() {
        return (base == null || base.foldable()) && value.foldable();
    }

    @Evaluator(extraName = "Constant", warnExceptions = { ArithmeticException.class })
    static double process(double value) throws ArithmeticException {
        if (value <= 0d) {
            throw new ArithmeticException("Log of non-positive number");
        }
        return Math.log(value);
    }

    @Evaluator(warnExceptions = { ArithmeticException.class })
    static double process(double base, double value) throws ArithmeticException {
        if (base <= 0d || value <= 0d) {
            throw new ArithmeticException("Log of non-positive number");
        }
        if (base == 1d) {
            throw new ArithmeticException("Log of base 1");
        }
        return Math.log10(value) / Math.log10(base);
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Log(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        Expression b = base != null ? base : value;
        Expression v = base != null ? value : null;
        return NodeInfo.create(this, Log::new, b, v);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var valueEval = Cast.cast(source(), value.dataType(), DataTypes.DOUBLE, toEvaluator.apply(value));
        if (base != null) {
            var baseEval = Cast.cast(source(), base.dataType(), DataTypes.DOUBLE, toEvaluator.apply(base));
            return new LogEvaluator.Factory(source(), baseEval, valueEval);
        }
        return new LogConstantEvaluator.Factory(source(), valueEval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Log other = (Log) obj;
        return Objects.equals(other.base, base) && Objects.equals(other.value, value);
    }
}
