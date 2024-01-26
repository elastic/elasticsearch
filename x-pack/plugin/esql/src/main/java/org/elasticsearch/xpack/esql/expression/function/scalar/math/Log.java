/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
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

public class Log extends ScalarFunction implements OptionalArgument, EvaluatorMapper {

    private final Expression base, value;

    @FunctionInfo(returnType = "double", description = "Returns the logarithm of a value to a base.")
    public Log(
        Source source,
        @Param(name = "base", type = { "integer", "unsigned_long", "long", "double" }) Expression base,
        @Param(name = "value", type = { "integer", "unsigned_long", "long", "double" }) Expression value
    ) {
        super(source, Arrays.asList(base, value));
        this.base = base;
        this.value = value;
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

        return isNumeric(value, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return base.foldable() && value.foldable();
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Evaluator(warnExceptions = { ArithmeticException.class })
    static double process(double base, double value) {
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
        return new Log(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Log::new, base(), value());
    }

    public Expression base() {
        return base;
    }

    public Expression value() {
        return value;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var baseEval = Cast.cast(source(), base.dataType(), DataTypes.DOUBLE, toEvaluator.apply(base));
        var valueEval = Cast.cast(source(), value.dataType(), DataTypes.DOUBLE, toEvaluator.apply(value));
        return new LogEvaluator.Factory(source(), baseEval, valueEval);
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
