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
import org.elasticsearch.xpack.esql.expression.function.Named;
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

import static org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast.cast;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Pow extends ScalarFunction implements OptionalArgument, EvaluatorMapper {

    private final Expression base, exponent;
    private final DataType dataType;

    public Pow(Source source, @Named("base") Expression base, @Named("exponent") Expression exponent) {
        super(source, Arrays.asList(base, exponent));
        this.base = base;
        this.exponent = exponent;
        this.dataType = determineDataType(base, exponent);
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

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Evaluator(extraName = "Double", warnExceptions = { ArithmeticException.class })
    static double process(double base, double exponent) {
        return validateAsDouble(base, exponent);
    }

    @Evaluator(extraName = "Long", warnExceptions = { ArithmeticException.class })
    static long processLong(double base, double exponent) {
        return exponent == 1 ? validateAsLong(base) : validateAsLong(base, exponent);
    }

    @Evaluator(extraName = "Int", warnExceptions = { ArithmeticException.class })
    static int processInt(double base, double exponent) {
        return exponent == 1 ? validateAsInt(base) : validateAsInt(base, exponent);
    }

    private static double validateAsDouble(double base, double exponent) {
        double result = Math.pow(base, exponent);
        if (Double.isNaN(result)) {
            throw new ArithmeticException("invalid result: pow(" + base + ", " + exponent + ")");
        }
        return result;
    }

    private static long validateAsLong(double base, double exponent) {
        double result = Math.pow(base, exponent);
        if (Double.isNaN(result)) {
            throw new ArithmeticException("invalid result: pow(" + base + ", " + exponent + ")");
        }
        return validateAsLong(result);
    }

    private static long validateAsLong(double value) {
        if (Double.compare(value, Long.MAX_VALUE) > 0) {
            throw new ArithmeticException("long overflow");
        }
        if (Double.compare(value, Long.MIN_VALUE) < 0) {
            throw new ArithmeticException("long overflow");
        }
        return (long) value;
    }

    private static int validateAsInt(double base, double exponent) {
        double result = Math.pow(base, exponent);
        if (Double.isNaN(result)) {
            throw new ArithmeticException("invalid result: pow(" + base + ", " + exponent + ")");
        }
        return validateAsInt(result);
    }

    private static int validateAsInt(double value) {
        if (Double.compare(value, Integer.MAX_VALUE) > 0) {
            throw new ArithmeticException("integer overflow");
        }
        if (Double.compare(value, Integer.MIN_VALUE) < 0) {
            throw new ArithmeticException("integer overflow");
        }
        return (int) value;
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

    private static DataType determineDataType(Expression base, Expression exponent) {
        if (base.dataType().isRational() || exponent.dataType().isRational()) {
            return DataTypes.DOUBLE;
        }
        if (base.dataType().size() == Long.BYTES || exponent.dataType().size() == Long.BYTES) {
            return DataTypes.LONG;
        }
        return DataTypes.INTEGER;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var baseEvaluator = toEvaluator.apply(base);
        var exponentEvaluator = toEvaluator.apply(exponent);
        if (dataType == DataTypes.DOUBLE) {
            return dvrCtx -> new PowDoubleEvaluator(
                source(),
                cast(base.dataType(), DataTypes.DOUBLE, baseEvaluator).get(dvrCtx),
                cast(exponent.dataType(), DataTypes.DOUBLE, exponentEvaluator).get(dvrCtx),
                dvrCtx
            );
        } else if (dataType == DataTypes.LONG) {
            return dvrCtx -> new PowLongEvaluator(
                source(),
                cast(base.dataType(), DataTypes.DOUBLE, baseEvaluator).get(dvrCtx),
                cast(exponent.dataType(), DataTypes.DOUBLE, exponentEvaluator).get(dvrCtx),
                dvrCtx
            );
        } else {
            return dvrCtx -> new PowIntEvaluator(
                source(),
                cast(base.dataType(), DataTypes.DOUBLE, baseEvaluator).get(dvrCtx),
                cast(exponent.dataType(), DataTypes.DOUBLE, exponentEvaluator).get(dvrCtx),
                dvrCtx
            );
        }
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
