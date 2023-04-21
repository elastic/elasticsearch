/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
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
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Pow extends ScalarFunction implements OptionalArgument, Mappable {

    private final Expression base, exponent;

    public Pow(Source source, Expression base, Expression exponent) {
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

    @Override
    public Object fold() {
        return Math.pow(((Number) base.fold()).doubleValue(), ((Number) exponent.fold()).doubleValue());
    }

    @Evaluator(extraName = "DoubleInt")
    static double process(double base, int exponent) {
        return Math.pow(base, exponent);
    }

    @Evaluator(extraName = "DoubleLong")
    static double process(double base, long exponent) {
        return Math.pow(base, exponent);
    }

    @Evaluator(extraName = "DoubleDouble")
    static double process(double base, double exponent) {
        return Math.pow(base, exponent);
    }

    @Evaluator(extraName = "LongInt")
    static long process(long base, int exponent) {
        return (long) Math.pow(base, exponent);
    }

    @Evaluator(extraName = "LongLong")
    static long process(long base, long exponent) {
        return (long) Math.pow(base, exponent);
    }

    @Evaluator(extraName = "LongDouble")
    static double process(long base, double exponent) {
        return Math.pow(base, exponent);
    }

    @Evaluator(extraName = "IntInt")
    static int process(int base, int exponent) {
        return (int) Math.pow(base, exponent);
    }

    @Evaluator(extraName = "IntLong")
    static long process(int base, long exponent) {
        return (long) Math.pow(base, exponent);
    }

    @Evaluator(extraName = "IntDouble")
    static double process(int base, double exponent) {
        return Math.pow(base, exponent);
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
        if (base.dataType().isRational() || exponent.dataType().isRational()) {
            return DataTypes.DOUBLE;
        }
        if (base.dataType() == DataTypes.LONG || exponent.dataType() == DataTypes.LONG) {
            return DataTypes.LONG;
        }
        return DataTypes.INTEGER;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        var baseEvaluator = toEvaluator.apply(base);
        var exponentEvaluator = toEvaluator.apply(exponent);
        if (base.dataType() == DataTypes.INTEGER) {
            if (exponent.dataType() == DataTypes.INTEGER) {
                return () -> new PowIntIntEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            } else if (exponent.dataType() == DataTypes.LONG) {
                return () -> new PowIntLongEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            } else {
                return () -> new PowIntDoubleEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            }
        } else if (base.dataType() == DataTypes.LONG) {
            if (exponent.dataType() == DataTypes.INTEGER) {
                return () -> new PowLongIntEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            } else if (exponent.dataType() == DataTypes.LONG) {
                return () -> new PowLongLongEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            } else {
                return () -> new PowLongDoubleEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            }
        } else {
            if (exponent.dataType() == DataTypes.INTEGER) {
                return () -> new PowDoubleIntEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            } else if (exponent.dataType() == DataTypes.LONG) {
                return () -> new PowDoubleLongEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            } else {
                return () -> new PowDoubleDoubleEvaluator(baseEvaluator.get(), exponentEvaluator.get());
            }
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
