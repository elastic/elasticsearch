/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
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
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Scalb extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Scalb", Scalb::new);

    private final Expression d;
    private final Expression scaleFactor;

    @FunctionInfo(returnType = "double", description = """
        Returns the result of `d * 2 ^ scaleFactor`,
        Similar to Java's `scalb` function. Result is rounded as if
        performed by a single correctly rounded floating-point multiply
        to a member of the double value set.""", examples = @Example(file = "floats", tag = "scalb"))

    public Scalb(
        Source source,
        @Param(
            name = "d",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression for the multiplier. If `null`, the function returns `null`."
        ) Expression d,
        @Param(
            name = "scaleFactor",
            type = { "integer", "long" },
            description = "Numeric expression for the scale factor. If `null`, the function returns `null`."
        ) Expression scaleFactor
    ) {
        super(source, Arrays.asList(d, scaleFactor));
        this.d = d;
        this.scaleFactor = scaleFactor;
    }

    private Scalb(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(d);
        out.writeNamedWriteable(scaleFactor);
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

        TypeResolution resolution = isNumeric(d, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return scaleFactor == null
            ? TypeResolution.TYPE_RESOLVED
            : isType(
                scaleFactor,
                dt -> dt.isWholeNumber() && dt != DataType.UNSIGNED_LONG,
                sourceText(),
                SECOND,
                "whole number except unsigned_long or counter types"
            );
    }

    @Override
    public boolean foldable() {
        return d.foldable() && scaleFactor.foldable();
    }

    @Evaluator(extraName = "Int", warnExceptions = { ArithmeticException.class })
    static double process(double d, int scaleFactor) {
        return NumericUtils.asFiniteNumber(Math.scalb(d, scaleFactor));
    }

    @Evaluator(extraName = "Long", warnExceptions = { ArithmeticException.class })
    static double process(double d, long scaleFactor) {
        return NumericUtils.asFiniteNumber(Math.scalb(d, Math.toIntExact(scaleFactor)));
    }

    @Evaluator(extraName = "ConstantInt", warnExceptions = { ArithmeticException.class })
    static double processConstantInt(double d, @Fixed int scaleFactor) {
        return NumericUtils.asFiniteNumber(Math.scalb(d, scaleFactor));
    }

    @Evaluator(extraName = "ConstantLong", warnExceptions = { ArithmeticException.class })
    static double processConstantLong(double d, @Fixed long scaleFactor) {
        return NumericUtils.asFiniteNumber(Math.scalb(d, Math.toIntExact(scaleFactor)));
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Scalb(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Scalb::new, d(), scaleFactor());
    }

    public Expression d() {
        return d;
    }

    public Expression scaleFactor() {
        return scaleFactor;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var dEval = Cast.cast(source(), d.dataType(), DataType.DOUBLE, toEvaluator.apply(d));
        if (scaleFactor.foldable()) {
            return switch (scaleFactor.dataType()) {
                case DataType.INTEGER -> new ScalbConstantIntEvaluator.Factory(
                    source(),
                    dEval,
                    (Integer) (scaleFactor.fold(toEvaluator.foldCtx()))
                );
                case DataType.LONG -> new ScalbConstantLongEvaluator.Factory(
                    source(),
                    dEval,
                    (Long) (scaleFactor.fold(toEvaluator.foldCtx()))
                );
                default -> throw new IllegalStateException("Invalid type for scaleFactor, should be int or long.");
            };
        }
        var scaleFactorEval = toEvaluator.apply(scaleFactor);
        return switch (scaleFactor.dataType()) {
            case DataType.INTEGER -> new ScalbIntEvaluator.Factory(source(), dEval, scaleFactorEval);
            case DataType.LONG -> new ScalbLongEvaluator.Factory(source(), dEval, scaleFactorEval);
            case DataType type -> throw new IllegalStateException(
                Strings.format("Invalid type for scaleFactor: %s, should be int or long.", type)
            );
        };
    }
}
