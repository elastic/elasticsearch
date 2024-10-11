/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isWholeNumber;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.bigIntegerToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;

public class Round extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Round", Round::new);

    private static final BiFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> EVALUATOR_IDENTITY = (s, e) -> e;

    private final Expression field, decimals;

    // @TODO: add support for "integer", "long", "unsigned_long" once tests are fixed
    @FunctionInfo(returnType = { "double", "integer", "long", "unsigned_long" }, description = """
        Rounds a number to the specified number of decimal places.
        Defaults to 0, which returns the nearest integer. If the
        precision is a negative number, rounds to the number of digits left
        of the decimal point.""", examples = @Example(file = "docs", tag = "round"))
    public Round(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "The numeric value to round. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            optional = true,
            name = "decimals",
            type = { "integer" },  // TODO long is supported here too
            description = "The number of decimal places to round to. Defaults to 0. If `null`, the function returns `null`."
        ) Expression decimals
    ) {
        super(source, decimals != null ? Arrays.asList(field, decimals) : Arrays.asList(field));
        this.field = field;
        this.decimals = decimals;
    }

    private Round(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeOptionalNamedWriteable(decimals);
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

        TypeResolution resolution = isNumeric(field, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return decimals == null ? TypeResolution.TYPE_RESOLVED : isWholeNumber(decimals, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return field.foldable() && (decimals == null || decimals.foldable());
    }

    @Evaluator(extraName = "DoubleNoDecimals")
    static double process(double val) {
        return Maths.round(val, 0).doubleValue();
    }

    @Evaluator(extraName = "Int")
    static int process(int val, long decimals) {
        return Maths.round(val, decimals).intValue();
    }

    @Evaluator(extraName = "Long")
    static long process(long val, long decimals) {
        return Maths.round(val, decimals).longValue();
    }

    @Evaluator(extraName = "UnsignedLong")
    static long processUnsignedLong(long val, long decimals) {
        Number ul = unsignedLongAsNumber(val);
        if (ul instanceof BigInteger bi) {
            BigInteger rounded = Maths.round(bi, decimals);
            return bigIntegerToUnsignedLong(rounded);
        } else {
            return longToUnsignedLong(Maths.round(ul.longValue(), decimals), false);
        }
    }

    @Evaluator(extraName = "Double")
    static double process(double val, long decimals) {
        return Maths.round(val, decimals).doubleValue();
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Round(source(), newChildren.get(0), decimals() == null ? null : newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Round::new, field(), decimals());
    }

    public Expression field() {
        return field;
    }

    public Expression decimals() {
        return decimals;
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        DataType fieldType = dataType();
        if (fieldType == DataType.DOUBLE) {
            return toEvaluator(toEvaluator, RoundDoubleNoDecimalsEvaluator.Factory::new, RoundDoubleEvaluator.Factory::new);
        }
        if (fieldType == DataType.INTEGER) {
            return toEvaluator(toEvaluator, EVALUATOR_IDENTITY, RoundIntEvaluator.Factory::new);
        }
        if (fieldType == DataType.LONG) {
            return toEvaluator(toEvaluator, EVALUATOR_IDENTITY, RoundLongEvaluator.Factory::new);
        }
        if (fieldType == DataType.UNSIGNED_LONG) {
            return toEvaluator(toEvaluator, EVALUATOR_IDENTITY, RoundUnsignedLongEvaluator.Factory::new);
        }
        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    private ExpressionEvaluator.Factory toEvaluator(
        ToEvaluator toEvaluator,
        BiFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> noDecimals,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> withDecimals
    ) {
        var fieldEvaluator = toEvaluator.apply(field());
        if (decimals == null) {
            return noDecimals.apply(source(), fieldEvaluator);
        }
        var decimalsEvaluator = Cast.cast(source(), decimals().dataType(), DataType.LONG, toEvaluator.apply(decimals()));
        return withDecimals.apply(source(), fieldEvaluator, decimalsEvaluator);
    }
}
