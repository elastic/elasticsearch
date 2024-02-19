/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.TriFunction;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asUnsignedLong;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;

public class Round extends EsqlScalarFunction implements OptionalArgument {

    private static final BiFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> EVALUATOR_IDENTITY = (s, e) -> e;

    private final Expression field, decimals;

    // @TODO: add support for "integer", "long", "unsigned_long" once tests are fixed
    @FunctionInfo(returnType = "double", description = "Rounds a number to the closest number with the specified number of digits.")
    public Round(
        Source source,
        @Param(name = "value", type = "double", description = "The numeric value to round") Expression field,
        @Param(
            optional = true,
            name = "decimals",
            type = { "integer" },
            description = "The number of decimal places to round to. Defaults to 0."
        ) Expression decimals
    ) {
        super(source, decimals != null ? Arrays.asList(field, decimals) : Arrays.asList(field));
        this.field = field;
        this.decimals = decimals;
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

        return decimals == null ? TypeResolution.TYPE_RESOLVED : isInteger(decimals, sourceText(), SECOND);
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
            BigInteger unsignedLong = asUnsignedLong(rounded);
            return asLongUnsigned(unsignedLong);
        } else {
            return asLongUnsigned(Maths.round(ul.longValue(), decimals));
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
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        DataType fieldType = dataType();
        if (fieldType == DataTypes.DOUBLE) {
            return toEvaluator(toEvaluator, RoundDoubleNoDecimalsEvaluator.Factory::new, RoundDoubleEvaluator.Factory::new);
        }
        if (fieldType == DataTypes.INTEGER) {
            return toEvaluator(toEvaluator, EVALUATOR_IDENTITY, RoundIntEvaluator.Factory::new);
        }
        if (fieldType == DataTypes.LONG) {
            return toEvaluator(toEvaluator, EVALUATOR_IDENTITY, RoundLongEvaluator.Factory::new);
        }
        if (fieldType == DataTypes.UNSIGNED_LONG) {
            return toEvaluator(toEvaluator, EVALUATOR_IDENTITY, RoundUnsignedLongEvaluator.Factory::new);
        }
        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    private ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, ExpressionEvaluator.Factory> toEvaluator,
        BiFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> noDecimals,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> withDecimals
    ) {
        var fieldEvaluator = toEvaluator.apply(field());
        if (decimals == null) {
            return noDecimals.apply(source(), fieldEvaluator);
        }
        var decimalsEvaluator = Cast.cast(source(), decimals().dataType(), DataTypes.LONG, toEvaluator.apply(decimals()));
        return withDecimals.apply(source(), fieldEvaluator, decimalsEvaluator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, decimals);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Round other = (Round) obj;
        return Objects.equals(other.field, field) && Objects.equals(other.decimals, decimals);
    }
}
