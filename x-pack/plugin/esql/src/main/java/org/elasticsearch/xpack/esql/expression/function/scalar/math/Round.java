/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
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

public class Round extends ScalarFunction implements OptionalArgument, EvaluatorMapper {

    private final Expression field, decimals;

    public Round(Source source, Expression field, Expression decimals) {
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

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
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
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        DataType fieldType = dataType();
        if (fieldType == DataTypes.DOUBLE) {
            return toEvaluator(toEvaluator, RoundDoubleNoDecimalsEvaluator.Factory::new, RoundDoubleEvaluator.Factory::new);
        }
        if (fieldType == DataTypes.INTEGER) {
            return toEvaluator(toEvaluator, Function.identity(), RoundIntEvaluator.Factory::new);
        }
        if (fieldType == DataTypes.LONG) {
            return toEvaluator(toEvaluator, Function.identity(), RoundLongEvaluator.Factory::new);
        }
        if (fieldType == DataTypes.UNSIGNED_LONG) {
            return toEvaluator(toEvaluator, Function.identity(), RoundUnsignedLongEvaluator.Factory::new);
        }
        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    private ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, ExpressionEvaluator.Factory> toEvaluator,
        Function<ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> noDecimals,
        BiFunction<ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> withDecimals
    ) {
        var fieldEvaluator = toEvaluator.apply(field());
        if (decimals == null) {
            return noDecimals.apply(fieldEvaluator);
        }
        var decimalsEvaluator = Cast.cast(decimals().dataType(), DataTypes.LONG, toEvaluator.apply(decimals()));
        return withDecimals.apply(fieldEvaluator, decimalsEvaluator);
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
