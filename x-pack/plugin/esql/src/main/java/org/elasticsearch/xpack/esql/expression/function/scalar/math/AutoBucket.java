/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Buckets dates into a given number of buckets.
 * <p>
 *     Takes a date field and three constants and picks a bucket size based on the
 *     constants. The constants are "target bucket count", "from", and "to". It looks like:
 *     {@code auto_bucket(hire_date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")}.
 *     We have a list of "human" bucket sizes like "one month" and "four hours". We pick
 *     the largest range that covers the range in fewer than the target bucket count. So
 *     in the above case we'll pick month long buckets, yielding 12 buckets.
 * </p>
 */
public class AutoBucket extends ScalarFunction implements EvaluatorMapper {
    // TODO maybe we should just cover the whole of representable dates here - like ten years, 100 years, 1000 years, all the way up.
    // That way you never end up with more than the target number of buckets.
    private static final Rounding LARGEST_HUMAN_DATE_ROUNDING = Rounding.builder(Rounding.DateTimeUnit.YEAR_OF_CENTURY).build();
    private static final Rounding[] HUMAN_DATE_ROUNDINGS = new Rounding[] {
        Rounding.builder(Rounding.DateTimeUnit.MONTH_OF_YEAR).build(),
        Rounding.builder(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR).build(),
        Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build(),
        Rounding.builder(TimeValue.timeValueHours(12)).build(),
        Rounding.builder(TimeValue.timeValueHours(3)).build(),
        Rounding.builder(TimeValue.timeValueHours(1)).build(),
        Rounding.builder(TimeValue.timeValueMinutes(30)).build(),
        Rounding.builder(TimeValue.timeValueMinutes(10)).build(),
        Rounding.builder(TimeValue.timeValueMinutes(5)).build(),
        Rounding.builder(TimeValue.timeValueMinutes(1)).build(),
        Rounding.builder(TimeValue.timeValueSeconds(30)).build(),
        Rounding.builder(TimeValue.timeValueSeconds(10)).build(),
        Rounding.builder(TimeValue.timeValueSeconds(5)).build(),
        Rounding.builder(TimeValue.timeValueSeconds(1)).build(),
        Rounding.builder(TimeValue.timeValueMillis(100)).build(),
        Rounding.builder(TimeValue.timeValueMillis(50)).build(),
        Rounding.builder(TimeValue.timeValueMillis(10)).build(),
        Rounding.builder(TimeValue.timeValueMillis(1)).build(), };

    private final Expression field;
    private final Expression buckets;
    private final Expression from;
    private final Expression to;

    @FunctionInfo(returnType = { "double", "date" })
    public AutoBucket(
        Source source,
        @Param(name = "field", type = { "integer", "long", "double", "date" }) Expression field,
        @Param(name = "buckets", type = { "integer" }) Expression buckets,
        @Param(name = "from", type = { "integer", "long", "double", "date" }) Expression from,
        @Param(name = "to", type = { "integer", "long", "double", "date" }) Expression to
    ) {
        super(source, List.of(field, buckets, from, to));
        this.field = field;
        this.buckets = buckets;
        this.from = from;
        this.to = to;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && buckets.foldable() && from.foldable() && to.foldable();
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        int b = ((Number) buckets.fold()).intValue();

        if (field.dataType() == DataTypes.DATETIME) {
            long f = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(((BytesRef) from.fold()).utf8ToString());
            long t = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(((BytesRef) to.fold()).utf8ToString());
            return DateTrunc.evaluator(toEvaluator.apply(field), new DateRoundingPicker(b, f, t).pickRounding().prepareForUnknown());
        }
        if (field.dataType().isNumeric()) {
            double f = ((Number) from.fold()).doubleValue();
            double t = ((Number) to.fold()).doubleValue();

            // We could make this more efficient, either by generating the evaluators with byte code or hand rolling this one.
            Literal rounding = new Literal(source(), pickRounding(b, f, t), DataTypes.DOUBLE);
            Div div = new Div(source(), field, rounding);
            Floor floor = new Floor(source(), div);
            Mul mul = new Mul(source(), floor, rounding);
            return toEvaluator.apply(mul);
        }
        throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
    }

    private record DateRoundingPicker(int buckets, long from, long to) {
        Rounding pickRounding() {
            Rounding prev = LARGEST_HUMAN_DATE_ROUNDING;
            for (Rounding r : HUMAN_DATE_ROUNDINGS) {
                if (roundingIsOk(r)) {
                    prev = r;
                } else {
                    return prev;
                }
            }
            return prev;
        }

        /**
         * True if the rounding produces less than or equal to the requested number of buckets.
         */
        boolean roundingIsOk(Rounding rounding) {
            Rounding.Prepared r = rounding.prepareForUnknown();
            long bucket = r.round(from);
            int used = 0;
            while (used < buckets) {
                bucket = r.nextRoundingValue(bucket);
                used++;
                if (bucket > to) {
                    return true;
                }
            }
            return false;
        }
    }

    private double pickRounding(int buckets, double from, double to) {
        double precise = (to - from) / buckets;
        double nextPowerOfTen = Math.pow(10, Math.ceil(Math.log10(precise)));
        double halfPower = nextPowerOfTen / 2;
        return precise < halfPower ? halfPower : nextPowerOfTen;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (field.dataType() == DataTypes.DATETIME) {
            return resolveType((e, o) -> isString(e, sourceText(), o));
        }
        if (field.dataType().isNumeric()) {
            return resolveType((e, o) -> isNumeric(e, sourceText(), o));
        }
        return isType(field, e -> false, sourceText(), FIRST, "datetime", "numeric");
    }

    private TypeResolution resolveType(BiFunction<Expression, TypeResolutions.ParamOrdinal, TypeResolution> checkThirdAndForth) {
        TypeResolution resolution = isInteger(buckets, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isFoldable(buckets, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = checkThirdAndForth.apply(from, THIRD);
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isFoldable(from, sourceText(), THIRD);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = checkThirdAndForth.apply(to, FOURTH);
        if (resolution.unresolved()) {
            return resolution;
        }
        return isFoldable(to, sourceText(), FOURTH);
    }

    @Override
    public DataType dataType() {
        if (field.dataType().isNumeric()) {
            return DataTypes.DOUBLE;
        }
        return field.dataType();
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new AutoBucket(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, AutoBucket::new, field, buckets, from, to);
    }

    public Expression field() {
        return field;
    }

    public Expression buckets() {
        return buckets;
    }

    public Expression from() {
        return from;
    }

    public Expression to() {
        return to;
    }

    @Override
    public String toString() {
        return "AutoBucket{" + "field=" + field + ", buckets=" + buckets + ", from=" + from + ", to=" + to + '}';
    }
}
