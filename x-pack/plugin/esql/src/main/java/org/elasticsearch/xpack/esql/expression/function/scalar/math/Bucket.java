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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.common.Failures;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Buckets dates into a given number of buckets.
 * <p>
 *     Takes a date field and three constants and picks a bucket size based on the
 *     constants. The constants are "target bucket count", "from", and "to". It looks like:
 *     {@code bucket(hire_date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")}.
 *     We have a list of "human" bucket sizes like "one month" and "four hours". We pick
 *     the largest range that covers the range in fewer than the target bucket count. So
 *     in the above case we'll pick month long buckets, yielding 12 buckets.
 * </p>
 */
public class Bucket extends EsqlScalarFunction implements Validatable, TwoOptionalArguments {
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

    private static final ZoneId DEFAULT_TZ = ZoneOffset.UTC; // TODO: plug in the config

    private final Expression field;
    private final Expression bucketsOrSpan;
    private final Expression from;
    private final Expression to;

    @FunctionInfo(returnType = { "double", "date" }, description = """
        Creates human-friendly buckets and returns a datetime value
        for each row that corresponds to the resulting bucket the row falls into.""")
    public Bucket(
        Source source,
        @Param(name = "field", type = { "integer", "long", "double", "date" }) Expression field,
        @Param(name = "bucketsOrSpan", type = { "integer", "double", "date_period", "time_duration" }) Expression bucketsOrSpan,
        @Param(name = "from", type = { "integer", "long", "double", "date", "keyword", "text" }, optional = true) Expression from,
        @Param(name = "to", type = { "integer", "long", "double", "date", "keyword", "text" }, optional = true) Expression to
    ) {
        super(source, from != null && to != null ? List.of(field, bucketsOrSpan, from, to) : List.of(field, bucketsOrSpan));
        this.field = field;
        this.bucketsOrSpan = bucketsOrSpan;
        this.from = from;
        this.to = to;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && bucketsOrSpan.foldable() && (from == null || from.foldable()) && (to == null || to.foldable());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        if (field.dataType() == DataTypes.DATETIME) {
            Rounding.Prepared preparedRounding;
            if (bucketsOrSpan.dataType().isInteger()) {
                int b = ((Number) bucketsOrSpan.fold()).intValue();
                long f = foldToLong(from);
                long t = foldToLong(to);
                preparedRounding = new DateRoundingPicker(b, f, t).pickRounding().prepareForUnknown();
            } else {
                preparedRounding = DateTrunc.createRounding(bucketsOrSpan.fold(), DEFAULT_TZ);
            }
            return DateTrunc.evaluator(source(), toEvaluator.apply(field), preparedRounding);
        }
        if (field.dataType().isNumeric()) {
            double r;
            if (from != null) {
                int b = ((Number) bucketsOrSpan.fold()).intValue();
                double f = ((Number) from.fold()).doubleValue();
                double t = ((Number) to.fold()).doubleValue();
                r = pickRounding(b, f, t);
            } else {
                r = ((Number) bucketsOrSpan.fold()).doubleValue();
            }
            Literal rounding = new Literal(source(), r, DataTypes.DOUBLE);

            // We could make this more efficient, either by generating the evaluators with byte code or hand rolling this one.
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

    // supported parameter type combinations (1st, 2nd, 3rd, 4th):
    // datetime, integer, string/datetime, string/datetime
    // datetime, span/duration, -, -
    // numeric, integer, numeric, numeric
    // numeric, double, -, -
    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var fieldType = field.dataType();
        var bucketsOrSpanType = bucketsOrSpan.dataType();
        if (fieldType == DataTypes.NULL || bucketsOrSpanType == DataTypes.NULL) {
            return TypeResolution.TYPE_RESOLVED;
        }

        if (fieldType == DataTypes.DATETIME) {
            TypeResolution resolution = isType(
                bucketsOrSpan,
                dt -> dt.isInteger() || EsqlDataTypes.isTemporalAmount(dt),
                sourceText(),
                SECOND,
                "integral",
                "date_period",
                "time_duration"
            );
            return bucketsOrSpanType.isInteger()
                ? resolution.and(checkArgsCount(4))
                    .and(() -> isStringOrDate(from, sourceText(), THIRD))
                    .and(() -> isStringOrDate(to, sourceText(), FOURTH))
                : resolution.and(checkArgsCount(2)); // temporal amount
        }
        if (fieldType.isNumeric()) {
            return bucketsOrSpanType.isInteger()
                ? checkArgsCount(4).and(() -> isNumeric(from, sourceText(), THIRD)).and(() -> isNumeric(to, sourceText(), FOURTH))
                : isNumeric(bucketsOrSpan, sourceText(), SECOND).and(checkArgsCount(2));
        }
        return isType(field, e -> false, sourceText(), FIRST, "datetime", "numeric");
    }

    private TypeResolution checkArgsCount(int expectedCount) {
        String expected = null;
        if (expectedCount == 2 && (from != null || to != null)) {
            expected = "two";
        } else if (expectedCount == 4 && (from == null || to == null)) {
            expected = "four";
        } else if ((from == null && to != null) || (from != null && to == null)) {
            expected = "two or four";
        }

        return expected == null
            ? TypeResolution.TYPE_RESOLVED
            : new TypeResolution(
                format(
                    null,
                    "function expects exactly {} arguments when the first one is of type [{}] and the second of type [{}]",
                    expected,
                    field.dataType(),
                    bucketsOrSpan.dataType()
                )
            );
    }

    private static TypeResolution isStringOrDate(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return TypeResolutions.isType(
            e,
            exp -> DataTypes.isString(exp) || DataTypes.isDateTime(exp),
            operationName,
            paramOrd,
            "datetime",
            "string"
        );
    }

    @Override
    public void validate(Failures failures) {
        String operation = sourceText();

        failures.add(isFoldable(bucketsOrSpan, operation, SECOND))
            .add(from != null ? isFoldable(from, operation, THIRD) : null)
            .add(to != null ? isFoldable(to, operation, FOURTH) : null);
    }

    private long foldToLong(Expression e) {
        Object value = Foldables.valueOf(e);
        return DataTypes.isDateTime(e.dataType()) ? ((Number) value).longValue() : dateTimeToLong(((BytesRef) value).utf8ToString());
    }

    @Override
    public DataType dataType() {
        if (field.dataType().isNumeric()) {
            return DataTypes.DOUBLE;
        }
        return field.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        Expression from = newChildren.size() > 2 ? newChildren.get(2) : null;
        Expression to = newChildren.size() > 3 ? newChildren.get(3) : null;
        return new Bucket(source(), newChildren.get(0), newChildren.get(1), from, to);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Bucket::new, field, bucketsOrSpan, from, to);
    }

    public Expression field() {
        return field;
    }

    public Expression buckets() {
        return bucketsOrSpan;
    }

    public Expression from() {
        return from;
    }

    public Expression to() {
        return to;
    }

    @Override
    public String toString() {
        return "Bucket{" + "field=" + field + ", bucketsOrSpan=" + bucketsOrSpan + ", from=" + from + ", to=" + to + '}';
    }
}
