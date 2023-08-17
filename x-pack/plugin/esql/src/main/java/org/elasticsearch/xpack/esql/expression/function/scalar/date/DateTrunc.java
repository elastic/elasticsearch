/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public class DateTrunc extends BinaryDateTimeFunction implements Mappable {

    public DateTrunc(Source source, Expression field, Expression interval) {
        super(source, field, interval);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isDate(timestampField(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isInterval(interval(), sourceText(), SECOND);
    }

    private static TypeResolution isInterval(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(
            e,
            dt -> dt == EsqlDataTypes.DATE_PERIOD || dt == EsqlDataTypes.TIME_DURATION,
            operationName,
            paramOrd,
            "dateperiod",
            "timeduration"
        );
    }

    @Override
    public Object fold() {
        return Mappable.super.fold();
    }

    @Evaluator
    static long process(long fieldVal, @Fixed Rounding.Prepared rounding) {
        return rounding.round(fieldVal);
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new DateTrunc(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateTrunc::new, timestampField(), interval());
    }

    public Expression interval() {
        return right();
    }

    static Rounding.Prepared createRounding(final Object interval) {
        return createRounding(interval, DEFAULT_TZ);
    }

    public static Rounding.Prepared createRounding(final Object interval, final ZoneId timeZone) {
        if (interval instanceof Period period) {
            return createRounding(period, timeZone);
        } else if (interval instanceof Duration duration) {
            return createRounding(duration, timeZone);
        }
        throw new IllegalArgumentException("Time interval is not supported");
    }

    private static Rounding.Prepared createRounding(final Period period, final ZoneId timeZone) {
        // Zero or negative intervals are not supported
        if (period == null || period.isNegative() || period.isZero()) {
            throw new IllegalArgumentException("Zero or negative time interval is not supported");
        }

        long periods = period.getUnits().stream().filter(unit -> period.get(unit) != 0).count();
        if (periods != 1) {
            throw new IllegalArgumentException("Time interval is not supported");
        }

        final Rounding.Builder rounding;
        if (period.getDays() == 1) {
            rounding = new Rounding.Builder(Rounding.DateTimeUnit.DAY_OF_MONTH);
        } else if (period.getDays() == 7) {
            // java.time.Period does not have a WEEKLY period, so a period of 7 days
            // returns a weekly rounding
            rounding = new Rounding.Builder(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR);
        } else if (period.getDays() > 1) {
            rounding = new Rounding.Builder(new TimeValue(period.getDays(), TimeUnit.DAYS));
        } else if (period.getMonths() == 1) {
            rounding = new Rounding.Builder(Rounding.DateTimeUnit.MONTH_OF_YEAR);
        } else if (period.getMonths() == 3) {
            // java.time.Period does not have a QUATERLY period, so a period of 3 months
            // returns a quarterly rounding
            rounding = new Rounding.Builder(Rounding.DateTimeUnit.QUARTER_OF_YEAR);
        } else if (period.getYears() == 1) {
            rounding = new Rounding.Builder(Rounding.DateTimeUnit.YEAR_OF_CENTURY);
        } else {
            throw new IllegalArgumentException("Time interval is not supported");
        }

        rounding.timeZone(timeZone);
        return rounding.build().prepareForUnknown();
    }

    private static Rounding.Prepared createRounding(final Duration duration, final ZoneId timeZone) {
        // Zero or negative intervals are not supported
        if (duration == null || duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("Zero or negative time interval is not supported");
        }

        final Rounding.Builder rounding = new Rounding.Builder(TimeValue.timeValueMillis(duration.toMillis()));
        rounding.timeZone(timeZone);
        return rounding.build().prepareForUnknown();
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> fieldEvaluator = toEvaluator.apply(timestampField());
        Expression interval = interval();
        if (interval.foldable() == false) {
            throw new IllegalArgumentException("Function [" + sourceText() + "] has invalid interval [" + interval().sourceText() + "].");
        }
        Object foldedInterval;
        try {
            foldedInterval = interval.fold();
            if (foldedInterval == null) {
                throw new IllegalArgumentException("Interval cannot not be null");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Function [" + sourceText() + "] has invalid interval [" + interval().sourceText() + "]. " + e.getMessage()
            );
        }
        return evaluator(fieldEvaluator, DateTrunc.createRounding(foldedInterval, zoneId()));
    }

    public static Supplier<EvalOperator.ExpressionEvaluator> evaluator(
        Supplier<EvalOperator.ExpressionEvaluator> fieldEvaluator,
        Rounding.Prepared rounding
    ) {
        return () -> new DateTruncEvaluator(fieldEvaluator.get(), rounding);
    }
}
