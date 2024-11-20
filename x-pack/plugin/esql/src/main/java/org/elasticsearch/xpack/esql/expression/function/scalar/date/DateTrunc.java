/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;

public class DateTrunc extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DateTrunc",
        DateTrunc::new
    );

    @FunctionalInterface
    public interface DateTruncFactoryProvider {
        ExpressionEvaluator.Factory apply(Source source, ExpressionEvaluator.Factory lhs, Rounding.Prepared rounding);
    }

    private static final Map<DataType, DateTruncFactoryProvider> evaluatorMap = Map.ofEntries(
        Map.entry(DATETIME, DateTruncDatetimeEvaluator.Factory::new),
        Map.entry(DATE_NANOS, DateTruncDateNanosEvaluator.Factory::new)
    );
    private final Expression interval;
    private final Expression timestampField;
    protected static final ZoneId DEFAULT_TZ = ZoneOffset.UTC;

    @FunctionInfo(
        returnType = { "date", "date_nanos" },
        description = "Rounds down a date to the closest interval.",
        examples = {
            @Example(file = "date", tag = "docsDateTrunc"),
            @Example(
                description = "Combine `DATE_TRUNC` with <<esql-stats-by>> to create date histograms. For\n"
                    + "example, the number of hires per year:",
                file = "date",
                tag = "docsDateTruncHistogram"
            ),
            @Example(description = "Or an hourly error rate:", file = "conditional", tag = "docsCaseHourlyErrorRate") }
    )
    public DateTrunc(
        Source source,
        // Need to replace the commas in the description here with semi-colon as there's a bug in the CSV parser
        // used in the CSVTests and fixing it is not trivial
        @Param(
            name = "interval",
            type = { "date_period", "time_duration" },
            description = "Interval; expressed using the timespan literal syntax."
        ) Expression interval,
        @Param(name = "date", type = { "date", "date_nanos" }, description = "Date expression") Expression field
    ) {
        super(source, List.of(interval, field));
        this.interval = interval;
        this.timestampField = field;
    }

    private DateTrunc(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(interval);
        out.writeNamedWriteable(timestampField);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression interval() {
        return interval;
    }

    Expression field() {
        return timestampField;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        String operationName = sourceText();
        return isType(interval, DataType::isTemporalAmount, sourceText(), FIRST, "dateperiod", "timeduration").and(
            isType(timestampField, evaluatorMap::containsKey, operationName, SECOND, "date_nanos or datetime")
        );
    }

    public DataType dataType() {
        // Default to DATETIME in the case of nulls. This mimics the behavior before DATE_NANOS support
        return timestampField.dataType() == DataType.NULL ? DATETIME : timestampField.dataType();
    }

    @Evaluator(extraName = "Datetime")
    static long processDatetime(long fieldVal, @Fixed Rounding.Prepared rounding) {
        return rounding.round(fieldVal);
    }

    @Evaluator(extraName = "DateNanos")
    static long processDateNanos(long fieldVal, @Fixed Rounding.Prepared rounding) {
        // Currently, ES|QL doesn't support rounding to sub-millisecond values, so it's safe to cast before rounding.
        return DateUtils.toNanoSeconds(rounding.round(DateUtils.toMilliSeconds(fieldVal)));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateTrunc(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateTrunc::new, children().get(0), children().get(1));
    }

    @Override
    public boolean foldable() {
        return interval.foldable() && timestampField.foldable();
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
            throw new IllegalArgumentException("Time interval with multiple periods is not supported");
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
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(timestampField);
        if (interval.foldable() == false) {
            throw new IllegalArgumentException("Function [" + sourceText() + "] has invalid interval [" + interval.sourceText() + "].");
        }
        Object foldedInterval;
        try {
            foldedInterval = interval.fold();
            if (foldedInterval == null) {
                throw new IllegalArgumentException("Interval cannot not be null");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Function [" + sourceText() + "] has invalid interval [" + interval.sourceText() + "]. " + e.getMessage()
            );
        }
        return evaluator(dataType(), source(), fieldEvaluator, DateTrunc.createRounding(foldedInterval, DEFAULT_TZ));
    }

    public static ExpressionEvaluator.Factory evaluator(
        DataType forType,
        Source source,
        ExpressionEvaluator.Factory fieldEvaluator,
        Rounding.Prepared rounding
    ) {
        return evaluatorMap.get(forType).apply(source, fieldEvaluator, rounding);
    }
}
