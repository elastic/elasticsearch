/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateUnitCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneOffset;
import java.time.temporal.ChronoField;

/**
 * PromQL built-in function definitions that do not correspond to a dedicated ES|QL function class.
 */
public class PromqlBuiltinFunctionDefinitions {

    /**
     * {@code topk(k, v)} selects the {@code k} series with the highest values rather than reducing the input
     * vector to a single value, so - unlike {@code sum}/{@code avg}/{@code max}/{@code min} - it has no ES|QL
     * aggregate function to build: the ctor reference passes the input field through unchanged, and the PromQL
     * translator (see {@code TranslatePromqlToEsqlPlan#wrapWithTopNBy}) appends the ranking/limiting step itself.
     */
    public static final PromqlFunctionDefinition TOPK = PromqlFunctionDefinition.def()
        .acrossSeriesBinaryReduction(PromqlFunctionDefinition.K, (source, field, filter, window, k) -> field)
        .description(
            "Returns `k` time series with the highest values, keeping their full label set. "
                + "When used with `by`, `topk` ranks independently within each group."
        )
        .example("topk(3, http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_GA_9_5)
        .differenceFromPrometheus(
            "A `k` close to Integer.MAX_VALUE can trip {{es}}'s circuit breaker (the execution engine allocates a "
                + "buffer sized to `k`, not to the number of matching series), whereas Prometheus has no equivalent limit. "
                + "A `without` grouping clause is not yet supported."
        )
        .name("topk");

    public static final PromqlFunctionDefinition VECTOR = PromqlFunctionDefinition.def()
        .vectorConversion()
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Returns the scalar as a vector with no labels.")
        .example("vector(1)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("vector");

    static final PromqlFunctionDefinition SCALAR = PromqlFunctionDefinition.def()
        .scalarConversion(Scalar::new)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            Returns the sample value of a single-element instant vector as a scalar. \
            If the input vector does not have exactly one element, scalar returns NaN.""")
        .example("scalar(sum(http_requests_total))")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("scalar");

    static final PromqlFunctionDefinition YEAR = dateExtraction(ChronoField.YEAR).description(
        "Returns the year for each of the input timestamps (in UTC)."
    ).example("year()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("year");

    static final PromqlFunctionDefinition MONTH = dateExtraction(ChronoField.MONTH_OF_YEAR).description(
        "Returns the month of the year for each of the input timestamps (in UTC). Returned values are from 1 to 12."
    ).example("month()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("month");

    static final PromqlFunctionDefinition DAY_OF_MONTH = dateExtraction(ChronoField.DAY_OF_MONTH).description(
        "Returns the day of the month for each of the input timestamps (in UTC). Returned values are from 1 to 31."
    ).example("day_of_month()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("day_of_month");

    static final PromqlFunctionDefinition DAY_OF_YEAR = dateExtraction(ChronoField.DAY_OF_YEAR).description(
        "Returns the day of the year for each of the input timestamps (in UTC). Returned values are from 1 to 366."
    ).example("day_of_year()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("day_of_year");

    // DateExtract(DAY_OF_WEEK) returns 1=Mon..7=Sun; PromQL expects 0=Sun..6=Sat, so we apply % 7.
    static final PromqlFunctionDefinition DAY_OF_WEEK = PromqlFunctionDefinition.def()
        .dateTime(
            (source, date, configuration) -> new Mod(
                source,
                new ToDouble(
                    source,
                    new DateExtract(
                        source,
                        Literal.keyword(source, ChronoField.DAY_OF_WEEK.name()),
                        date,
                        configuration.withSetting(QuerySettings.TIME_ZONE, ZoneOffset.UTC)
                    )
                ),
                Literal.fromDouble(source, 7.0)
            )
        )
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "Returns the day of the week for each of the input timestamps (in UTC). Returned values are from 0 to 6, where 0 means Sunday."
        )
        .example("day_of_week()")
        .stack(PromqlFunctionDefinition.STACK_GA_9_5)
        .name("day_of_week");

    static final PromqlFunctionDefinition HOUR = dateExtraction(ChronoField.HOUR_OF_DAY).description(
        "Returns the hour of the day for each of the input timestamps (in UTC). Returned values are from 0 to 23."
    ).example("hour()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("hour");

    static final PromqlFunctionDefinition MINUTE = dateExtraction(ChronoField.MINUTE_OF_HOUR).description(
        "Returns the minute of the hour for each of the input timestamps (in UTC). Returned values are from 0 to 59."
    ).example("minute()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("minute");

    static final PromqlFunctionDefinition DAYS_IN_MONTH = PromqlFunctionDefinition.def()
        .dateTime(
            (source, date, configuration) -> new ToDouble(
                source,
                new DateUnitCount(
                    source,
                    Literal.keyword(source, "day"),
                    Literal.keyword(source, "month"),
                    date,
                    configuration.withSetting(QuerySettings.TIME_ZONE, ZoneOffset.UTC)
                )
            )
        )
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "Returns the number of days in the month for each of the input timestamps (in UTC). Returned values are from 28 to 31."
        )
        .example("days_in_month()")
        .stack(PromqlFunctionDefinition.STACK_GA_9_5)
        .name("days_in_month");

    static final PromqlFunctionDefinition TIME = PromqlFunctionDefinition.def()
        .scalarWithStep((source, step) -> new Div(source, new ToDouble(source, step), Literal.fromDouble(source, 1000.0)))
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            Returns the number of seconds since January 1, 1970 UTC. \
            Note that this does not actually return the current time, but the time at which the expression is being evaluated.""")
        .example("time()")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("time");

    static final PromqlFunctionDefinition ROUND = PromqlFunctionDefinition.def()
        .binaryOptionalValueTransformation(PromqlFunctionDefinition.TO_NEAREST, (source, value, toNearest, configuration) -> {
            if (toNearest == null) {
                return new Round(source, value, null);
            } else {
                return promqlRoundToNearest(source, value, toNearest, configuration);
            }
        })
        .example("round(rate(http_requests_total[5m]))")
        .description("Rounds the sample values to the nearest integer, or to the nearest multiple of the optional argument.")
        .differenceFromPrometheus(
            "With a `to_nearest` argument, ties round up, matching Prometheus. Called with a single argument, a `NaN` input "
                + "returns `0` instead of `NaN`."
        )
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("round");

    /**
     * PromQL {@code round(v, to_nearest)} rounds to the nearest multiple of {@code to_nearest},
     * with ties resolved by rounding up. Matches Prometheus:
     * {@code floor(v * (1 / to_nearest) + 0.5) / (1 / to_nearest)}.
     */
    private static Expression promqlRoundToNearest(Source source, Expression value, Expression toNearest, Configuration configuration) {
        Expression inverse = new Div(source, Literal.fromDouble(source, 1.0), toNearest);
        Expression half = Literal.fromDouble(source, 0.5);
        Expression scaled = new Mul(source, value, inverse);
        Expression withHalf = new Add(source, scaled, half, configuration);
        return new Div(source, new Floor(source, withHalf), inverse);
    }

    private static PromqlFunctionDefinition.Builder dateExtraction(ChronoField field) {
        return PromqlFunctionDefinition.def()
            .dateTime(
                (source, date, configuration) -> new ToDouble(
                    source,
                    new DateExtract(
                        source,
                        Literal.keyword(source, field.name()),
                        date,
                        configuration.withSetting(QuerySettings.TIME_ZONE, ZoneOffset.UTC)
                    )
                )
            )
            .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED);
    }

    private PromqlBuiltinFunctionDefinitions() {}
}
