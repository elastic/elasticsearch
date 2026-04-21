/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;

import java.time.ZoneOffset;
import java.time.temporal.ChronoField;

/**
 * PromQL built-in function definitions that do not correspond to a dedicated ES|QL function class.
 */
class PromqlBuiltinFunctionDefinitions {

    static final PromqlFunctionDefinition VECTOR = PromqlFunctionDefinition.def()
        .vectorConversion()
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Returns the scalar as a vector with no labels.")
        .example("vector(1)")
        .name("vector");

    static final PromqlFunctionDefinition SCALAR = PromqlFunctionDefinition.def()
        .scalarConversion(Scalar::new)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            Returns the sample value of a single-element instant vector as a scalar. \
            If the input vector does not have exactly one element, scalar returns NaN.""")
        .example("scalar(sum(http_requests_total))")
        .name("scalar");

    static final PromqlFunctionDefinition YEAR = dateExtraction(ChronoField.YEAR).description(
        "returns the year of each of those timestamps (in UTC)"
    ).example("year()").name("year");

    static final PromqlFunctionDefinition MONTH = dateExtraction(ChronoField.MONTH_OF_YEAR).description(
        "returns the month of the year for each of those timestamps (in UTC). Returned values are from 1 to 12."
    ).example("month()").name("month");

    static final PromqlFunctionDefinition DAY_OF_MONTH = dateExtraction(ChronoField.DAY_OF_MONTH).description(
        "returns the day of the month for each of those timestamps (in UTC). Returned values are from 1 to 31."
    ).example("day_of_month()").name("day_of_month");

    static final PromqlFunctionDefinition DAY_OF_YEAR = dateExtraction(ChronoField.DAY_OF_YEAR).description(
        "returns the day of the year for each of those timestamps (in UTC). Returned values are from 1 to 366."
    ).example("day_of_year()").name("day_of_year");

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
                        configuration.withZoneId(ZoneOffset.UTC)
                    )
                ),
                Literal.fromDouble(source, 7.0)
            )
        )
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "returns the day of the week for each of those timestamps (in UTC). Returned values are from 0 to 6, where 0 means Sunday."
        )
        .example("day_of_week()")
        .name("day_of_week");

    static final PromqlFunctionDefinition HOUR = dateExtraction(ChronoField.HOUR_OF_DAY).description(
        "returns the hour of the day for each of those timestamps (in UTC). Returned values are from 0 to 23."
    ).example("hour()").name("hour");

    static final PromqlFunctionDefinition MINUTE = dateExtraction(ChronoField.MINUTE_OF_HOUR).description(
        "returns the minute of the hour for each of those timestamps (in UTC). Returned values are from 0 to 59."
    ).example("minute()").name("minute");

    static final PromqlFunctionDefinition TIME = PromqlFunctionDefinition.def()
        .scalarWithStep((source, step) -> new Div(source, new ToDouble(source, step), Literal.fromDouble(source, 1000.0)))
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            returns the number of seconds since January 1, 1970 UTC. \
            Note that this does not actually return the current time, but the time at which the expression is to be evaluated.""")
        .example("time()")
        .name("time");

    private static PromqlFunctionDefinition.Builder dateExtraction(ChronoField field) {
        return PromqlFunctionDefinition.def()
            .dateTime(
                (source, date, configuration) -> new ToDouble(
                    source,
                    new DateExtract(source, Literal.keyword(source, field.name()), date, configuration.withZoneId(ZoneOffset.UTC))
                )
            )
            .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED);
    }

    private PromqlBuiltinFunctionDefinitions() {}
}
