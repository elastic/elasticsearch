/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.List;

/**
 * An optimizer rule that replaces DATE_FORMAT function calls in GROUP BY clauses with more efficient DATE_TRUNC operations.
 * This optimization is possible when the date format pattern can be mapped to a specific time interval.
 * <p>
 * For example,
 * {@code STATS my_sum = SUM(value) BY month = DATE_FORMAT("yyyy-MM", timestamp) }
 * can be optimized to
 * {@code STATS my_sum = SUM(value) BY month= DATE_TRUNC(1 month, timestamp) | EVAL month = DATE_FORMAT("yyyy-MM", month) }
 * which is more efficient for grouping operations.
 * <p>
 * The rule analyzes the format pattern and maps it to the smallest possible time interval that preserves the grouping semantics.
 * Supported intervals range from nanoseconds to years, including special cases like quarters and weeks.
 * <p>
 * This optimization not only improves performance but also ensures correctness in time-based grouping:
 * DATE_TRUNC properly handles timezone and daylight saving time (DST) transitions when using Period or Duration intervals,
 * while DATE_FORMAT does not account for these timezone-related considerations.
 */
public class ReplaceGroupingByDateFormatWithDateTrunc extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {

        List<Alias> evals = new ArrayList<>();
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings());

        // extract `DateFormat` in groupings
        for (int i = 0, s = newGroupings.size(); i < s; i++) {
            Expression g = newGroupings.get(i);
            if (g instanceof Alias as && as.child() instanceof DateFormat df) {
                Literal format = (Literal) df.children().getFirst();
                Expression field = df.children().get(1);

                Literal interval = formatToMinimalInterval((String) format.value(), g.source());
                // if the format can be optimized to `DATE_TRUNC`
                if (interval != null) {
                    DateTrunc dateTrunc = new DateTrunc(df.source(), interval, field);
                    Alias alias = as.replaceChild(dateTrunc);
                    newGroupings.set(i, alias);

                    Expression expression = df.replaceChildren(List.of(format, alias.toAttribute()));
                    evals.add(new Alias(as.source(), as.name(), expression));
                }
            }
        }

        if (evals.isEmpty() == false) {
            aggregate = aggregate.with(aggregate.child(), newGroupings, aggregate.aggregates());
            return new Eval(aggregate.source(), aggregate, evals);
        }
        return aggregate;
    }

    private static Literal formatToMinimalInterval(String format, Source source) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            String formatterAsString = formatter.toString();
            if (formatterAsString.contains(ChronoField.NANO_OF_SECOND.toString())
                || formatterAsString.contains(ChronoField.NANO_OF_DAY.toString())) {
                return new Literal(source, ChronoUnit.NANOS.getDuration(), DataType.TIME_DURATION);
            } else if (formatterAsString.contains(ChronoField.MILLI_OF_DAY.toString())) {
                return new Literal(source, ChronoUnit.MILLIS.getDuration(), DataType.TIME_DURATION);
            } else if (formatterAsString.contains(ChronoField.SECOND_OF_MINUTE.toString())) {
                return new Literal(source, ChronoUnit.SECONDS.getDuration(), DataType.TIME_DURATION);
            } else if (formatterAsString.contains(ChronoField.MINUTE_OF_HOUR.toString())) {
                return new Literal(source, ChronoUnit.MINUTES.getDuration(), DataType.TIME_DURATION);
            } else if (formatterAsString.contains(ChronoField.HOUR_OF_DAY.toString())
                || formatterAsString.contains(ChronoField.CLOCK_HOUR_OF_DAY.toString())
                || formatterAsString.contains(ChronoField.HOUR_OF_AMPM.toString())
                || formatterAsString.contains(ChronoField.CLOCK_HOUR_OF_AMPM.toString())) {
                    return new Literal(source, ChronoUnit.HOURS.getDuration(), DataType.TIME_DURATION);
                } else if (formatterAsString.contains(ChronoField.AMPM_OF_DAY.toString())) {
                    return new Literal(source, ChronoUnit.HALF_DAYS, DataType.TIME_DURATION);
                } else if (formatterAsString.contains(ChronoField.DAY_OF_WEEK.toString())) {
                    return new Literal(source, Period.ofDays(1), DataType.DATE_PERIOD);
                } else if (formatterAsString.contains(ChronoField.ALIGNED_WEEK_OF_MONTH.toString())) {
                    return new Literal(source, Period.ofDays(7), DataType.DATE_PERIOD);
                } else if (formatterAsString.contains(ChronoField.MONTH_OF_YEAR.toString())) {
                    return new Literal(source, Period.ofMonths(1), DataType.DATE_PERIOD);
                } else if (formatterAsString.contains(IsoFields.QUARTER_OF_YEAR.toString())) {
                    return new Literal(source, Period.ofMonths(3), DataType.DATE_PERIOD);
                } else if (formatterAsString.contains(ChronoField.YEAR_OF_ERA.toString())
                    || formatterAsString.contains(ChronoField.YEAR.toString())) {
                        return new Literal(source, Period.ofYears(1), DataType.DATE_PERIOD);
                    }
        } catch (IllegalArgumentException ignored) {}
        return null;
    }
}
