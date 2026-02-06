/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.RuleUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * An optimizer rule that performs two main optimizations:
 * 1. Replaces nested expressions inside a {@link Aggregate} with synthetic eval
 * 2. Optimizes DATE_FORMAT function calls in GROUP BY clauses with more efficient DATE_TRUNC operations
 * <p>
 * For nested expressions in aggregates:
 * {@code STATS SUM(a + 1) BY x % 2}
 * becomes
 * {@code EVAL `a + 1` = a + 1, `x % 2` = x % 2 | STATS SUM(`a+1`_ref) BY `x % 2`_ref}
 * and
 * {@code INLINE STATS SUM(a + 1) BY x % 2}
 * becomes
 * {@code EVAL `a + 1` = a + 1, `x % 2` = x % 2 | INLINE STATS SUM(`a+1`_ref) BY `x % 2`_ref}
 * <p>
 * For date formatting optimization:
 * {@code STATS sum = SUM(value) BY month = DATE_FORMAT("yyyy-MM", timestamp) }
 * can be optimized to
 * {@code STATS sum = SUM(value) BY month1 = DATE_TRUNC(1 month, timestamp) | EVAL month = DATE_FORMAT("yyyy-MM", month1) | KEEP sum, month}
 * which is more efficient for grouping operations.
 * <p>
 * The date formatting optimization analyzes the format pattern and maps it to the smallest possible time interval
 * that preserves the grouping semantics. Supported intervals range from nanoseconds to years, including special
 * cases like quarters and weeks.
 * <p>
 * This date optimization not only improves performance but also ensures correctness in time-based grouping:
 * DATE_TRUNC properly handles timezone and daylight saving time (DST) transitions when using Period or Duration
 * intervals, while DATE_FORMAT does not account for these timezone-related considerations.
 */
public final class ReplaceAggregateNestedExpressionWithEval extends OptimizerRules.ParameterizedOptimizerRule<
    Aggregate,
    LogicalOptimizerContext> {

    public ReplaceAggregateNestedExpressionWithEval() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate, LogicalOptimizerContext ctx) {
        List<Alias> evalsBeforeAgg = new ArrayList<>();
        List<Alias> evalsAfterAgg = new ArrayList<>();
        Map<String, Attribute> evalNames = new HashMap<>();
        Map<GroupingFunction, Attribute> groupingAttributes = new HashMap<>();
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings());
        List<NamedExpression> newProjections = new ArrayList<>();
        Map<NamedExpression, Attribute> referenceAttributes = new HashMap<>();
        boolean groupingChanged = false;

        // start with the groupings since the aggs might reuse/reference them
        for (int i = 0, s = newGroupings.size(); i < s; i++) {
            Expression g = newGroupings.get(i);
            if (g instanceof Alias as) {
                Expression asChild = as.child();
                // for non-evaluable grouping functions, replace their nested expressions with attributes and extract the expression out
                // into an eval (added later below)
                if (asChild instanceof GroupingFunction.NonEvaluatableGroupingFunction gf) {
                    Expression newGroupingFunction = transformNonEvaluatableGroupingFunction(gf, evalsBeforeAgg);
                    if (newGroupingFunction != gf) {
                        groupingChanged = true;
                        newGroupings.set(i, as.replaceChild(newGroupingFunction));
                    }
                } else {
                    // Move the alias into an eval and replace it with its attribute.
                    groupingChanged = true;
                    var attr = as.toAttribute();
                    final Attribute finalAttribute = as.toAttribute();
                    if (asChild instanceof DateFormat df
                        && aggregate.aggregates()
                            .stream()
                            .anyMatch(
                                expression -> expression instanceof Alias alias && alias.references().contains(finalAttribute)
                            ) == false) {
                        // Extract the format pattern and field from DateFormat
                        Expression rawFormat = df.format();
                        AttributeMap<Expression> collectRefs = RuleUtils.foldableReferences(aggregate, ctx);

                        // Try to convert the format pattern to a minimal time interval
                        // This optimization attempts to simplify date formatting to DATE_TRUNC operations
                        if (collectRefs.resolve(rawFormat, rawFormat) instanceof Literal format) {
                            Literal interval = inferTruncIntervalFromFormat(BytesRefs.toString(format.value()), g.source());
                            // If we can optimize the format to use DATE_TRUNC
                            if (interval != null) {
                                // Create a new DateTrunc operation with the optimized interval
                                DateTrunc dateTrunc = new DateTrunc(df.source(), interval, df.field());
                                // Create a synthetic alias for the DateTrunc operation
                                var alias = new Alias(as.source(), as.name(), dateTrunc, null, true);
                                attr = alias.toAttribute();
                                // Replace the original DateFormat children with the new format and attribute
                                Expression expression = df.replaceChildren(List.of(format, attr));
                                // Create a new eval alias for the optimized expression
                                Alias newEval = as.replaceChild(expression);
                                evalsAfterAgg.add(newEval);
                                referenceAttributes.put(attr, newEval.toAttribute());
                                evalNames.put(as.name(), attr);
                                as = alias;
                            }
                        }
                    }

                    evalsBeforeAgg.add(as);
                    evalNames.put(as.name(), attr);
                    newGroupings.set(i, attr);
                    if (asChild instanceof GroupingFunction.EvaluatableGroupingFunction gf) {
                        groupingAttributes.put(gf, attr);
                    }
                }
            }
        }

        Holder<Boolean> aggsChanged = new Holder<>(false);
        List<? extends NamedExpression> aggs = aggregate.aggregates();
        List<NamedExpression> newAggs = new ArrayList<>(aggs.size());

        // map to track common expressions
        Map<Expression, Attribute> expToAttribute = new HashMap<>();
        for (Alias a : evalsBeforeAgg) {
            expToAttribute.put(a.child().canonical(), a.toAttribute());
        }

        int[] counter = new int[] { 0 };
        // for the aggs make sure to unwrap the agg function and check the existing groupings
        for (NamedExpression agg : aggs) {
            NamedExpression a = (NamedExpression) agg.transformDown(Alias.class, as -> {
                // if the child is a nested expression
                Expression child = as.child();

                if (child instanceof AggregateFunction af && skipOptimisingAgg(af)) {
                    return as;
                }

                // check if the alias matches any from grouping otherwise unwrap it
                Attribute ref = evalNames.get(as.name());
                if (ref != null) {
                    aggsChanged.set(true);
                    return ref;
                }

                // look for the aggregate function
                var replaced = child.transformUp(
                    AggregateFunction.class,
                    af -> transformAggregateFunction(af, expToAttribute, evalsBeforeAgg, counter, aggsChanged)
                );
                // replace any evaluatable grouping functions with their references pointing to the added synthetic eval
                replaced = replaced.transformDown(GroupingFunction.EvaluatableGroupingFunction.class, gf -> {
                    aggsChanged.set(true);
                    // should never return null, as it's verified.
                    // but even if broken, the transform will fail safely; otoh, returning `gf` will fail later due to incorrect plan.
                    return groupingAttributes.get(gf);
                });

                return as.replaceChild(replaced);
            });
            if (groupingChanged) {
                Attribute ref = null;

                if (agg instanceof ReferenceAttribute ra) {
                    // stats
                    ref = evalNames.get(ra.name());
                } else if (agg instanceof Alias alias) {
                    // inline stats
                    ref = evalNames.get(alias.toAttribute().name());
                }

                if (ref != null) {
                    aggsChanged.set(true);
                    newAggs.add(ref);
                    newProjections.add(referenceAttributes.getOrDefault(ref, ref.toAttribute()));
                    continue;
                }
            }
            newAggs.add(a);
            newProjections.add(a.toAttribute());
        }

        if (evalsBeforeAgg.size() > 0) {
            var groupings = groupingChanged ? newGroupings : aggregate.groupings();
            var aggregates = aggsChanged.get() ? newAggs : aggregate.aggregates();

            var newEval = new Eval(aggregate.source(), aggregate.child(), evalsBeforeAgg);
            aggregate = aggregate.with(newEval, groupings, aggregates);
        }
        if (evalsAfterAgg.size() > 0) {
            Eval eval = new Eval(aggregate.source(), aggregate, evalsAfterAgg);
            return new Project(aggregate.source(), eval, newProjections);
        }

        return aggregate;
    }

    private static Expression transformNonEvaluatableGroupingFunction(
        GroupingFunction.NonEvaluatableGroupingFunction gf,
        List<Alias> evals
    ) {
        int counter = 0;
        boolean childrenChanged = false;
        List<Expression> newChildren = new ArrayList<>(gf.children().size());

        for (Expression ex : gf.children()) {
            if (ex instanceof Attribute || ex instanceof MapExpression) {
                newChildren.add(ex);
            } else { // TODO: foldables shouldn't require eval'ing either
                var alias = new Alias(ex.source(), syntheticName(ex, gf, counter++), ex, null, true);
                evals.add(alias);
                newChildren.add(alias.toAttribute());
                childrenChanged = true;
            }
        }

        return childrenChanged ? gf.replaceChildren(newChildren) : gf;
    }

    private static boolean skipOptimisingAgg(AggregateFunction af) {
        // shortcut for the common scenario
        if (af.field() instanceof Attribute) {
            return true;
        }

        // do not replace nested aggregates
        Holder<Boolean> foundNestedAggs = new Holder<>(Boolean.FALSE);
        af.field().forEachDown(AggregateFunction.class, unused -> foundNestedAggs.set(Boolean.TRUE));
        return foundNestedAggs.get();
    }

    private static Expression transformAggregateFunction(
        AggregateFunction af,
        Map<Expression, Attribute> expToAttribute,
        List<Alias> evals,
        int[] counter,
        Holder<Boolean> aggsChanged
    ) {
        Expression result = af;

        Expression field = af.field();
        // if the field is a nested expression (not attribute or literal), replace it
        if (field instanceof Attribute == false && field.foldable() == false) {
            // create a new alias if one doesn't exist yet
            Attribute attr = expToAttribute.computeIfAbsent(field.canonical(), k -> {
                Alias newAlias = new Alias(k.source(), syntheticName(k, af, counter[0]++), k, null, true);
                evals.add(newAlias);
                return newAlias.toAttribute();
            });
            aggsChanged.set(true);
            // replace field with attribute
            List<Expression> newChildren = new ArrayList<>(af.children());
            newChildren.set(0, attr);
            result = af.replaceChildren(newChildren);
        }
        return result;
    }

    private static String syntheticName(Expression expression, Expression func, int counter) {
        return TemporaryNameUtils.temporaryName(expression, func, counter);
    }

    /**
     * Attempts to infer the minimal time interval that corresponds to a given date format.
     * <p>
     * The idea is to map {@code DATE_FORMAT} patterns to the smallest truncation unit
     * (year, month, day, hour, minute, second, millisecond, quarter) so that we can optimize
     * {@code DATE_FORMAT} by rewriting it as {@code DATE_TRUNC} when possible.
     * <p>
     * Limitations:
     * <ul>
     *   <li>The format must represent a continuous hierarchy of time units starting from "year".
     *       For example: {@code yyyy-MM-dd HH:mm:ss} is valid, but skipping "month" while using "day"
     *       is not.</li>
     *   <li>Quarter optimization is a special case: it's only applied when the format contains
     *       only year and quarter fields (e.g., "yyyy-Q").</li>
     *   <li>Patterns involving unsupported fields (e.g., ERA, DAY_OF_WEEK, AM/PM, nanoseconds)
     *       cannot be mapped to {@code DATE_TRUNC} and will return {@code null}.</li>
     *   <li>Nanosecond-level precision is not supported by {@code DATE_TRUNC}.</li>
     * </ul>
     *
     * @param format The date format pattern (e.g., "yyyy-MM-dd HH:mm:ss").
     * @param source The source of the query.
     * @return The corresponding minimal interval as a {@link Literal}, or {@code null} if the format
     *         cannot be represented as a truncation unit.
     * @see EsqlDataTypeConverter.INTERVALS for supported truncation units.
     */
    public static Literal inferTruncIntervalFromFormat(String format, Source source) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format, Locale.ROOT);
            String formatterAsString = formatter.toString();
            // Not supported to be converted to interval
            if (formatterAsString.contains("Text(" + ChronoField.ERA) // G
                || formatterAsString.contains("Value(" + JulianFields.MODIFIED_JULIAN_DAY) // g
                || formatterAsString.contains("Value(" + ChronoField.ALIGNED_WEEK_OF_MONTH) // f
                || formatterAsString.contains("Text(" + ChronoField.DAY_OF_WEEK) // E
                || formatterAsString.contains("Localized(" + ChronoField.DAY_OF_WEEK) // c/e
                || formatterAsString.contains("Text(" + ChronoField.AMPM_OF_DAY) // a
                || formatterAsString.contains("Value(" + ChronoField.HOUR_OF_AMPM) // K
                || formatterAsString.contains("Value(" + ChronoField.CLOCK_HOUR_OF_AMPM) // h
                // nanosecond interval not supported in DATE_TRUNC
                || formatterAsString.contains("Fraction(" + ChronoField.NANO_OF_SECOND) // S
                || formatterAsString.contains("Value(" + ChronoField.NANO_OF_SECOND) // n
                || formatterAsString.contains("Value(" + ChronoField.NANO_OF_DAY) // N
                // others
                || formatterAsString.contains("ZoneText(FULL)") // zzzz/vvvv
                || formatterAsString.contains("ZoneText(SHORT)") // z/v
                || formatterAsString.contains("ZoneId()") // VV
                || formatterAsString.contains("Offset(+HHMM,'+0000')") // Z/xx
                || formatterAsString.contains("LocalizedOffset(FULL)") // ZZZZ/OOOO
                || formatterAsString.contains("Offset(+HH:MM:ss,'Z')") // ZZZZZ/XXXXX
                || formatterAsString.contains("LocalizedOffset(SHORT)") // O
                || formatterAsString.contains("Offset(+HHmm,'Z')") // X
                || formatterAsString.contains("Offset(+HHMM,'Z')") // XX
                || formatterAsString.contains("Offset(+HH:MM,'Z')") // XXX
                || formatterAsString.contains("Offset(+HHMMss,'Z')") // XXXX
                || formatterAsString.contains("Offset(+HHmm,'+00')") // x
                || formatterAsString.contains("Offset(+HH:MM,'+00:00')") // xxx
                || formatterAsString.contains("Offset(+HHMMss,'+0000')") // xxxx
                || formatterAsString.contains("Offset(+HH:MM:ss,'+00:00')") // xxxxx
                || formatterAsString.contains("Localized(WeekOfMonth,1)") // W
                || formatterAsString.contains("Localized(WeekOfWeekBasedYear,1)") // w
                || formatterAsString.contains("Localized(WeekOfWeekBasedYear,2)") // ww
                || formatterAsString.contains("DayPeriod(SHORT)") // B
                || formatterAsString.contains("DayPeriod(FULL)") // BBBB
                || formatterAsString.contains("DayPeriod(NARROW)")) {// BBBBB
                return null;
            }

            // Define the hierarchy of time units, starting from year and gradually decreasing.
            // 0: year, 1: month, 2: day, 3: hour, 4: minute, 5: second, 6: millisecond
            boolean[] levels = new boolean[7];
            boolean hasQuarter = false;
            // year
            // y/u/Y
            if (formatterAsString.contains("Value(" + ChronoField.YEAR_OF_ERA)
                || formatterAsString.contains("Value(" + ChronoField.YEAR)
                || formatterAsString.contains("Localized(WeekBasedYear")) {
                levels[0] = true;
            }

            // quarter
            // Q/q
            if (formatterAsString.contains("Value(" + IsoFields.QUARTER_OF_YEAR)
                || formatterAsString.contains("Text(" + IsoFields.QUARTER_OF_YEAR)) {
                hasQuarter = true;
            }

            // month
            // M/L
            if (formatterAsString.contains("Value(" + ChronoField.MONTH_OF_YEAR)
                || formatterAsString.contains("Text(" + ChronoField.MONTH_OF_YEAR)) {
                levels[1] = true;
            }

            // day
            // d
            if (formatterAsString.contains("Value(" + ChronoField.DAY_OF_MONTH)) {
                levels[2] = true;
            }
            // D
            if (formatterAsString.contains("Value(" + ChronoField.DAY_OF_YEAR)) {
                levels[1] = true;
                levels[2] = true;
            }

            // hour
            // H/k
            if (formatterAsString.contains("Value(" + ChronoField.HOUR_OF_DAY)
                || formatterAsString.contains("Value(" + ChronoField.CLOCK_HOUR_OF_DAY)) {
                levels[3] = true;
            }

            // minute
            // m
            if (formatterAsString.contains("Value(" + ChronoField.MINUTE_OF_HOUR)) {
                levels[4] = true;
            }

            // second
            // s
            if (formatterAsString.contains("Value(" + ChronoField.SECOND_OF_MINUTE)) {
                levels[5] = true;
            }

            // millisecond
            // A
            if (formatterAsString.contains("Value(" + ChronoField.MILLI_OF_DAY)) {
                levels[3] = true;
                levels[4] = true;
                levels[5] = true;
                levels[6] = true;
            }

            // Check for continuity
            int lastLevel = -1;
            for (int i = 0; i < levels.length; i++) {
                if (levels[i]) {
                    if (lastLevel == i - 1) {
                        lastLevel = i;
                    } else {
                        return null; // Not continuous, return null
                    }
                }
            }

            // Special case: when format contains only year and quarter fields (e.g., "yyyy-Q"),
            // return a 3-month period to represent quarterly truncation at the year level
            if (lastLevel == 0 && hasQuarter) {
                return new Literal(source, Period.ofMonths(3), DataType.DATE_PERIOD);
            }

            // Return the smallest time unit.
            switch (lastLevel) {
                case 0:
                    return new Literal(source, Period.ofYears(1), DataType.DATE_PERIOD);
                case 1:
                    return new Literal(source, Period.ofMonths(1), DataType.DATE_PERIOD);
                case 2:
                    return new Literal(source, Period.ofDays(1), DataType.DATE_PERIOD);
                case 3:
                    return new Literal(source, ChronoUnit.HOURS.getDuration(), DataType.TIME_DURATION);
                case 4:
                    return new Literal(source, ChronoUnit.MINUTES.getDuration(), DataType.TIME_DURATION);
                case 5:
                    return new Literal(source, ChronoUnit.SECONDS.getDuration(), DataType.TIME_DURATION);
                case 6:
                    return new Literal(source, ChronoUnit.MILLIS.getDuration(), DataType.TIME_DURATION);
            }
        } catch (IllegalArgumentException ignored) {}
        return null;
    }
}
