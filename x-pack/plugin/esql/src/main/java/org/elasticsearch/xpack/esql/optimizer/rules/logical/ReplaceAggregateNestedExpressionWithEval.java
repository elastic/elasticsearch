/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 * {@code INLINESTATS SUM(a + 1) BY x % 2}
 * becomes
 * {@code EVAL `a + 1` = a + 1, `x % 2` = x % 2 | INLINESTATS SUM(`a+1`_ref) BY `x % 2`_ref}
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
public final class ReplaceAggregateNestedExpressionWithEval extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        List<Alias> evals = new ArrayList<>();
        Map<String, Attribute> evalNames = new HashMap<>();
        Map<GroupingFunction, Attribute> groupingAttributes = new HashMap<>();
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings());
        List<NamedExpression> newProjections = new ArrayList<>();

        boolean groupingChanged = false;

        List<Alias> newEvals = new ArrayList<>();
        int[] counter = new int[] { 0 };

        // start with the groupings since the aggs might reuse/reference them
        for (int i = 0, s = newGroupings.size(); i < s; i++) {
            Expression g = newGroupings.get(i);
            if (g instanceof Alias as) {
                Expression asChild = as.child();
                // for non-evaluable grouping functions, replace their nested expressions with attributes and extract the expression out
                // into an eval (added later below)
                if (asChild instanceof GroupingFunction.NonEvaluatableGroupingFunction gf) {
                    Expression newGroupingFunction = transformNonEvaluatableGroupingFunction(gf, evals);
                    if (newGroupingFunction != gf) {
                        groupingChanged = true;
                        newGroupings.set(i, as.replaceChild(newGroupingFunction));
                    }
                } else {
                    // Move the alias into an eval and replace it with its attribute.
                    groupingChanged = true;
                    var attr = as.toAttribute();
                    if (asChild instanceof DateFormat df) {
                        // Extract the format pattern and field from DateFormat
                        Literal format = (Literal) df.children().getFirst();
                        Expression field = df.children().get(1);

                        // Try to convert the format pattern to a minimal time interval
                        // This optimization attempts to simplify date formatting to DATE_TRUNC operations
                        Literal interval = formatToMinimalInterval((String) format.value(), g.source());
                        // If we can optimize the format to use DATE_TRUNC
                        if (interval != null) {
                            // Create a new DateTrunc operation with the optimized interval
                            DateTrunc dateTrunc = new DateTrunc(df.source(), interval, field);
                            // Create a synthetic alias for the DateTrunc operation
                            var alias = new Alias(as.source(), syntheticName(dateTrunc, as, counter[0]++), dateTrunc, null, true);
                            attr = alias.toAttribute();
                            // Replace the original DateFormat children with the new format and attribute
                            Expression expression = df.replaceChildren(List.of(format, attr));
                            // Create a new eval alias for the optimized expression
                            Alias newEval = as.replaceChild(expression);
                            newEvals.add(newEval);
                            newProjections.add(newEval.toAttribute());
                            evalNames.put(as.name(), attr);
                            as = alias;
                        }
                    }

                    evals.add(as);
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
        for (Alias a : evals) {
            expToAttribute.put(a.child().canonical(), a.toAttribute());
        }

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
                    af -> transformAggregateFunction(af, expToAttribute, evals, counter, aggsChanged)
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
            if (groupingChanged && agg instanceof ReferenceAttribute ra) {
                Attribute ref = evalNames.get(ra.name());
                if (ref != null) {
                    aggsChanged.set(true);
                    newAggs.add(ref);
                }
            } else {
                newAggs.add(a);
                newProjections.add(a.toAttribute());
            }
        }

        if (evals.size() > 0) {
            var groupings = groupingChanged ? newGroupings : aggregate.groupings();
            var aggregates = aggsChanged.get() ? newAggs : aggregate.aggregates();

            var newEval = new Eval(aggregate.source(), aggregate.child(), evals);
            aggregate = aggregate.with(newEval, groupings, aggregates);
        }
        if (newEvals.size() > 0) {
            Eval eval = new Eval(aggregate.source(), aggregate, newEvals);
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
            if (ex instanceof Attribute == false) { // TODO: foldables shouldn't require eval'ing either
                var alias = new Alias(ex.source(), syntheticName(ex, gf, counter++), ex, null, true);
                evals.add(alias);
                newChildren.add(alias.toAttribute());
                childrenChanged = true;
            } else {
                newChildren.add(ex);
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
