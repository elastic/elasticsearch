/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DefaultTimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;

/**
 * Ensures that {@link TypedAttribute}s used inside a {@link TimeSeriesAggregate} are wrapped in a
 * {@link TimeSeriesAggregateFunction}.
 * Examples:
 * <pre>
 * foo + bar ->
 * LAST_OVER_TIME(foo) + LAST_OVER_TIME(bar)
 *
 * SUM(foo + LAST_OVER_TIME(bar)) ->
 * SUM(LAST_OVER_TIME(foo) + LAST_OVER_TIME(bar))
 *
 * foo / 2 + bar * 2 ->
 * LAST_OVER_TIME(foo) / 2 + LAST_OVER_TIME(bar) * 2
 * </pre>
 */
public class InsertDefaultInnerTimeSeriesAggregate extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        return logicalPlan.transformUp(node -> node instanceof TimeSeriesAggregate, this::rule);
    }

    public LogicalPlan rule(TimeSeriesAggregate aggregate) {
        Holder<Boolean> changed = new Holder<>(false);
        List<NamedExpression> newAggregates = aggregate.aggregates().stream().map(agg -> {
            // The actual aggregation functions in aggregates will be aliases, while the groupings in aggregates will be Attributes
            if (agg instanceof Alias alias) {
                return alias.replaceChild(addDefaultInnerAggs(alias.child(), aggregate.timestamp(), changed));
            } else {
                return agg;
            }
        }).toList();
        if (changed.get() == false) {
            return aggregate;
        }
        return aggregate.with(aggregate.groupings(), newAggregates);
    }

    private static Expression addDefaultInnerAggs(Expression expression, Expression timestamp, Holder<Boolean> changed) {
        return expression.transformDownSkipBranch((expr, skipBranch) -> {
            // the default is to end the traversal here as we're either done or a recursive call will handle it
            skipBranch.set(true);
            return switch (expr) {
                // this is already a time series aggregation, no need to go deeper
                case TimeSeriesAggregateFunction ts -> ts;
                // only transform field, not all children (such as inline filter or window)
                case AggregateFunction af -> af.withField(addDefaultInnerAggs(af.field(), timestamp, changed));
                // avoid modifying filter conditions, just the delegate
                case FilteredExpression filtered -> filtered.withDelegate(addDefaultInnerAggs(filtered.delegate(), timestamp, changed));
                case ConvertFunction convert when expr.allMatch(e -> e instanceof ConvertFunction || e instanceof TypedAttribute) -> {
                    changed.set(true);
                    yield new DefaultTimeSeriesAggregateFunction(expr, timestamp);
                }

                // if we reach a TypedAttribute, it hasn't been wrapped in a TimeSeriesAggregateFunction yet
                // (otherwise the traversal would have stopped earlier)
                // so we wrap it with a default one
                case TypedAttribute ta -> {
                    changed.set(true);
                    yield new DefaultTimeSeriesAggregateFunction(ta, timestamp);
                }
                default -> {
                    // for other expressions, continue the traversal
                    skipBranch.set(false);
                    yield expr;
                }
            };
        });
    }
}
