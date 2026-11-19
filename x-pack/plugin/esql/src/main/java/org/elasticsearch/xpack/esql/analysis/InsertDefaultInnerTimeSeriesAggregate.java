/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.PentaFunction;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DefaultTimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Earliest;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Latest;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MaxOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MinOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
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
 *
 * LAST(field, @timestamp) ->
 * LAST(LAST_OVER_TIME(field), MAX_OVER_TIME(@timestamp))
 *
 * FIRST(field, @timestamp) ->
 * FIRST(FIRST_OVER_TIME(field), MIN_OVER_TIME(@timestamp))
 *
 * LATEST(field) ->
 * LATEST(LAST_OVER_TIME(field), MAX_OVER_TIME(@timestamp))
 *
 * EARLIEST(field) ->
 * EARLIEST(FIRST_OVER_TIME(field), MIN_OVER_TIME(@timestamp))
 * </pre>
 */
public class InsertDefaultInnerTimeSeriesAggregate extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        return logicalPlan.transformUp(node -> node instanceof TimeSeriesAggregate, this::rule);
    }

    public LogicalPlan rule(TimeSeriesAggregate aggregate) {
        // The TranslatePromqlToEsqlPlan rule handles wrapping the DefaultTimeSeriesAggregate for queries
        // originating from the PromqlCommand. Additionally, certain PromQL queries should *not* be wrapped
        // with a LastOverTime (such as queries with scalars and ones that use the time() function), so
        // we skip this rule for PromQL queries altogether.
        if (aggregate.origin() == TimeSeriesAggregate.Origin.PROMQL_COMMAND) {
            return aggregate;
        }
        if (aggregate.timestamp() == null || aggregate.timestamp().resolved() == false) {
            /*
             * Timestamp was dropped before the function. Leave the aggregate untouched so the Verifier can
             * report a proper resolution failure
             */
            return aggregate;
        }
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
                // Last/First have a sort parameter that must also be wrapped so TranslateTimeSeriesAggregate
                // handles it during the two-phase split. Field and sort use correlated over-time functions
                // to ensure they pick from the same document within a _tsid group.
                // Latest/Earliest sort by the implicit @timestamp, so they need the same wrapping. The generic
                // branch below would wrap both their arguments in LAST_OVER_TIME, which makes EARLIEST return the
                // last value of the series whose last sample is the earliest.
                case Last last when last.sort() instanceof TimeSeriesAggregateFunction == false -> wrapLast(
                    last,
                    last.field(),
                    last.sort(),
                    timestamp,
                    changed
                );
                case Latest latest when latest.timestamp() instanceof TimeSeriesAggregateFunction == false -> wrapLast(
                    latest,
                    latest.field(),
                    latest.timestamp(),
                    timestamp,
                    changed
                );
                case First first when first.sort() instanceof TimeSeriesAggregateFunction == false -> wrapFirst(
                    first,
                    first.field(),
                    first.sort(),
                    timestamp,
                    changed
                );
                case Earliest earliest when earliest.timestamp() instanceof TimeSeriesAggregateFunction == false -> wrapFirst(
                    earliest,
                    earliest.field(),
                    earliest.timestamp(),
                    timestamp,
                    changed
                );
                case AggregateFunction af -> {
                    List<Expression> newFields = new ArrayList<>(af.fields());
                    newFields.replaceAll(field -> addDefaultInnerAggs(field, timestamp, changed));
                    yield af.withFields(newFields);
                }
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

    /**
     * Wraps the arguments of {@link Last} or {@link Latest} so the second pass picks the value of the series with the latest sample.
     * The field uses {@link LastOverTime} for every type, histograms included, so the result is a real sample like outside of
     * {@code TS}. The default inner aggregate would merge all histograms of a series instead.
     */
    private static Expression wrapLast(
        AggregateFunction agg,
        Expression field,
        Expression sort,
        Expression timestamp,
        Holder<Boolean> changed
    ) {
        return wrapSortedAgg(
            agg,
            sort,
            timestamp,
            changed,
            new LastOverTime(field.source(), field, timestamp, Literal.TRUE, AggregateFunction.NO_WINDOW),
            MaxOverTime::new,
            LastOverTime::new
        );
    }

    /**
     * Wraps the arguments of {@link First} or {@link Earliest} so the second pass picks the value of the series with the earliest
     * sample. Like {@link #wrapLast}, it keeps a real sample for every type, histograms included.
     */
    private static Expression wrapFirst(
        AggregateFunction agg,
        Expression field,
        Expression sort,
        Expression timestamp,
        Holder<Boolean> changed
    ) {
        return wrapSortedAgg(
            agg,
            sort,
            timestamp,
            changed,
            new FirstOverTime(field.source(), field, timestamp, Literal.TRUE, AggregateFunction.NO_WINDOW),
            MinOverTime::new,
            FirstOverTime::new
        );
    }

    /**
     * Wraps field and sort of {@link Last}/{@link First} with correlated over-time functions so both pick from
     * the same document within a _tsid group. When sort is {@code @timestamp}, uses {@code onTimestampSort}
     * (MaxOverTime/MinOverTime). Otherwise uses {@code onOtherSort} (LastOverTime/FirstOverTime).
     */
    private static Expression wrapSortedAgg(
        AggregateFunction agg,
        Expression sort,
        Expression timestamp,
        Holder<Boolean> changed,
        Expression newField,
        PentaFunction<Source, Expression, Expression, Expression, Expression, Expression> onTimestampSort,
        PentaFunction<Source, Expression, Expression, Expression, Expression, Expression> onOtherSort
    ) {
        changed.set(true);
        var newSort = sort.semanticEquals(timestamp)
            ? onTimestampSort.apply(sort.source(), sort, timestamp, Literal.TRUE, AggregateFunction.NO_WINDOW)
            : onOtherSort.apply(sort.source(), sort, timestamp, Literal.TRUE, AggregateFunction.NO_WINDOW);
        return agg.withFields(List.of(newField, newSort));
    }
}
