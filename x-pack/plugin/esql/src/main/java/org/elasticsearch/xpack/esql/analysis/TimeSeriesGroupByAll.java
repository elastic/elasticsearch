/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.Functions;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule implements the "group by all" logic for time series aggregations.  It is intended to work in conjunction with
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate}, and should be run before that
 * rule.  This rule adds output columns corresponding to the dimensions on the indices involved in the query, as discovered
 * by the {@link org.elasticsearch.xpack.esql.session.IndexResolver}. Despite the name, this does not actually group on the
 * dimension values, for efficiency reasons.
 * <p>
 * This rule will operate on "bare" over time aggregations.
 */
public class TimeSeriesGroupByAll extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        return logicalPlan.transformUp(node -> node instanceof TimeSeriesAggregate, this::rule);
    }

    public LogicalPlan rule(TimeSeriesAggregate aggregate) {
        Holder<Expression> lastTSAggFunction = new Holder<>();
        Holder<Expression> lastNonTSAggFunction = new Holder<>();

        List<NamedExpression> newAggregateFunctions = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression agg : aggregate.aggregates()) {
            Holder<NamedExpression> newAggHolder = new Holder<>(agg);
            if (agg instanceof Alias alias) {
                alias.forEachDownMayReturnEarly((lp, exit) -> {
                    if (lp instanceof TimeSeriesAggregateFunction) {
                        // we've encountered a time-series aggregation function first, so we'll enable the "group by all" logic
                        newAggHolder.set(
                            new Alias(alias.source(), alias.name(), new Values(alias.child().source(), alias.child()), alias.id())
                        );
                        lastTSAggFunction.set(agg);
                        exit.set(true);
                    } else if (lp instanceof AggregateFunction) {
                        lastNonTSAggFunction.set(agg);
                        exit.set(true);
                    }
                });
            }
            newAggregateFunctions.add(newAggHolder.get());
        }
        if (lastTSAggFunction.get() == null) {
            return aggregate;
        }

        if (lastNonTSAggFunction.get() != null) {
            throw new IllegalArgumentException(
                "Cannot mix time-series aggregate ["
                    + lastTSAggFunction.get().sourceText()
                    + "] and regular aggregate ["
                    + lastNonTSAggFunction.get().sourceText()
                    + "] in the same TimeSeriesAggregate."

            );
        }

        var timeSeries = FieldAttribute.timeSeriesAttribute(aggregate.source());
        List<Expression> groupings = new ArrayList<>();
        groupings.add(timeSeries);

        for (Expression grouping : aggregate.groupings()) {
            if (Functions.isGrouping(Alias.unwrap(grouping)) == false) {
                throw new IllegalArgumentException(
                    "Only grouping functions are supported (e.g. tbucket) when the time series aggregation function ["
                        + lastTSAggFunction.get().sourceText()
                        + "] is not wrapped with another aggregation function. Found ["
                        + grouping.sourceText()
                        + "]."
                );
            }
            groupings.add(grouping);
        }

        TimeSeriesAggregate newStats = new TimeSeriesAggregate(
            aggregate.source(),
            aggregate.child(),
            groupings,
            newAggregateFunctions,
            null,
            aggregate.timestamp()
        );
        // insert the time_series
        return newStats.transformDown(EsRelation.class, r -> r.withAdditionalAttribute(timeSeries));
    }
}
