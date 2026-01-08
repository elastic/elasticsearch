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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMergeOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;

/**
 * Ensures that {@link TypedAttribute}s used inside a {@link TimeSeriesAggregate} are wrapped in a
 * {@link TimeSeriesAggregateFunction}.
 * For example, it rewrites
 * <pre>
 * SUM(foo + LAST_OVER_TIME(bar))
 * </pre>
 * into
 * <pre>
 * SUM(LAST_OVER_TIME(foo) + LAST_OVER_TIME(bar))
 * </pre>
 */
public class InsertDefaultInnerTimeSeriesAggregate extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        return logicalPlan.transformUp(node -> node instanceof TimeSeriesAggregate, this::rule);
    }

    public LogicalPlan rule(TimeSeriesAggregate aggregate) {
        List<NamedExpression> newAggregates = aggregate.aggregates().stream().map(agg -> {
            if (agg instanceof Alias alias) {
                return alias.replaceChild(addDefaultInnerAggs(alias.child(), aggregate.timestamp()));
            } else {
                return agg;
            }
        }).toList();
        return aggregate.with(aggregate.groupings(), newAggregates);
    }

    private static Expression addDefaultInnerAggs(Expression expression, Expression timestamp) {
        return expression.transformDownSkipBranch((expr, skipBranch) -> switch (expr) {
            case TimeSeriesAggregateFunction ts -> {
                // if we find a TimeSeriesAggregateFunction, we can skip the branch
                skipBranch.set(true);
                yield ts;
            }
            // only transform field, not all children
            case AggregateFunction af -> {
                skipBranch.set(true);
                yield af.withField(addDefaultInnerAggs(af.field(), timestamp));
            }
            case FilteredExpression filtered -> {
                // avoid modifying filter conditions
                skipBranch.set(true);
                yield filtered.withDelegate(addDefaultInnerAggs(filtered.delegate(), timestamp));
            }
            case FieldAttribute field -> {
                skipBranch.set(true);
                yield createDefaultInnerAggregation(field, timestamp);
            }
            default -> expr;
        });
    }

    private static TimeSeriesAggregateFunction createDefaultInnerAggregation(TypedAttribute attr, Expression timestamp) {
        if (attr.dataType() == DataType.EXPONENTIAL_HISTOGRAM || attr.dataType() == DataType.TDIGEST) {
            return new HistogramMergeOverTime(attr.source(), attr, Literal.TRUE, AggregateFunction.NO_WINDOW);
        } else {
            return new LastOverTime(attr.source(), attr, AggregateFunction.NO_WINDOW, timestamp);
        }
    }
}
