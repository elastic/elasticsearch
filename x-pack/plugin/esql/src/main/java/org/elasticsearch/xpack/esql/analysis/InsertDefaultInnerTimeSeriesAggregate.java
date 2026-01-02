/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMergeOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
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
        Holder<Attribute> timestamp = new Holder<>();
        aggregate.forEachDown(EsRelation.class, (EsRelation r) -> {
            for (Attribute attr : r.output()) {
                if (attr.name().equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                    timestamp.set(attr);
                }
            }
        });
        if (timestamp.get() == null) {
            throw new IllegalArgumentException("@timestamp field is missing from the time-series source");
        }
        List<NamedExpression> newAggregates = aggregate.aggregates().stream().map(agg -> {
            if (agg instanceof Alias alias) {
                return alias.replaceChild(addDefaultInnerAggs(alias.child(), timestamp.get()));
            } else {
                return agg;
            }
        }).toList();
        return aggregate.with(aggregate.groupings(), newAggregates);
    }

    private static Expression addDefaultInnerAggs(Expression expression, Attribute timestamp) {
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
            case TypedAttribute ta -> {
                skipBranch.set(true);
                yield createDefaultInnerAggregation(ta, timestamp);
            }
            default -> expr;
        });
    }

    private static TimeSeriesAggregateFunction createDefaultInnerAggregation(TypedAttribute attr, Attribute timestamp) {
        if (attr.dataType() == DataType.EXPONENTIAL_HISTOGRAM || attr.dataType() == DataType.TDIGEST) {
            return new HistogramMergeOverTime(
                wrapSource(HistogramMergeOverTime.class, attr),
                attr,
                Literal.TRUE,
                AggregateFunction.NO_WINDOW
            );
        } else {
            return new LastOverTime(wrapSource(LastOverTime.class, attr), attr, AggregateFunction.NO_WINDOW, timestamp);
        }
    }

    private static Source wrapSource(Class<? extends TimeSeriesAggregateFunction> timeSeriesFunction, TypedAttribute attr) {
        String functionNameSnakeCase = timeSeriesFunction.getSimpleName().replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase();
        Source attrSource = attr.source();
        return new Source(attrSource.source(), functionNameSnakeCase + "(" + attrSource.text() + ")");
    }
}
