/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This rule implements the "group by all" logic for time series aggregations.  It is intended to work in conjunction with
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate}, and should be run before that
 * rule.  This rule adds output columns corresponding to the dimensions on the indices involved in the query, as discovered
 * by the {@link org.elasticsearch.xpack.esql.session.IndexResolver}. Despite the name, this does not acutally group on the
 * dimension values, for efficiency reasons.
 * <p>
 * This rule will operate on "bare" over time aggregations,
 */
public class TimeSeriesGroupByAll extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        return logicalPlan.transformUp(node -> node instanceof TimeSeriesAggregate, this::rule);
    }

    public LogicalPlan rule(TimeSeriesAggregate aggregate) {
        // Flag to check if we should apply this rule.
        boolean hasTopLevelOverTimeAggs = false;
        Holder<Boolean> hasRateAggregates = new Holder<>(Boolean.FALSE);
        // the new `Value(dimension)` aggregation functions we intend to add to the query, along with the translated over time aggs
        List<NamedExpression> newAggregateFunctions = new ArrayList<>();
        for (NamedExpression agg : aggregate.aggregates()) {
            // We assume that all the aggregate functions in aggregates are wrapped as Aliases. I don't know why
            // this should be the case, but it is ubiquitous throughout the planner.
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                if (af instanceof TimeSeriesAggregateFunction tsAgg) {
                    hasTopLevelOverTimeAggs = true;
                    newAggregateFunctions.add(new Alias(alias.source(), alias.name(), tsAgg.perTimeSeriesAggregation()));
                }
                af.forEachDown(TimeSeriesAggregateFunction.class, tsAgg -> {
                    if (tsAgg instanceof Rate) {
                        hasRateAggregates.set(Boolean.TRUE);
                    }
                });
                // TODO: Deal with mixed top level and wrapped aggs case
            }
        }
        if (hasTopLevelOverTimeAggs == false) {
            // If there are no top level time series aggregations, there's no need for this rule to apply
            return aggregate;
        }

        // Grouping parameters for the new aggregation node.
        List<Expression> groupings = new ArrayList<>();

        Holder<Attribute> tsid = new Holder<>();
        Holder<Attribute> timestamp = new Holder<>();
        Set<Attribute> dimensions = new HashSet<>();
        getTsFields(aggregate, tsid, timestamp, dimensions);
        if (tsid.get() == null) {
            tsid.set(new MetadataAttribute(aggregate.source(), MetadataAttribute.TSID_FIELD, DataType.KEYWORD, false));
        }
        if (timestamp.get() == null) {
            throw new IllegalArgumentException("_tsid or @timestamp field are missing from the time-series source");
        }

        // NOCOMMIT - this is redundant with behavior in TranslateTSA
        // NOCOMMIT - This behavior is tangential to this rule; maybe we should break it out into a separate rule?
        // Add the _tsid to the EsRelation Leaf, if it's not there already
        LogicalPlan newChild = aggregate.child().transformUp(EsRelation.class, r -> {
            IndexMode indexMode = hasRateAggregates.get() ? r.indexMode() : IndexMode.STANDARD;
            if (r.output().contains(tsid.get()) == false) {
                return new EsRelation(
                    r.source(),
                    r.indexPattern(),
                    indexMode,
                    r.indexNameWithModes(),
                    CollectionUtils.combine(r.output(), tsid.get())
                );
            } else {
                return new EsRelation(r.source(), r.indexPattern(), indexMode, r.indexNameWithModes(), r.output());
            }
        });
        // Group the new aggregations by tsid. This is equivalent to grouping by all dimensions.
        groupings.add(tsid.get());
        // Add the time bucket grouping for the new agg
        // NOCOMMIT - validation rule for groupings?
        groupings.addAll(aggregate.groupings());
        for (Attribute dimension : dimensions) {
            // We add the dimensions as Values aggs here as an optimization. Grouping by the _tsid should already ensure
            // one row per unique combination of dimensions, and collecting those values in a Values aggregation is less
            // computation than hashing them for a grouping operation.
            // NOCOMMIT - This is also redundant. TranslateTSA already turns all the inner groupings into values aggs. Just do this there.
            newAggregateFunctions.add(new Alias(dimension.source(), dimension.name(), new Values(dimension.source(), dimension)));
        }
        TimeSeriesAggregate newAggregate = new TimeSeriesAggregate(
            aggregate.source(),
            newChild,
            groupings,
            newAggregateFunctions,
            null,
            true
        );
        // NOCOMMIT - We should drop the _tsid here
        // return new Drop(aggregate.source(), newAggregate, List.of(tsid.get()));
        return newAggregate;
    }

    private static void getTsFields(
        TimeSeriesAggregate aggregate,
        Holder<Attribute> tsid,
        Holder<Attribute> timestamp,
        Set<Attribute> dimensions
    ) {
        aggregate.forEachDown(EsRelation.class, r -> {
            for (Attribute attr : r.output()) {
                if (attr.name().equals(MetadataAttribute.TSID_FIELD)) {
                    tsid.set(attr);
                }
                if (attr.name().equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                    timestamp.set(attr);
                }
                if (attr.isDimension()) {
                    dimensions.add(attr);
                }
            }
        });
    }
}
