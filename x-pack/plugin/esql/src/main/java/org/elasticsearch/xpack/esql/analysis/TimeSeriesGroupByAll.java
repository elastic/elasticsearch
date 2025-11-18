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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
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
        // Flag to check if we should apply this rule.
        boolean hasTopLevelOverTimeAggs = false;
        // the new `Value(dimension)` aggregation functions we intend to add to the query, along with the translated over time aggs
        List<NamedExpression> newAggregateFunctions = new ArrayList<>();
        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                if (af instanceof TimeSeriesAggregateFunction tsAgg) {
                    hasTopLevelOverTimeAggs = true;
                    newAggregateFunctions.add(new Alias(alias.source(), alias.name(), new Values(tsAgg.source(), tsAgg)));
                } else {
                    // Preserve non-time-series aggregates
                    newAggregateFunctions.add(agg);
                }
            } else {
                // Preserve non-aggregate expressions (like grouping keys that are already in aggregates)
                newAggregateFunctions.add(agg);
            }
        }
        if (hasTopLevelOverTimeAggs == false) {
            return aggregate;
        }

        List<Expression> groupings = new ArrayList<>();

        Holder<Attribute> tsid = new Holder<>();
        getTsFields(aggregate, tsid);

        LogicalPlan newChild = aggregate.child().transformUp(EsRelation.class, r -> {
            List<Attribute> attributesToAdd = new ArrayList<>();

            boolean tsidFoundInOutput = r.output().stream().anyMatch(attr -> attr.name().equalsIgnoreCase(MetadataAttribute.TSID_FIELD));
            if (tsidFoundInOutput == false) {
                attributesToAdd.add(tsid.get());
            }

            if (attributesToAdd.isEmpty()) {
                return r;
            }

            return new EsRelation(
                r.source(),
                r.indexPattern(),
                r.indexMode(),
                r.indexNameWithModes(),
                CollectionUtils.combine(r.output(), attributesToAdd)
            );
        });

        // Group the new aggregations by tsid. This is equivalent to grouping by all dimensions.
        groupings.add(tsid.get());

        // We add the tsid to the aggregates list because we want to include it in the output of the first pass in
        // TranslateTimeSeriesAggregate
        newAggregateFunctions.add(tsid.get());

        // Add the user defined grouping
        groupings.addAll(aggregate.groupings());

        // Add user-defined groupings to aggregates if they're attributes (expressions like TBUCKET are handled later)
        for (Expression userGrouping : aggregate.groupings()) {
            if (userGrouping instanceof Attribute attr) {
                // Check if it's already in aggregates (might be if it was in SELECT)
                boolean alreadyInAggregates = newAggregateFunctions.stream().anyMatch(agg -> {
                    Attribute aggAttr = Expressions.attribute(agg);
                    return aggAttr != null && aggAttr.id().equals(attr.id());
                });
                if (alreadyInAggregates == false) {
                    newAggregateFunctions.add(attr);
                }
            }
        }

        return new TimeSeriesAggregate(aggregate.source(), newChild, groupings, newAggregateFunctions, null);
    }

    private static void getTsFields(TimeSeriesAggregate aggregate, Holder<Attribute> tsidHolder) {
        aggregate.forEachDown(
            EsRelation.class,
            r -> r.output()
                .stream()
                .filter(attr -> attr.name().equalsIgnoreCase(MetadataAttribute.TSID_FIELD))
                .findFirst()
                .ifPresentOrElse(
                    tsidHolder::set,
                    () -> tsidHolder.set(
                        new MetadataAttribute(aggregate.source(), MetadataAttribute.TSID_FIELD, DataType.TSID_DATA_TYPE, false)
                    )
                )
        );
    }
}
