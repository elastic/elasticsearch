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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
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
import java.util.Map;

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
        AggregateFunction lastTSAggFunction = null;
        AggregateFunction lastNonTSAggFunction = null;

        List<NamedExpression> newAggregateFunctions = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                if (af instanceof TimeSeriesAggregateFunction tsAgg) {
                    newAggregateFunctions.add(new Alias(alias.source(), alias.name(), new Values(tsAgg.source(), tsAgg)));
                    lastTSAggFunction = tsAgg;
                } else {
                    newAggregateFunctions.add(agg);
                    lastNonTSAggFunction = af;
                }
            } else {
                newAggregateFunctions.add(agg);
            }
        }
        if (lastTSAggFunction == null) {
            return aggregate;
        }

        if (lastNonTSAggFunction != null) {
            throw new IllegalArgumentException(
                "Cannot mix time-series aggregate ["
                    + lastTSAggFunction.sourceText()
                    + "] and regular aggregate ["
                    + lastNonTSAggFunction.sourceText()
                    + "] in the same TimeSeriesAggregate."

            );
        }

        var timeSeries = new FieldAttribute(
            aggregate.source(),
            null,
            null,
            MetadataAttribute.TIMESERIES,
            new EsField(MetadataAttribute.TIMESERIES, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
        );
        List<Expression> groupings = new ArrayList<>();
        groupings.add(timeSeries);

        for (Expression grouping : aggregate.groupings()) {
            if (Functions.isGrouping(Alias.unwrap(grouping)) == false) {
                throw new IllegalArgumentException(
                    "Cannot mix time-series aggregate and grouping attributes. Found [" + grouping.sourceText() + "]."
                );
            }
            groupings.add(grouping);
        }

        TimeSeriesAggregate newStats = new TimeSeriesAggregate(
            aggregate.source(),
            aggregate.child(),
            groupings,
            newAggregateFunctions,
            null
        );
        // insert the time_series
        return newStats.transformDown(EsRelation.class, r -> {
            ArrayList<Attribute> attributes = new ArrayList<>(r.output());
            attributes.add(timeSeries);
            return new EsRelation(
                r.source(),
                r.indexPattern(),
                r.indexMode(),
                r.originalIndices(),
                r.concreteIndices(),
                r.indexNameWithModes(),
                attributes
            );
        });
    }
}
