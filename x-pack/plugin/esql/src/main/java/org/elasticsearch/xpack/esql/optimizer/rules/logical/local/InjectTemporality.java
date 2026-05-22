/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DeltaOnlyHistogramMergeOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMerge;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

public final class InjectTemporality extends OptimizerRules.OptimizerRule<TimeSeriesAggregate> {

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate tsAgg) {
        TimeSeriesAggregate transformed;
        transformed = replaceMergeWithMergeOverTime(tsAgg);
        transformed = (TimeSeriesAggregate) transformed.transformExpressionsOnly(AggregateFunction.class, agg -> {
            if (agg instanceof TemporalityAware temporalityAware && temporalityAware.temporality() == null) {
                return temporalityAware.withTemporality(new TemporalityAttribute(agg.source()));
            }
            return agg;
        });
        return injectTemporalityAttributesIntoEsRelation(transformed);
    }

    // Replaces HistogramMerge in timeseries aggregations with merge_over_time which loads the temporality column
    // and ignores non-delta temporalities with a warning
    // This is done in local planning for backwards compatibility: Older versions would use HistogramMerge as the
    // per-time series aggregation. As we might be running in a CCS scenario where the older node still does this
    // we have to fix it in local planning here
    private TimeSeriesAggregate replaceMergeWithMergeOverTime(TimeSeriesAggregate tsAgg) {
        return (TimeSeriesAggregate) tsAgg.transformExpressionsOnly(
            HistogramMerge.class,
            agg -> new DeltaOnlyHistogramMergeOverTime(agg.source(), agg.field(), agg.filter(), agg.window())
        );
    }

    private static TimeSeriesAggregate injectTemporalityAttributesIntoEsRelation(TimeSeriesAggregate tsAgg) {
        // Normalize all temporality references to a single NameId so one EsRelation output can satisfy all uses.
        Holder<NameId> canonicalTemporalityId = new Holder<>();
        TimeSeriesAggregate normalized = (TimeSeriesAggregate) tsAgg.transformExpressionsOnly(TemporalityAttribute.class, t -> {
            if (canonicalTemporalityId.get() == null) {
                canonicalTemporalityId.set(t.id());
                return t;
            } else {
                return t.withId(canonicalTemporalityId.get());
            }
        });

        if (canonicalTemporalityId.get() == null) {
            // no temporality references found, nothing to do
            return tsAgg;
        }

        // Inject a single, canonical temporality attribute into the first EsRelation
        return (TimeSeriesAggregate) normalized.transformDown(plan -> {
            if (plan instanceof EsRelation relation) {
                return relation.withAdditionalAttribute(new TemporalityAttribute(Source.EMPTY).withId(canonicalTemporalityId.get()));
            }
            return plan;
        });
    }

}
