/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DeltaOnlyHistogramMergeOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMerge;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.FilterUnsupportedTemporality;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.List;

public final class InjectTemporality extends OptimizerRules.OptimizerRule<TimeSeriesAggregate> {

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate tsAgg) {
        TimeSeriesAggregate transformed;
        // TODO: remove the two workarounds below when we no longer have to deal with 9.3 and 9.4 clusters in CCS
        transformed = replaceMergeWithMergeOverTime(tsAgg);
        transformed = patchHistogramComponentExtraction(transformed);
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

    /**
     * Detects plans produced by older coordinating nodes where the _over_time aggregation for histograms
     * is implemented as an EVAL with {@link ExtractHistogramComponent} directly before the {@link TimeSeriesAggregate}.
     * In these plans, the histogram component extraction is applied without respecting temporality.
     * <p>
     * We patch this by wrapping the histogram argument to {@link ExtractHistogramComponent} with
     * {@link FilterUnsupportedTemporality}, which returns null (with a warning) for non-delta temporalities.
     */
    private TimeSeriesAggregate patchHistogramComponentExtraction(TimeSeriesAggregate tsAgg) {
        LogicalPlan candidate = tsAgg.child();
        while (candidate instanceof Eval == false && candidate.children().size() == 1) {
            candidate = candidate.children().get(0);
        }
        if (candidate instanceof Eval eval) {
            boolean hasExtractHistogramComponent = false;
            for (Alias alias : eval.fields()) {
                if (alias.child() instanceof ExtractHistogramComponent) {
                    hasExtractHistogramComponent = true;
                    break;
                }
            }

            if (hasExtractHistogramComponent == false) {
                return tsAgg;
            }

            TemporalityAttribute temporalityAttr = new TemporalityAttribute(Source.EMPTY);

            List<Alias> patchedFields = new ArrayList<>(eval.fields().size());
            for (Alias alias : eval.fields()) {
                if (alias.child() instanceof ExtractHistogramComponent extractComponent) {
                    Expression filteredHistogram = new FilterUnsupportedTemporality(
                        extractComponent.source(),
                        extractComponent.field(),
                        temporalityAttr
                    );
                    Expression patchedExtract = new ExtractHistogramComponent(
                        extractComponent.source(),
                        filteredHistogram,
                        extractComponent.componentOrdinal()
                    );
                    patchedFields.add(new Alias(alias.source(), alias.name(), patchedExtract, alias.id(), alias.synthetic()));
                } else {
                    patchedFields.add(alias);
                }
            }

            Eval patchedEval = new Eval(eval.source(), eval.child(), patchedFields);
            return (TimeSeriesAggregate) tsAgg.transformDown(Eval.class, e -> e == eval ? patchedEval : e);
        }
        return tsAgg;
    }

    private static TimeSeriesAggregate injectTemporalityAttributesIntoEsRelation(TimeSeriesAggregate tsAgg) {
        // Normalize all temporality references to a single NameId so one EsRelation output can satisfy all uses.
        Holder<NameId> canonicalTemporalityId = new Holder<>();
        TimeSeriesAggregate normalized = (TimeSeriesAggregate) tsAgg.transformExpressionsDown(TemporalityAttribute.class, t -> {
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
