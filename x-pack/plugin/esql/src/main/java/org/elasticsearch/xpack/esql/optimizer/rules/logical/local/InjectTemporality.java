/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

public final class InjectTemporality extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan, LocalLogicalOptimizerContext context) {
        return logicalPlan.transformDown(TimeSeriesAggregate.class, tsAgg -> transform(tsAgg, context));
    }

    private TimeSeriesAggregate transform(TimeSeriesAggregate tsAgg, LocalLogicalOptimizerContext context) {
        TimeSeriesAggregate transformed;
        transformed = (TimeSeriesAggregate) tsAgg.transformExpressionsOnly(AggregateFunction.class, agg -> {
            if (agg instanceof TemporalityAware temporalityAware && temporalityAware.temporality() == null) {
                if (IndexSettings.TIME_SERIES_TEMPORALITY_FEATURE_FLAG.isEnabled()) {
                    return temporalityAware.withTemporality(new TemporalityAttribute(agg.source()));
                } else {
                    // inject a placeholder so that the aggregators are always invoked with the expected number of arguments
                    return temporalityAware.withTemporality(Literal.NULL);
                }
            }
            return agg;
        });
        return injectTemporalityAttributesIntoEsRelation(transformed);
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
        return (TimeSeriesAggregate) normalized.transformDownSkipBranch((plan, skip) -> {
            if (plan instanceof EsRelation relation) {
                // no need to look further, we only want to transform the first EsRelation we encounter
                skip.set(true);
                return relation.withAdditionalAttribute(new TemporalityAttribute(Source.EMPTY).withId(canonicalTemporalityId.get()));
            }
            return plan;
        });
    }

}
