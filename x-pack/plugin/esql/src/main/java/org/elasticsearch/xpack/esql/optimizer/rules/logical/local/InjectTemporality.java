/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

public final class InjectTemporality extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan, LocalLogicalOptimizerContext context) {
        return logicalPlan.transformDown(TimeSeriesAggregate.class, tsAgg -> transform(tsAgg, context));
    }

    private TimeSeriesAggregate transform(TimeSeriesAggregate tsAgg, LocalLogicalOptimizerContext context) {
        TimeSeriesAggregate transformed;
        transformed = (TimeSeriesAggregate) tsAgg.transformExpressionsOnly(AggregateFunction.class, agg -> {
            if (agg instanceof TemporalityAware temporalityAware) {
                if (TemporalityAware.TEMPORALITY_UNSUPPORTED_MARKER.equals(temporalityAware.temporality())) {
                    // if the function doesn't support temporality on all nodes, make sure to filter out unsupported data
                    return filterOutTemporalitiesOtherThan(agg, temporalityAware.defaultTemporality(), context);
                } else if (temporalityAware.temporality() == null) {
                    // temporality is supported, but has not been explicitly provided as a parameter
                    // so we inject the temporality column
                    return temporalityAware.withTemporality(createTemporalityAttributeFor(agg));
                }
            }
            return agg;
        });
        return injectTemporalityAttributesIntoEsRelation(transformed);
    }

    private static TemporalityAttribute createTemporalityAttributeFor(AggregateFunction agg) {
        return new TemporalityAttribute(agg.source());
    }

    private AggregateFunction filterOutTemporalitiesOtherThan(
        AggregateFunction agg,
        TemporalityAware.Temporality defaultTemporality,
        LocalLogicalOptimizerContext context
    ) {
        Set<FieldAttribute.FieldName> temporalityFieldNames = context.searchStats()
            .targetShards()
            .values()
            .stream()
            .map(shard -> IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.get(shard.getSettings()))
            .filter(name -> name != null && name.isBlank() == false)
            .map(FieldAttribute.FieldName::new)
            .filter(fieldName -> context.searchStats().exists(fieldName))
            .collect(Collectors.toSet());

        // TODO: refine this check: use skipper min/max values to detect the actually used temporality values
        // and filter based on that instead of just the presence of the field name
        if (temporalityFieldNames.isEmpty()) {
            // We do support temporality on this node, but there there is no temporality data in the involved shards
            // so we don't have to filter anything out and don't need to warn about it
            return agg;
        }

        Source source = agg.source();
        addWarning(
            "The aggregate function [{}] operates on data that may contain unsupported temporality data; "
                + "unsupported data will be filtered out, upgrade older nodes to support querying this data",
            source
        );

        TemporalityAttribute tempAttrib = createTemporalityAttributeFor(agg);
        var condition = new Or(
            agg.source(),
            new Equals(agg.source(), tempAttrib, defaultTemporality.literal()),
            new IsNull(agg.source(), tempAttrib)
        );
        AggregateFunction result;
        if (agg.filter() == null) {
            result = agg.withFilter(condition);
        } else {
            result = agg.withFilter(new And(agg.source(), agg.filter(), condition));
        }
        return ((TemporalityAware) result).withTemporality(null);
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
