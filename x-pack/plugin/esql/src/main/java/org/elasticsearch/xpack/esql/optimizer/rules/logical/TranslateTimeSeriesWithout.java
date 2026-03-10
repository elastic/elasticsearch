/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lowers {@link TimeSeriesWithout} into the {@code _timeseries} metadata attribute expected by
 * the time-series aggregation pipeline.
 */
public final class TranslateTimeSeriesWithout extends OptimizerRules.ParameterizedOptimizerRule<
    TimeSeriesAggregate,
    LogicalOptimizerContext> {

    public TranslateTimeSeriesWithout() {
        super(OptimizerRules.TransformDirection.UP);
    }

    private static NamedExpression replaceReferences(NamedExpression expression, Map<NameId, TimeSeriesMetadataAttribute> replacements) {
        return (NamedExpression) expression.transformDown(Attribute.class, attribute -> {
            Attribute replacement = replacements.get(attribute.id());
            return replacement != null ? replacement : attribute;
        });
    }

    private static EsRelation addLoweredAttributes(EsRelation relation, Iterable<TimeSeriesMetadataAttribute> loweredAttributes) {
        var existingIds = relation.outputSet();
        List<Attribute> attributes = new ArrayList<>(relation.output());
        boolean changed = false;
        for (TimeSeriesMetadataAttribute loweredAttribute : loweredAttributes) {
            if (existingIds.contains(loweredAttribute) == false) {
                attributes.add(loweredAttribute);
                changed = true;
            }
        }
        return changed ? relation.withAttributes(attributes) : relation;
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, LogicalOptimizerContext context) {
        // Collect TimeSeriesWithout groupings and lower each into a TimeSeriesMetadataAttribute
        // that carries the excluded dimension names.
        Map<NameId, TimeSeriesMetadataAttribute> replacements = new LinkedHashMap<>();
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings().size());
        boolean changed = false;

        for (Expression grouping : aggregate.groupings()) {
            if (Alias.unwrap(grouping) instanceof TimeSeriesWithout without) {
                // Replace the semantic WITHOUT node with a concrete _timeseries attribute
                // preserving the original attribute's identity (name id) so downstream
                // references resolve correctly.
                Attribute groupingAttribute = Expressions.attribute(grouping);
                assert groupingAttribute != null : "time-series grouping must be a named expression";
                TimeSeriesMetadataAttribute lowered = TimeSeriesMetadataAttribute.from(groupingAttribute, without.excludedFieldNames());
                replacements.put(groupingAttribute.id(), lowered);
                newGroupings.add(lowered);
                changed = true;
            } else {
                newGroupings.add(grouping);
            }
        }

        if (changed == false) {
            return aggregate;
        }

        // Propagate the lowered attributes into aggregate expressions that reference the old grouping ids.
        List<NamedExpression> newAggregates = aggregate.aggregates().stream().map(agg -> replaceReferences(agg, replacements)).toList();
        // Add the new _timeseries attribute to the EsRelation so field extraction can find it.
        LogicalPlan newChild = aggregate.child()
            .transformDown(EsRelation.class, relation -> addLoweredAttributes(relation, replacements.values()));

        return aggregate.with(newChild, newGroupings, newAggregates);
    }
}
