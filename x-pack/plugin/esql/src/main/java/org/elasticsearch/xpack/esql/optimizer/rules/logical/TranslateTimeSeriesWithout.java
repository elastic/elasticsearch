/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
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
public final class TranslateTimeSeriesWithout extends AnalyzerRules.ParameterizedAnalyzerRule<TimeSeriesAggregate, AnalyzerContext> {

    @Override
    protected boolean skipResolved() {
        return false;
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

    /**
     * Injects the lowered {@code _timeseries} attributes into the time-series source relation(s) feeding this aggregate, walking only the
     * main input path. The right-hand side of a {@code BinaryPlan} (a lookup index, or an {@code IN}-subquery rewritten to a
     * {@code SemiJoin}) is skipped: that subtree could be a separate time-series source with its own {@code _timeseries} grouping.
     * Crossing that boundary would inject this aggregate's lowered attribute into the nested subquery relation, producing a relation with
     * a duplicate {@code _timeseries} attribute when the subquery itself groups {@code BY WITHOUT}.
     */
    private static LogicalPlan addLoweredAttributesToTimeSeriesSource(
        LogicalPlan plan,
        Iterable<TimeSeriesMetadataAttribute> loweredAttributes
    ) {
        if (plan instanceof EsRelation relation) {
            return addLoweredAttributes(relation, loweredAttributes);
        }
        if (plan instanceof BinaryPlan binary) {
            return binary.replaceLeft(addLoweredAttributesToTimeSeriesSource(binary.left(), loweredAttributes));
        }
        List<LogicalPlan> newChildren = new ArrayList<>(plan.children().size());
        boolean changed = false;
        for (LogicalPlan child : plan.children()) {
            LogicalPlan newChild = addLoweredAttributesToTimeSeriesSource(child, loweredAttributes);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        return changed ? plan.replaceChildren(newChildren) : plan;
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, AnalyzerContext context) {
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
        LogicalPlan newChild = addLoweredAttributesToTimeSeriesSource(aggregate.child(), replacements.values());
        return aggregate.with(newChild, newGroupings, newAggregates);
    }
}
