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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.FieldAttribute.timeSeriesField;

/**
 * Lowers {@link TimeSeriesWithout} into the {@code _timeseries} metadata attribute expected by
 * the time-series aggregation pipeline.
 */
public final class TranslateTimeSeriesWithout extends AnalyzerRules.ParameterizedAnalyzerRule<TimeSeriesAggregate, AnalyzerContext> {

    @Override
    protected boolean skipResolved() {
        return false;
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
     * Injects the lowered {@code _timeseries} attributes into the time-series source relation(s) feeding this aggregate. Traversal scope
     * (skipping the right-hand side of a {@code BinaryPlan}) is documented on {@code TranslateTimeSeriesUtils#transformTimeSeriesSource}.
     */
    private static LogicalPlan addLoweredAttributesToTimeSeriesSource(
        LogicalPlan plan,
        Iterable<TimeSeriesMetadataAttribute> loweredAttributes
    ) {
        return TranslateTimeSeriesUtils.transformTimeSeriesSource(plan, relation -> addLoweredAttributes(relation, loweredAttributes));
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, AnalyzerContext context) {
        // Keep metadata source fields canonical and preserve each grouping's identity through an alias.
        var renamedGroupings = new LinkedHashMap<NameId, Attribute>();
        var loweredAttributes = new ArrayList<TimeSeriesMetadataAttribute>();
        var newGroupings = new ArrayList<Expression>(aggregate.groupings().size());

        for (Expression grouping : aggregate.groupings()) {
            if (Alias.unwrap(grouping) instanceof TimeSeriesWithout fn) {
                // Replace the semantic WITHOUT node with the concrete _timeseries field attribute,
                // collecting the excluded dimension names from the function's children.
                var excludedFields = new LinkedHashSet<String>(fn.children().size());
                for (var field : fn.children()) {
                    if (field instanceof FieldAttribute fa) {
                        excludedFields.add(fa.fieldName().string());
                    } else if (field instanceof Attribute attr) {
                        excludedFields.add(attr.name());
                    }
                }

                Attribute groupingAttribute = Expressions.attribute(grouping);
                assert groupingAttribute != null : "time-series grouping must be a named expression";
                var lowered = new TimeSeriesMetadataAttribute(
                    grouping.source(),
                    null,
                    null,
                    MetadataAttribute.TIMESERIES,
                    timeSeriesField(),
                    groupingAttribute.nullable(),
                    new NameId(),
                    groupingAttribute.synthetic(),
                    excludedFields
                );
                loweredAttributes.add(lowered);
                // Native WITHOUT exposes _timeseries; PromQL assigns a distinct name to each metadata projection.
                String name = aggregate.origin() == TimeSeriesAggregate.Origin.PROMQL_COMMAND
                    ? groupingAttribute.name()
                    : MetadataAttribute.TIMESERIES;
                var alias = new Alias(grouping.source(), name, lowered, groupingAttribute.id(), groupingAttribute.synthetic());
                newGroupings.add(alias);
                if (name.equals(groupingAttribute.name()) == false) {
                    renamedGroupings.put(groupingAttribute.id(), alias.toAttribute());
                }
            } else {
                newGroupings.add(grouping);
            }
        }

        if (loweredAttributes.isEmpty()) {
            return aggregate;
        }

        // Preserve the native WITHOUT output column name in references to the grouping.
        List<NamedExpression> newAggregates = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression agg : aggregate.aggregates()) {
            newAggregates.add(
                (NamedExpression) agg.transformDown(Attribute.class, attribute -> renamedGroupings.getOrDefault(attribute.id(), attribute))
            );
        }
        LogicalPlan newChild = addLoweredAttributesToTimeSeriesSource(aggregate.child(), loweredAttributes);
        return aggregate.with(newChild, newGroupings, newAggregates);
    }
}
