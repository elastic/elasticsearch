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
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

    private static Set<String> excludedFieldsOf(TimeSeriesWithout fn) {
        var excludedFields = new LinkedHashSet<String>(fn.children().size());
        for (var field : fn.children()) {
            if (field instanceof FieldAttribute fa) {
                excludedFields.add(fa.fieldName().string());
            } else if (field instanceof Attribute attr) {
                excludedFields.add(attr.name());
            }
        }
        return excludedFields;
    }

    /** The exclusion sets of the packings the source already carries, in the scope the lowering injects into. */
    private static void collectSourcePackings(LogicalPlan plan, Set<Set<String>> packings) {
        if (plan instanceof EsRelation relation) {
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof TimeSeriesMetadataAttribute packing) {
                    packings.add(packing.excludedFields());
                }
            }
            return;
        }
        if (plan instanceof BinaryPlan binary) {
            collectSourcePackings(binary.left(), packings);
            return;
        }
        for (LogicalPlan child : plan.children()) {
            collectSourcePackings(child, packings);
        }
    }

    /**
     * A packing the source already carries for these exclusions, if any, searched in the same scope the lowering
     * injects into (down the left-hand side of binary plans). Sharing it keeps a second aggregate over the same
     * source from injecting a same-named twin.
     */
    private static TimeSeriesMetadataAttribute findLoweredInSource(LogicalPlan plan, Set<String> excludedFields) {
        if (plan instanceof EsRelation relation) {
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof TimeSeriesMetadataAttribute packing && packing.excludedFields().equals(excludedFields)) {
                    return packing;
                }
            }
            return null;
        }
        if (plan instanceof BinaryPlan binary) {
            return findLoweredInSource(binary.left(), excludedFields);
        }
        for (LogicalPlan child : plan.children()) {
            TimeSeriesMetadataAttribute found = findLoweredInSource(child, excludedFields);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, AnalyzerContext context) {
        // Collect TimeSeriesWithout groupings and lower each into a TimeSeriesMetadataAttribute
        // that carries the excluded dimension names.
        var replacements = new LinkedHashMap<NameId, TimeSeriesMetadataAttribute>();
        // Lowered attributes that stay hidden behind a user alias: they are added to the source relation but
        // never substituted into the aggregates, which keep referencing the alias by its original id.
        var aliasedLowered = new ArrayList<TimeSeriesMetadataAttribute>();
        var newGroupings = new ArrayList<Expression>(aggregate.groupings().size());
        // Every exclusion set that must coexist at the source: this aggregate's groupings plus the packings
        // already carried below. A lone set lowers to the canonical `_timeseries` name; several need distinct
        // derived names, or the injected attributes would collide with each other in the source output.
        var coexisting = new LinkedHashSet<Set<String>>();
        for (Expression grouping : aggregate.groupings()) {
            if (Alias.unwrap(grouping) instanceof TimeSeriesWithout fn) {
                coexisting.add(excludedFieldsOf(fn));
            }
        }
        collectSourcePackings(aggregate.child(), coexisting);
        boolean sharedSource = coexisting.size() > 1;
        // Lowerings minted for identical exclusions within this aggregate share one attribute.
        var loweredByExclusions = new LinkedHashMap<Set<String>, TimeSeriesMetadataAttribute>();

        for (Expression grouping : aggregate.groupings()) {
            if (Alias.unwrap(grouping) instanceof TimeSeriesWithout fn) {
                // Replace the semantic WITHOUT node with the concrete _timeseries field attribute,
                // collecting the excluded dimension names from the function's children.
                var excludedFields = excludedFieldsOf(fn);

                // The grouping is a named expression (Alias) wrapping the WITHOUT function; resolve its
                // attribute so the lowered _timeseries attribute can preserve the original identity (name id).
                Attribute groupingAttribute = Expressions.attribute(grouping);
                assert groupingAttribute != null : "time-series grouping must be a named expression";

                // A user alias (BY dims = WITHOUT(...)) must survive as the grouping's output name. The lowered
                // attribute then gets a fresh id and sits behind the alias, which keeps the user's id, so references to
                // the grouping resolved before this rule ran still point at a column named by the user.
                boolean keepAlias = grouping instanceof Alias alias && alias.name().equals(MetadataAttribute.TIMESERIES) == false;
                TimeSeriesMetadataAttribute lowered;
                if (groupingAttribute instanceof TimeSeriesMetadataAttribute t && excludedFields.equals(t.excludedFields())) {
                    lowered = t;
                } else if (loweredByExclusions.get(excludedFields) instanceof TimeSeriesMetadataAttribute repeated) {
                    lowered = repeated;
                } else if (findLoweredInSource(aggregate.child(), excludedFields) instanceof TimeSeriesMetadataAttribute existing) {
                    // Another aggregate over the same source already lowered these exclusions: share its attribute
                    // instead of injecting a same-named twin into the source relation.
                    lowered = existing;
                } else {
                    String parentName = groupingAttribute instanceof FieldAttribute fieldAttribute ? fieldAttribute.parentName() : null;
                    lowered = new TimeSeriesMetadataAttribute(
                        groupingAttribute.source(),
                        parentName,
                        groupingAttribute.qualifier(),
                        // A lone packing keeps the canonical name the pipeline expects; coexisting packings take
                        // distinct derived names (the same mapping the translation mints for them).
                        sharedSource ? TranslationContext.mapOpen(excludedFields) : MetadataAttribute.TIMESERIES,
                        timeSeriesField(),
                        groupingAttribute.nullable(),
                        keepAlias ? new NameId() : groupingAttribute.id(),
                        groupingAttribute.synthetic(),
                        excludedFields
                    );
                    loweredByExclusions.put(excludedFields, lowered);
                }

                if (keepAlias) {
                    Alias alias = (Alias) grouping;
                    aliasedLowered.add(lowered);
                    newGroupings.add(new Alias(alias.source(), alias.name(), lowered, alias.id()));
                } else {
                    replacements.put(lowered.id(), lowered);
                    newGroupings.add(lowered);
                }
            } else {
                newGroupings.add(grouping);
            }
        }

        if (replacements.isEmpty() && aliasedLowered.isEmpty()) {
            return aggregate;
        }

        // Propagate the lowered attributes into aggregate expressions that reference the old grouping ids.
        List<NamedExpression> newAggregates = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression agg : aggregate.aggregates()) {
            newAggregates.add((NamedExpression) agg.transformDown(Attribute.class, attribute -> {
                Attribute replacement = replacements.get(attribute.id());
                return replacement != null ? replacement : attribute;
            }));
        }

        // Add the new _timeseries attributes to the EsRelation so field extraction can find them.
        var allLowered = new ArrayList<>(replacements.values());
        allLowered.addAll(aliasedLowered);
        LogicalPlan newChild = addLoweredAttributesToTimeSeriesSource(aggregate.child(), allLowered);
        return aggregate.with(newChild, newGroupings, newAggregates);
    }
}
