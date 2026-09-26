/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PlaceholderRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.DynamicColumnList;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.Static;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult.Kind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base class representing a PromQL vector selector.
 * A vector selector is defined by a set of label matchers and a point in time evaluation context.
 */
public abstract sealed class Selector extends UnaryPlan implements PromqlPlan permits InstantSelector, RangeSelector, LiteralSelector {
    // implements TelemetryAware

    // in Promql this is the __name__ label however for now, this gets mapped to an exact field
    private final Expression series;
    private final List<Expression> labels;
    private final LabelMatchers labelMatchers;
    private final Evaluation evaluation;
    protected List<Attribute> output;

    Selector(Source source, Expression series, List<Expression> labels, LabelMatchers labelMatchers, Evaluation evaluation) {
        this(source, PlaceholderRelation.INSTANCE, series, labels, labelMatchers, evaluation);
    }

    Selector(
        Source source,
        LogicalPlan child,
        Expression series,
        List<Expression> labels,
        LabelMatchers labelMatchers,
        Evaluation evaluation
    ) {
        super(source, child);
        this.series = series;
        this.labels = labels;
        this.labelMatchers = labelMatchers;
        this.evaluation = evaluation;
    }

    public Expression series() {
        return series;
    }

    public List<Expression> labels() {
        return labels;
    }

    public LabelMatchers labelMatchers() {
        return labelMatchers;
    }

    public Evaluation evaluation() {
        return evaluation;
    }

    /**
     * The scan: the source relation with the labels the enclosing node requires bound to its fields and its dynamic
     * columns to {@link TimeSeriesMetadataAttribute}s added to the relation. The label matchers lower to a pending filter
     * predicate the enclosing translation pushes down to the relation.
     */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        PromqlCommand cmd = translation.cmd();
        LogicalPlan input = cmd.child();
        if (PromqlLogicalPlanBuilder.tryFoldRelation(cmd, input) != null) {
            // no matching index: an empty compile-time relation
            var empty = new LocalRelation(cmd.source(), List.of(cmd.valueAttribute(), cmd.stepAttribute()), EmptyLocalSupplier.EMPTY);
            return new TranslationResult(empty, Map.of(), Literal.NULL, cmd.stepAttribute(), null, Kind.CONSTANT);
        }
        Expression matcherPredicate = labelMatchers.predicate(source(), labels, translation.configuration());

        // Dimension fields define series identity; non-metric, non-packed fields are available as labels too. Non-dimension
        // keyword fields (e.g. k8s.pod.name in a TSDB index that lacks it as a dimension) must still be bindable as keys.
        List<Attribute> labelFields = input.output()
            .stream()
            .filter(
                attr -> attr instanceof FieldAttribute field
                    && field.isMetric() == false
                    && attr instanceof TimeSeriesMetadataAttribute == false
            )
            .toList();
        // IN -> columns: a complement becomes a packed column, a name the relation's field (missing ones null-fill later)
        TranslationConstraint required = translation.required();
        var labels = new LinkedHashMap<TranslationColumn, Attribute>();
        var added = new ArrayList<TimeSeriesMetadataAttribute>();
        for (Set<String> except : required.allBut()) {
            var column = new DynamicColumnList(except);
            Attribute packed = column.find(input.output());
            if (packed == null) {
                var metadata = new TimeSeriesMetadataAttribute(source(), except);
                added.add(metadata);
                packed = metadata;
            }
            labels.put(column, packed);
        }
        for (String name : required.names()) {
            Attribute field = PromqlLabels.find(labelFields, name);
            if (field != null) {
                labels.put(new Static(name), field);
            }
        }
        if (added.isEmpty() == false) {
            // in grouping-key order: fewest exclusions first
            added.sort(
                Comparator.comparingInt((TimeSeriesMetadataAttribute a) -> a.excludedFields().size())
                    .thenComparing(a -> new DynamicColumnList(a.excludedFields()).name())
            );
            List<Attribute> additional = List.copyOf(added);
            input = input.transformUp(EsRelation.class, relation -> relation.withAdditionalAttributes(additional));
        }
        return new TranslationResult(
            input,
            labels,
            sample(translation.time()),
            translation.stepAttr(),
            matcherPredicate,
            Kind.BEFORE_INITIAL_AGGREGATE
        );
    }

    /** The per-series sample: the series itself for a range vector; an instant vector overrides with its latest value. */
    protected Expression sample(Expression time) {
        return series;
    }

    @Override
    public boolean expressionsResolved() {
        return (series == null || series.resolved()) && Resolvables.resolved(labels);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            Selector selector = (Selector) o;
            return Objects.equals(evaluation, selector.evaluation)
                && Objects.equals(labelMatchers, selector.labelMatchers)
                && Objects.equals(series, selector.series)
                && Objects.equals(labels, selector.labels);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), series, labels, labelMatchers, evaluation);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("should not serialize");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("should not serialize");
    }
}
