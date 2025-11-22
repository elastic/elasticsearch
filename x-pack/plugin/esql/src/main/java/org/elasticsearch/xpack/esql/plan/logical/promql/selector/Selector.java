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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PlaceholderRelation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Base class representing a PromQL vector selector.
 * A vector selector is defined by a set of label matchers and a point in time evaluation context.
 */
public abstract class Selector extends UnaryPlan {
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

    @Override
    public boolean expressionsResolved() {
        return series.resolved() && Resolvables.resolved(labels);
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
