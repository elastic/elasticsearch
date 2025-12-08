/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PlaceholderRelation;

import java.util.List;

/**
 * Represents a PromQL instant vector selector.
 *
 * An instant vector selects time series based on metric name and label matchers,
 * returning the most recent sample at the evaluation timestamp. This corresponds to PromQL syntax:
 *   metric_name{label="value"} offset 5m @ timestamp
 *
 * Examples:
 *   http_requests_total
 *   cpu_usage{host="web-1"}
 *   memory_used{env=~"prod.*"} offset 10m
 *   up{job="prometheus"} @ 1609746000
 *
 * The instant vector selects a single sample per matching time series at the
 * evaluation time (with optional offset/@ modifiers), representing the current state.
 *
 * Conceptually an instant selector is a range selector with a null range.
 */
public class InstantSelector extends Selector {

    public InstantSelector(Source source, Expression series, List<Expression> labels, LabelMatchers labelMatchers, Evaluation evaluation) {
        this(source, PlaceholderRelation.INSTANCE, series, labels, labelMatchers, evaluation);
    }

    public InstantSelector(
        Source source,
        LogicalPlan child,
        Expression series,
        List<Expression> labels,
        LabelMatchers labelMatchers,
        Evaluation evaluation
    ) {
        super(source, child, series, labels, labelMatchers, evaluation);
    }

    @Override
    protected NodeInfo<InstantSelector> info() {
        return NodeInfo.create(this, InstantSelector::new, child(), series(), labels(), labelMatchers(), evaluation());
    }

    @Override
    public InstantSelector replaceChild(LogicalPlan newChild) {
        return new InstantSelector(source(), newChild, series(), labels(), labelMatchers(), evaluation());
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_SELECTOR_INSTANT";
    // }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return super.equals(o);
    }

    /**
     * InstantSelector outputs three columns representing time series data:
     * 1. labels - The metric name and all label key-value pairs
     * 2. timestamp - The sample timestamp in milliseconds since epoch
     * 3. value - The metric value
     */
    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = List.of(
                new ReferenceAttribute(source(), "promql$labels", DataType.KEYWORD),
                new ReferenceAttribute(source(), "promql$timestamp", DataType.DATETIME),
                new ReferenceAttribute(source(), "promql$value", DataType.DOUBLE)
            );
        }
        return output;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
