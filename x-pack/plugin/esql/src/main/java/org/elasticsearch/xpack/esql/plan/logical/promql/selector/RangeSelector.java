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
import java.util.Objects;

/**
 * Represents a PromQL range vector selector.
 *
 * A range vector selects time series based on metric name and label matchers,
 * with a lookback time range. This corresponds to PromQL syntax:
 *   metric_name{label="value"}[duration] offset 5m @ timestamp
 *
 * Examples:
 *   http_requests_total[5m]
 *   cpu_usage{host="web-1"}[1h]
 *   memory_used{env=~"prod.*"}[30m] offset 10m
 *
 * The range vector selects all samples within the specified duration for each
 * matching time series, preparing data for range functions like rate() or avg_over_time().
 */
public class RangeSelector extends Selector {
    // Time_Period or Duration
    private final Expression range;

    public RangeSelector(
        Source source,
        Expression series,
        List<Expression> labels,
        LabelMatchers labelMatchers,
        Expression range,
        Evaluation evaluation
    ) {
        this(source, PlaceholderRelation.INSTANCE, series, labels, labelMatchers, range, evaluation);
    }

    public RangeSelector(
        Source source,
        LogicalPlan child,
        Expression series,
        List<Expression> labels,
        LabelMatchers labelMatchers,
        Expression range,
        Evaluation evaluation
    ) {
        super(source, child, series, labels, labelMatchers, evaluation);
        this.range = range;
    }

    public Expression range() {
        return range;
    }

    @Override
    protected NodeInfo<RangeSelector> info() {
        return NodeInfo.create(this, RangeSelector::new, child(), series(), labels(), labelMatchers(), range, evaluation());
    }

    @Override
    public RangeSelector replaceChild(LogicalPlan newChild) {
        return new RangeSelector(source(), newChild, series(), labels(), labelMatchers(), range, evaluation());
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_SELECTOR_RANGE";
    // }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        RangeSelector that = (RangeSelector) o;
        return Objects.equals(range, that.range);
    }

    /**
     * RangeSelector outputs three columns representing time series data:
     * 0. range - the step instance for the given interval
     * 1. labels - The metric name and all label key-value pairs
     * 2. timestamp - The sample timestamp in milliseconds since epoch
     * 3. value - The metric value
     */
    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = List.of(
                new ReferenceAttribute(source(), "promql$range", DataType.DATETIME),
                new ReferenceAttribute(source(), "promql$labels", DataType.KEYWORD),
                new ReferenceAttribute(source(), "promql$timestamp", DataType.DATETIME),
                new ReferenceAttribute(source(), "promql$value", DataType.DOUBLE)
            );
        }
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), range);
    }
}
