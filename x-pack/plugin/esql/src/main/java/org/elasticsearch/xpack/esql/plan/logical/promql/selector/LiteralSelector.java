/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PlaceholderRelation;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * Represents a PromQL literal scalar value wrapped as a vector selector.
 *
 * In PromQL, literal numeric values can be used as instant vectors where each
 * sample in the result has the same scalar value with no labels. This corresponds
 * to PromQL syntax like:
 *   42
 *   3.14
 *   -5.5
 *
 * Examples:
 *   http_requests_total + 10    // Add 10 to each sample
 *   cpu_usage * 100             // Multiply by 100
 *   rate(requests[5m]) > 0.5    // Compare against threshold
 *
 * The literal selector produces a single-value vector with no labels, allowing
 * literals to participate in vector operations and binary expressions.
 */
public class LiteralSelector extends Selector {
    private final Literal literal;

    public LiteralSelector(Source source, Literal literal) {
        this(source, PlaceholderRelation.INSTANCE, literal);
    }

    public LiteralSelector(Source source, LogicalPlan child, Literal literal) {
        super(source, child, literal, emptyList(), LabelMatchers.EMPTY, Evaluation.NONE);
        this.literal = literal;
    }

    public Literal literal() {
        return literal;
    }

    @Override
    protected NodeInfo<LiteralSelector> info() {
        return NodeInfo.create(this, LiteralSelector::new, child(), literal);
    }

    @Override
    public LiteralSelector replaceChild(LogicalPlan newChild) {
        return new LiteralSelector(source(), newChild, literal);
    }

    // @Override
    public String telemetryLabel() {
        return "PROMQL_SELECTOR_LITERAL";
    }

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
        LiteralSelector that = (LiteralSelector) o;
        return Objects.equals(literal, that.literal);
    }

    /**
     * LiteralSelector outputs three columns representing a scalar vector:
     * 0. labels - Empty (no labels for scalar values)
     * 1. timestamp - The evaluation timestamp
     * 2. value - The literal scalar value
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
        return Objects.hash(super.hashCode(), literal);
    }
}
