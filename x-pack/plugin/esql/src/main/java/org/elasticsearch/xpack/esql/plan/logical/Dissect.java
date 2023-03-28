/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Dissect extends UnaryPlan {
    private final Expression input;
    private final Parser parser;
    List<Attribute> extractedFields;

    public record Parser(String pattern, String appendSeparator, DissectParser parser) {

    }

    public Dissect(Source source, LogicalPlan child, Expression input, Parser parser, List<Attribute> extracted) {
        super(source, child);
        this.input = input;
        this.parser = parser;
        this.extractedFields = extracted;
    }

    @Override
    public boolean expressionsResolved() {
        return input.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Dissect(source(), newChild, input, parser, extractedFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Dissect::new, child(), input, parser, extractedFields);
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(extractedFields, child().output());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Dissect dissect = (Dissect) o;
        return Objects.equals(input, dissect.input)
            && Objects.equals(parser, dissect.parser)
            && Objects.equals(extractedFields, dissect.extractedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, parser, extractedFields);
    }

    public Expression input() {
        return input;
    }

    public Parser parser() {
        return parser;
    }

    public List<Attribute> extractedFields() {
        return extractedFields;
    }
}
