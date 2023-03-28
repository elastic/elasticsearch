/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

@Experimental
public class DissectExec extends UnaryExec {

    private final Expression inputExpression;
    private final Dissect.Parser parser;
    List<Attribute> extractedAttributes;

    public DissectExec(
        Source source,
        PhysicalPlan child,
        Expression inputExpression,
        Dissect.Parser parser,
        List<Attribute> extractedAttributes
    ) {
        super(source, child);
        this.inputExpression = inputExpression;
        this.parser = parser;
        this.extractedAttributes = extractedAttributes;
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(extractedAttributes, child().output());
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new DissectExec(source(), newChild, inputExpression, parser, extractedAttributes);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, DissectExec::new, child(), inputExpression, parser, extractedAttributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DissectExec that = (DissectExec) o;
        return Objects.equals(inputExpression, that.inputExpression)
            && Objects.equals(parser, that.parser)
            && Objects.equals(extractedAttributes, that.extractedAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inputExpression, parser, extractedAttributes);
    }

    public Expression inputExpression() {
        return inputExpression;
    }

    public Dissect.Parser parser() {
        return parser;
    }

    public List<Attribute> extractedFields() {
        return extractedAttributes;
    }
}
