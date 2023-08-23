/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class GrokExec extends RegexExtractExec {

    private final Grok.Parser parser;

    public GrokExec(
        Source source,
        PhysicalPlan child,
        Expression inputExpression,
        Grok.Parser parser,
        List<Attribute> extractedAttributes
    ) {
        super(source, child, inputExpression, extractedAttributes);
        this.parser = parser;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new GrokExec(source(), newChild, inputExpression, parser, extractedFields);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, GrokExec::new, child(), inputExpression, parser, extractedFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        GrokExec grokExec = (GrokExec) o;
        return Objects.equals(parser, grokExec.parser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parser);
    }

    public Grok.Parser pattern() {
        return parser;
    }
}
