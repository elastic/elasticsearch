/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Insist extends UnaryPlan {
    private final Attribute insistedAttribute;

    public Insist(Source source, Attribute insistedAttribute, LogicalPlan child) {
        super(source, child);
        this.insistedAttribute = insistedAttribute;
    }

    private @Nullable List<Attribute> lazyOutput = null;

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = computeOutput();
        }
        return lazyOutput;
    }

    private List<Attribute> computeOutput() {
        return NamedExpressions.mergeOutputAttributes(List.of(insistedAttribute), child().output());
    }

    public Attribute attribute() {
        return insistedAttribute;
    }

    @Override
    public Insist replaceChild(LogicalPlan newChild) {
        return new Insist(source(), insistedAttribute, newChild);
    }

    @Override
    public boolean expressionsResolved() {
        // Like EsqlProject, we allow unsupported attributes to flow through the engine.
        return attribute().resolved() || attribute() instanceof UnsupportedAttribute;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Insist::new, insistedAttribute, child());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Objects.hashCode(insistedAttribute));
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && ((Insist) obj).insistedAttribute.equals(insistedAttribute);
    }

    public LogicalPlan withAttribute(Attribute attribute) {
        return new Insist(source(), attribute, child());
    }
}
