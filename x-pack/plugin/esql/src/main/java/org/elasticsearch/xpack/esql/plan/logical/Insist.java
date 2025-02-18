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

public class Insist extends UnaryPlan implements SurrogateLogicalPlan {
    private final List<? extends Attribute> insistedAttributes;
    private @Nullable List<Attribute> lazyOutput = null;

    public Insist(Source source, LogicalPlan child, List<? extends Attribute> insistedAttributes) {
        super(source, child);
        this.insistedAttributes = insistedAttributes;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = NamedExpressions.mergeOutputAttributes(insistedAttributes, child().output());
        }
        return lazyOutput;
    }

    public List<? extends Attribute> insistedAttributes() {
        return insistedAttributes;
    }

    @Override
    public Insist replaceChild(LogicalPlan newChild) {
        return new Insist(source(), newChild, insistedAttributes);
    }

    @Override
    public boolean expressionsResolved() {
        // Like EsqlProject, we allow unsupported attributes to flow through the engine.
        return insistedAttributes().stream().allMatch(a -> a.resolved() || a instanceof UnsupportedAttribute);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            (source, insistedAttributes1, child) -> new Insist(source, child, insistedAttributes1),
            insistedAttributes,
            child()
        );
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
        return Objects.hash(super.hashCode(), Objects.hashCode(insistedAttributes));
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(((Insist) obj).insistedAttributes, insistedAttributes);
    }

    @Override
    public LogicalPlan surrogate() {
        return new Project(source(), child(), output());
    }

    public Insist withAttributes(List<Attribute> attributes) {
        return new Insist(source(), child(), attributes);
    }
}
