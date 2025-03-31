/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * A Merge is a {@code LogicalPlan}, which has several logical subplans/children.
 */
public class Merge extends LogicalPlan {

    /**
     * Replaces the stubbed source with the actual source.
     */
    public static LogicalPlan replaceStub(LogicalPlan source, LogicalPlan stubbed) {
        return stubbed.transformUp(StubRelation.class, stubRelation -> source);
    }

    List<Attribute> lazyOutput;

    public Merge(Source source, List<LogicalPlan> children) {
        super(source, children);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new Merge(source(), newChildren);
    }

    @Override
    public boolean expressionsResolved() {
        return children().stream().allMatch(LogicalPlan::expressionsResolved);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Merge::new, children());
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(children().get(1).output(), children().getFirst().output());
        }
        return lazyOutput;
    }

    @Override
    public int hashCode() {
        return Objects.hash(children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Merge other = (Merge) o;
        return Objects.equals(children(), other.children());
    }
}
