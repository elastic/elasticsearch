/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A Fork is a {@code Plan} with one child, but holds several logical subplans, e.g.
 * {@code FORK [WHERE content:"fox" ] [WHERE content:"dog"] }
 */
public class Fork extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Fork", Fork::new);

    private final List<LogicalPlan> subPlans;

    public Fork(Source source, LogicalPlan child, List<LogicalPlan> subPlans) {
        super(source, child);
        this.subPlans = subPlans;
    }

    public Fork(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class));
        this.subPlans = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public List<LogicalPlan> subPlans() {
        return subPlans;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Fork(source(), newChild, subPlans);
    }

    @Override
    public boolean expressionsResolved() {
        return subPlans.stream().allMatch(LogicalPlan::expressionsResolved);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Fork::new, child(), subPlans);
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
        Fork other = (Fork) o;
        return Objects.equals(subPlans, other.subPlans);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subPlans);
    }
}
