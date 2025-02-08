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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A Merge is a {@code LeafPlan}, which holds several logical subplans.
 */
public class Merge extends LeafPlan {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Merge", Merge::new);

    private final List<LogicalPlan> subPlans;

    public Merge(Source source, List<LogicalPlan> subPlans) {
        super(source);
        this.subPlans = subPlans;
    }

    public Merge(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            null // in.readCollectionAsList(LogicalPlan::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeCollection(subPlans);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean expressionsResolved() {
        return subPlans.stream().allMatch(LogicalPlan::expressionsResolved);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source().hashCode(), subPlans);
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

        return Objects.equals(source(), other.source()) && Objects.equals(subPlans, other.subPlans);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Merge::new, subPlans);
    }

    @Override
    public List<Attribute> output() {
        return subPlans.getFirst().output();
    }

    public List<LogicalPlan> subPlans() {
        return subPlans;
    }
}
