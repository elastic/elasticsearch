/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * Synthetic {@link LogicalPlan} used by the planner that the child plan is referred elsewhere.
 * Essentially this means
 * referring to another node in the plan and acting as a relationship.
 * Used for duplicating parts of the plan without having to clone the nodes.
 */
public class StubRelation extends LeafPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "StubRelation",
        StubRelation::new
    );

    public static final StubRelation EMPTY = new StubRelation(null, null);

    private final List<Attribute> output;

    public StubRelation(Source source, List<Attribute> output) {
        super(source);
        this.output = output;
    }

    public StubRelation(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), emptyList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<StubRelation> info() {
        return NodeInfo.create(this, StubRelation::new, output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(StubRelation.class, output);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StubRelation other = (StubRelation) obj;
        return Objects.equals(output, other.output());
    }
}
