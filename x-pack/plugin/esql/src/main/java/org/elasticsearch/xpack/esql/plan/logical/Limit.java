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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

public class Limit extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Limit", Limit::new);

    private final Expression limit;
    /**
     * Important for optimizations. This should be {@code true} in most cases, which allows this instance to be duplicated past a child plan
     * node that increases the number of rows, like for LOOKUP JOIN and MV_EXPAND.
     * Needs to be set to {@code false} in {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits} to avoid
     * infinite loops from adding a duplicate of the limit past the child over and over again.
     */
    private final boolean allowDuplicatePastExpandingNode;

    public Limit(Source source, Expression limit, LogicalPlan child, boolean allowDuplicatePastExpandingNode) {
        super(source, child);
        this.limit = limit;
        this.allowDuplicatePastExpandingNode = allowDuplicatePastExpandingNode;
    }

    private Limit(StreamInput in) throws IOException {
        // TODO: new transport version
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(LogicalPlan.class),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: new transport version
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(limit());
        out.writeNamedWriteable(child());
        out.writeBoolean(allowDuplicatePastExpandingNode);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Limit> info() {
        return NodeInfo.create(this, Limit::new, limit, child(), allowDuplicatePastExpandingNode);
    }

    @Override
    public Limit replaceChild(LogicalPlan newChild) {
        return new Limit(source(), limit, newChild, allowDuplicatePastExpandingNode);
    }

    public Expression limit() {
        return limit;
    }

    public boolean allowDuplicatePastExpandingNode() {
        return allowDuplicatePastExpandingNode;
    }

    @Override
    public String commandName() {
        return "LIMIT";
    }

    @Override
    public boolean expressionsResolved() {
        return limit.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, child(), allowDuplicatePastExpandingNode);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Limit other = (Limit) obj;

        return Objects.equals(limit, other.limit)
            && Objects.equals(child(), other.child())
            && Objects.equals(allowDuplicatePastExpandingNode, other.allowDuplicatePastExpandingNode);
    }
}
