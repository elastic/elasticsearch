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
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

public class Limit extends UnaryPlan implements TelemetryAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Limit", Limit::new);

    private final Expression limit;
    /**
     * Important for optimizations. This should be {@code false} in most cases, which allows this instance to be duplicated past a child
     * plan node that increases the number of rows, like for LOOKUP JOIN and MV_EXPAND.
     * Needs to be set to {@code true} in {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits} to avoid
     * infinite loops from adding a duplicate of the limit past the child over and over again.
     */
    private final transient boolean duplicated;

    /**
     * Default way to create a new instance. Do not use this to copy an existing instance, as this sets {@link Limit#duplicated} to
     * {@code false}.
     */
    public Limit(Source source, Expression limit, LogicalPlan child) {
        this(source, limit, child, false);
    }

    public Limit(Source source, Expression limit, LogicalPlan child, boolean duplicated) {
        super(source, child);
        this.limit = limit;
        this.duplicated = duplicated;
    }

    /**
     * Omits reading {@link Limit#duplicated}, c.f. {@link Limit#writeTo}.
     */
    private Limit(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(LogicalPlan.class),
            false
        );
    }

    /**
     * Omits serializing {@link Limit#duplicated} because when sent to a data node, this should always be {@code false}.
     * That's because if it's true, this means a copy of this limit was pushed down below an MvExpand or Join, and thus there's
     * another pipeline breaker further upstream - we're already on the coordinator node.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(limit());
        out.writeNamedWriteable(child());
        // Let's make sure we notice during tests if we ever serialize a duplicated Limit.
        assert duplicated == false;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Limit> info() {
        return NodeInfo.create(this, Limit::new, limit, child(), duplicated);
    }

    @Override
    public Limit replaceChild(LogicalPlan newChild) {
        return new Limit(source(), limit, newChild, duplicated);
    }

    public Expression limit() {
        return limit;
    }

    public Limit withLimit(Expression limit) {
        return new Limit(source(), limit, child(), duplicated);
    }

    public boolean duplicated() {
        return duplicated;
    }

    public Limit withDuplicated(boolean duplicated) {
        return new Limit(source(), limit, child(), duplicated);
    }

    @Override
    public boolean expressionsResolved() {
        return limit.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, child(), duplicated);
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

        return Objects.equals(limit, other.limit) && Objects.equals(child(), other.child()) && (duplicated == other.duplicated);
    }
}
