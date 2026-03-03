/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Limit extends UnaryPlan implements TelemetryAware, PipelineBreaker, ExecutesOn {
    public static final TransportVersion ESQL_LIMIT_BY = TransportVersion.fromName("esql_limit_by");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Limit", Limit::new);

    private final Expression limit;

    private List<Expression> groupings;
    /**
     * Important for optimizations. This should be {@code false} in most cases, which allows this instance to be duplicated past a child
     * plan node that increases the number of rows, like for LOOKUP JOIN and MV_EXPAND.
     * Needs to be set to {@code true} in {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits} to avoid
     * infinite loops from adding a duplicate of the limit past the child over and over again.
     */
    private final transient boolean duplicated;
    /**
     * Local limit is not a pipeline breaker, and is applied only to the local node's data.
     * It should always end up inside a fragment.
     */
    private final transient boolean local;

    /**
     * Default way to create a new instance. Do not use this to copy an existing instance, as this sets {@link Limit#duplicated}
     * and {@link Limit#local} to {@code false}.
     */
    public Limit(Source source, Expression limit, LogicalPlan child) {
        this(source, limit, child, false, false);
    }

    /**
     * Create a new instance with groupings, which are the expressions used in LIMIT BY. This sets {@link Limit#duplicated}
     * and {@link Limit#local} to {@code false}.
     */
    public Limit(Source source, Expression limit, LogicalPlan child, List<Expression> groupings) {
        this(source, limit, child, groupings, false, false);
    }

    public Limit(Source source, Expression limit, LogicalPlan child, List<Expression> groupings, boolean duplicated, boolean local) {
        super(source, child);
        this.limit = limit;
        this.duplicated = duplicated;
        this.local = local;
        this.groupings = groupings;
    }

    public Limit(Source source, Expression limit, LogicalPlan child, boolean duplicated, boolean local) {
        this(source, limit, child, List.of(), duplicated, local);
    }

    /**
     * Omits reading {@link Limit#duplicated}, c.f. {@link Limit#writeTo}.
     */
    private Limit(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(LogicalPlan.class),
            List.of(),
            false,
            false
        );

        if (in.getTransportVersion().supports(ESQL_LIMIT_BY)) {
            this.groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        }
    }

    /**
     * Omits serializing {@link Limit#duplicated} because this is only required to avoid duplicating a limit past
     * {@link org.elasticsearch.xpack.esql.plan.logical.join.Join} or {@link MvExpand} in an infinite loop, see
     * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits}.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(limit());
        out.writeNamedWriteable(child());

        if (out.getTransportVersion().supports(ESQL_LIMIT_BY)) {
            out.writeNamedWriteableCollection(groupings());
        } else if (groupings.isEmpty() == false) {
            throw new IllegalArgumentException("LIMIT BY is not supported by all nodes in the cluster");
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Limit> info() {
        return NodeInfo.create(this, Limit::new, limit, child(), groupings, duplicated, local);
    }

    @Override
    public Limit replaceChild(LogicalPlan newChild) {
        return new Limit(source(), limit, newChild, groupings, duplicated, local);
    }

    public Expression limit() {
        return limit;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Limit withLimit(Expression limit) {
        return new Limit(source(), limit, child(), groupings, duplicated, local);
    }

    public boolean duplicated() {
        return duplicated;
    }

    public boolean local() {
        return local;
    }

    public Limit withDuplicated(boolean duplicated) {
        return new Limit(source(), limit, child(), groupings, duplicated, local);
    }

    public Limit withLocal(boolean newLocal) {
        return new Limit(source(), limit, child(), groupings, duplicated, newLocal);
    }

    @Override
    public boolean expressionsResolved() {
        return limit.resolved() && Resolvables.resolved(groupings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, child(), duplicated, local, groupings);
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
            && (duplicated == other.duplicated)
            && (local == other.local)
            && Objects.equals(groupings, other.groupings);
    }

    @Override
    public ExecuteLocation executesOn() {
        // Global limit always needs to be on the coordinator
        return local ? ExecuteLocation.ANY : ExecuteLocation.COORDINATOR;
    }
}
