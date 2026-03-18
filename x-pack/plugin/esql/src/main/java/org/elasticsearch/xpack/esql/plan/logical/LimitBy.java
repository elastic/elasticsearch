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
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Retains at most N rows per group defined by one or more grouping key expressions.
 * This is the {@code LIMIT N BY expr1, expr2, ...} command.
 */
public class LimitBy extends UnaryPlan implements TelemetryAware, PipelineBreaker {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "LimitBy", LimitBy::new);

    private final Expression limitPerGroup;
    private final List<Expression> groupings;

    /**
     * Important for optimizations. This should be {@code false} in most cases, which allows this instance to be duplicated past a child
     * plan node that increases the number of rows, like for LOOKUP JOIN and MV_EXPAND.
     * Needs to be set to {@code true} in {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimitBy} to avoid
     * infinite loops from adding a duplicate of the limit past the child over and over again.
     */
    private final transient boolean duplicated;

    public LimitBy(Source source, Expression limitPerGroup, LogicalPlan child, List<Expression> groupings) {
        this(source, limitPerGroup, child, groupings, false);
    }

    public LimitBy(Source source, Expression limitPerGroup, LogicalPlan child, List<Expression> groupings, boolean duplicated) {
        super(source, child);
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
        this.duplicated = duplicated;
    }

    private LimitBy(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            false
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(limitPerGroup());
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(groupings());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<LimitBy> info() {
        return NodeInfo.create(this, LimitBy::new, limitPerGroup, child(), groupings, duplicated);
    }

    @Override
    public LimitBy replaceChild(LogicalPlan newChild) {
        return new LimitBy(source(), limitPerGroup, newChild, groupings, duplicated);
    }

    public Expression limitPerGroup() {
        return limitPerGroup;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public boolean duplicated() {
        return duplicated;
    }

    public LimitBy withDuplicated(boolean duplicated) {
        return new LimitBy(source(), limitPerGroup, child(), groupings, duplicated);
    }

    @Override
    public boolean expressionsResolved() {
        return limitPerGroup.resolved() && Resolvables.resolved(groupings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limitPerGroup, child(), duplicated, groupings);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LimitBy other = (LimitBy) obj;

        return Objects.equals(limitPerGroup, other.limitPerGroup)
            && Objects.equals(child(), other.child())
            && (duplicated == other.duplicated)
            && Objects.equals(groupings, other.groupings);
    }
}
