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
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Retains a ratio of rows per group using Bresenham-style streaming sampling.
 * For {@code limit_ratio(r, v)}, exactly {@code ceil(r * N)} of N rows are kept per group,
 * in arrival order, with O(groups) state and no buffering.
 */
public class LimitRatioBy extends UnaryPlan implements PipelineBreaker {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "LimitRatioBy",
        LimitRatioBy::new
    );

    private final Expression ratio;
    private final List<Expression> groupings;

    public LimitRatioBy(Source source, LogicalPlan child, Expression ratio, List<Expression> groupings) {
        super(source, child);
        this.ratio = ratio;
        this.groupings = groupings;
    }

    private LimitRatioBy(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(ratio());
        out.writeNamedWriteableCollection(groupings());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<LimitRatioBy> info() {
        return NodeInfo.create(this, LimitRatioBy::new, child(), ratio, groupings);
    }

    @Override
    public LimitRatioBy replaceChild(LogicalPlan newChild) {
        return new LimitRatioBy(source(), newChild, ratio, groupings);
    }

    public Expression ratio() {
        return ratio;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    @Override
    public boolean expressionsResolved() {
        return ratio.resolved() && Resolvables.resolved(groupings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ratio, child(), groupings);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LimitRatioBy other = (LimitRatioBy) obj;
        return Objects.equals(ratio, other.ratio) && Objects.equals(child(), other.child()) && Objects.equals(groupings, other.groupings);
    }
}
