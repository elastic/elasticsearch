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

/**
 * A {@code Filter} is a type of Plan that performs filtering of results. In
 * {@code SELECT x FROM y WHERE z ..} the "WHERE" clause is a Filter. A
 * {@code Filter} has a "condition" Expression that does the filtering.
 */
public class Filter extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Filter", Filter::new);

    private final Expression condition;

    public Filter(Source source, LogicalPlan child, Expression condition) {
        super(source, child);
        this.condition = condition;
    }

    private Filter(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(condition());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Filter> info() {
        return NodeInfo.create(this, Filter::new, child(), condition);
    }

    @Override
    public Filter replaceChild(LogicalPlan newChild) {
        return new Filter(source(), newChild, condition);
    }

    public Expression condition() {
        return condition;
    }

    @Override
    public boolean expressionsResolved() {
        return condition.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Filter other = (Filter) obj;

        return Objects.equals(condition, other.condition) && Objects.equals(child(), other.child());
    }

    public Filter with(Expression conditionExpr) {
        return new Filter(source(), child(), conditionExpr);
    }

    public Filter with(LogicalPlan child, Expression conditionExpr) {
        return new Filter(source(), child, conditionExpr);
    }
}
