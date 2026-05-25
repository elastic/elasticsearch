/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Objects;

/**
 * Unresolved expression for {@code field IN (subquery)} where the subquery is a full ES|QL query.
 * Produced by the parser as a placeholder; the analyzer half (resolution into a join-based plan) is
 * not yet implemented on this branch, and the Verifier rejects any plan that still contains an
 * {@code InSubquery} after analysis.
 */
public class InSubquery extends Expression {

    private final Expression value;
    private final LogicalPlan subquery;

    public InSubquery(Source source, Expression value, LogicalPlan subquery) {
        super(source, List.of(value));
        this.value = value;
        this.subquery = subquery;
    }

    public Expression value() {
        return value;
    }

    public LogicalPlan subquery() {
        return subquery;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("InSubquery is not serializable; it should be resolved during analysis");
    }

    @Override
    public void writeTo(org.elasticsearch.common.io.stream.StreamOutput out) {
        throw new UnsupportedOperationException("InSubquery is not serializable; it should be resolved during analysis");
    }

    @Override
    protected NodeInfo<InSubquery> info() {
        return NodeInfo.create(this, InSubquery::new, value, subquery);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new InSubquery(source(), newChildren.get(0), subquery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, subquery);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InSubquery other = (InSubquery) obj;
        return Objects.equals(value, other.value) && Objects.equals(subquery, other.subquery);
    }
}
