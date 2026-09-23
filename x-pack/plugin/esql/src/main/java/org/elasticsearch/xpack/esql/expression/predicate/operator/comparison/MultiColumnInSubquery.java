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
 * Unresolved expression for {@code (f1, f2) IN (subquery)} where the subquery is a full ES|QL query.
 * Multi-column generalization of {@link InSubquery}.
 * <p>
 * This node will be resolved by {@code InSubqueryResolver} into a concrete logical plan. {@code MultiColumnInSubquery} serves as the clean
 * boundary between parsing and pre-analysis: {@code LogicalPlanBuilder} creates an expression, and
 * {@link org.elasticsearch.xpack.esql.analysis.InSubqueryResolver InSubqueryResolver} transforms it into a logical plan.
 * <p>
 * If any {@code MultiColumnInSubquery} expressions remain after {@code InSubqueryResolver} runs, {@code InSubqueryResolver} raises a
 * {@link org.elasticsearch.xpack.esql.VerificationException}.
 */
public class MultiColumnInSubquery extends Expression {

    private final List<Expression> values;
    private final LogicalPlan subquery;

    public MultiColumnInSubquery(Source source, List<Expression> values, LogicalPlan subquery) {
        super(source, values);
        this.values = values;
        this.subquery = subquery;
    }

    public List<Expression> values() {
        return values;
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
        throw new UnsupportedOperationException("MultiColumnInSubquery is not serializable; it should be resolved during analysis");
    }

    @Override
    public void writeTo(org.elasticsearch.common.io.stream.StreamOutput out) {
        throw new UnsupportedOperationException("MultiColumnInSubquery is not serializable; it should be resolved during analysis");
    }

    @Override
    protected NodeInfo<MultiColumnInSubquery> info() {
        return NodeInfo.create(this, MultiColumnInSubquery::new, values, subquery);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MultiColumnInSubquery(source(), newChildren, subquery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, subquery);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MultiColumnInSubquery other = (MultiColumnInSubquery) obj;
        return Objects.equals(values, other.values) && Objects.equals(subquery, other.subquery);
    }
}
