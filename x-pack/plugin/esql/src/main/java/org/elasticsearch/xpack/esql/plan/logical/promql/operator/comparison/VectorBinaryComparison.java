/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator.comparison;

import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;

import java.util.Objects;

public class VectorBinaryComparison extends VectorBinaryOperator {

    public enum ComparisonOp implements BinaryOp {
        EQ,
        NEQ,
        GT,
        GTE,
        LT,
        LTE;

        @Override
        public ScalarFunctionFactory asFunction() {
            return switch (this) {
                case EQ -> Equals::new;
                case NEQ -> NotEquals::new;
                case GT -> GreaterThan::new;
                case GTE -> GreaterThanOrEqual::new;
                case LT -> LessThan::new;
                case LTE -> LessThanOrEqual::new;
            };
        }
    }

    private final ComparisonOp op;
    private final boolean boolMode;

    public VectorBinaryComparison(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        VectorMatch match,
        boolean boolMode,
        ComparisonOp op
    ) {
        super(source, left, right, match, boolMode == false, op);
        this.op = op;
        this.boolMode = boolMode;
    }

    public ComparisonOp op() {
        return op;
    }

    public boolean boolMode() {
        return boolMode;
    }

    @Override
    public VectorBinaryOperator replaceChildren(LogicalPlan newLeft, LogicalPlan newRight) {
        return new VectorBinaryComparison(source(), newLeft, newRight, match(), boolMode, op());
    }

    @Override
    protected NodeInfo<VectorBinaryComparison> info() {
        return NodeInfo.create(this, VectorBinaryComparison::new, left(), right(), match(), boolMode(), op());
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            VectorBinaryComparison that = (VectorBinaryComparison) o;
            return boolMode == that.boolMode;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), boolMode);
    }
}
