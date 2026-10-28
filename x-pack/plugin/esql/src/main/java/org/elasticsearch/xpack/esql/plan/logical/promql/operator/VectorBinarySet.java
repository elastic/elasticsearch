/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class VectorBinarySet extends VectorBinaryOperator {

    public enum SetOp implements BinaryOp {
        INTERSECT("and"),
        SUBTRACT("unless"),
        UNION("or");

        private final String keyword;

        SetOp(String keyword) {
            this.keyword = keyword;
        }

        /** The PromQL keyword for this operator ({@code and}/{@code unless}/{@code or}), as used in error messages. */
        public String keyword() {
            return keyword;
        }

        @Override
        public ScalarFunctionFactory asFunction() {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    private final SetOp op;

    public VectorBinarySet(Source source, LogicalPlan left, LogicalPlan right, VectorMatch match, SetOp op) {
        // Set operators preserve the metric name (__name__); unlike arithmetic/comparison operators they do not drop it.
        super(source, left, right, match, false, op);
        this.op = op;
    }

    public SetOp op() {
        return op;
    }

    /**
     * A set operator combines series from both operands, so its output schema is the union of both label sets
     * (deduplicated by name, left operand first). This differs from arithmetic/comparison operators, which
     * require matching label sets. Only {@code or} (UNION) is currently translated; {@code and}/{@code unless}
     * are rejected by the verifier before reaching translation.
     */
    @Override
    public List<Attribute> output() {
        return unionOutputByName(List.of(left(), right()));
    }

    /**
     * Computes the combined output of the given plans as the union of their output attributes, deduplicated by
     * name with the first occurrence winning. This is the output schema of a set operator (see {@link #output()})
     * and is also used to compute the combined output of a flattened top-level {@code or} chain during translation.
     */
    public static List<Attribute> unionOutputByName(List<? extends LogicalPlan> plans) {
        List<Attribute> result = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (LogicalPlan plan : plans) {
            for (Attribute attr : plan.output()) {
                if (names.add(attr.name())) {
                    result.add(attr);
                }
            }
        }
        return result;
    }

    @Override
    public VectorBinarySet replaceChildren(LogicalPlan newLeft, LogicalPlan newRight) {
        return new VectorBinarySet(source(), newLeft, newRight, match(), op());
    }

    @Override
    protected NodeInfo<VectorBinarySet> info() {
        return NodeInfo.create(this, VectorBinarySet::new, left(), right(), match(), op());
    }
}
