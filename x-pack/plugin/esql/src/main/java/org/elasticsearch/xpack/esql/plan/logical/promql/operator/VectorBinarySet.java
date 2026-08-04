/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
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
     * {@code or} combines series from both operands, so its output schema is the union of both label sets
     * (deduplicated by name, left operand first). {@code and} keeps (a subset of) the left operand's series and
     * carries its labels; {@code unless} is rejected by the verifier before reaching translation.
     */
    @Override
    public List<Attribute> output() {
        // `or` combines series from both sides, so its schema is the union of both label sets. `and`/`unless` keep (a subset of) the
        // left operand's series, so they carry only the left operand's labels - plus the on(...) match keys, which the
        // translation guarantees as concrete columns even when the left operand is opaque (identity packed into
        // `_timeseries`); keys no operand declares are synthesized references, mirroring VectorBinaryOperator.output.
        return switch (op) {
            case UNION -> unionOutputByName(List.of(left(), right()));
            case INTERSECT, SUBTRACT -> {
                if (match() == null || match().filter() != VectorMatch.Filter.ON) {
                    yield left().output();
                }
                List<Attribute> result = new ArrayList<>(left().output());
                Set<String> names = new HashSet<>();
                result.forEach(attr -> names.add(attr.name()));
                for (String label : match().filterLabels()) {
                    if (names.add(label)) {
                        Attribute attr = findByName(label, right().output());
                        result.add(attr != null ? attr : new ReferenceAttribute(source(), label, DataType.KEYWORD));
                    }
                }
                yield result;
            }
        };
    }

    private static Attribute findByName(String name, List<Attribute> attributes) {
        for (Attribute attr : attributes) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        return null;
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
