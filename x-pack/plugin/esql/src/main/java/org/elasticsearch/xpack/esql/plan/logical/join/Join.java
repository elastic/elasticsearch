/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Join extends BinaryPlan {

    private final JoinConfig config;
    // TODO: The matching attributes from the left and right logical plans should become part of the `expressions()`
    // so that `references()` returns the attributes we actually rely on.
    private List<Attribute> lazyOutput;

    public Join(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right);
        this.config = config;
    }

    public Join(PlanStreamInput in) throws IOException {
        super(Source.readFrom(in), in.readLogicalPlanNode(), in.readLogicalPlanNode());
        this.config = new JoinConfig(in);
    }

    public void writeTo(PlanStreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeLogicalPlanNode(left());
        out.writeLogicalPlanNode(right());
        config.writeTo(out);
    }

    public JoinConfig config() {
        return config;
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, Join::new, left(), right(), config);
    }

    @Override
    public Join replaceChildren(List<LogicalPlan> newChildren) {
        return new Join(source(), newChildren.get(0), newChildren.get(1), config);
    }

    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new Join(source(), left, right, config);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = computeOutput();
        }
        return lazyOutput;
    }

    private List<Attribute> computeOutput() {
        List<Attribute> right = makeReference(right().output());
        return switch (config.type()) {
            case LEFT -> // right side becomes nullable
                mergeOutput(left().output(), makeNullable(right), config.matchFields());
            case RIGHT -> // left side becomes nullable
                mergeOutput(makeNullable(left().output()), right, config.matchFields());
            case FULL -> // both sides become nullable
                mergeOutput(makeNullable(left().output()), makeNullable(right), config.matchFields());
            default -> // neither side becomes nullable
                mergeOutput(left().output(), right, config.matchFields());
        };
    }

    /**
     * Merge output fields, left hand side wins in name conflicts <strong>except</strong>
     * for fields defined in {@link JoinConfig#matchFields()}.
     */
    public static List<Attribute> mergeOutput(
        List<? extends Attribute> lhs,
        List<? extends Attribute> rhs,
        List<NamedExpression> matchFields
    ) {
        List<Attribute> results = new ArrayList<>(lhs.size() + rhs.size());

        for (Attribute a : lhs) {
            if (rhs.contains(a) == false || matchFields.stream().anyMatch(m -> m.name().equals(a.name()))) {
                results.add(a);
            }
        }
        for (Attribute a : rhs) {
            if (false == matchFields.stream().anyMatch(m -> m.name().equals(a.name()))) {
                results.add(a);
            }
        }
        return results;
    }

    /**
     * Make fields references, so we don't check if they exist in the index.
     * We do this for fields that we know don't come from the index.
     * <p>
     *   It's important that name is returned as a *reference* here
     *   instead of a field. If it were a field we'd use SearchStats
     *   on it and discover that it doesn't exist in the index. It doesn't!
     *   We don't expect it to. It exists only in the lookup table.
     *   TODO we should rework stats so we don't have to do this
     * </p>
     */
    public static List<Attribute> makeReference(List<Attribute> output) {
        List<Attribute> out = new ArrayList<>(output.size());
        for (Attribute a : output) {
            if (a.resolved()) {
                out.add(new ReferenceAttribute(a.source(), a.name(), a.dataType(), a.qualifier(), a.nullable(), a.id(), a.synthetic()));
            } else {
                out.add(a);
            }
        }
        return out;
    }

    public static List<Attribute> makeNullable(List<Attribute> output) {
        List<Attribute> out = new ArrayList<>(output.size());
        for (Attribute a : output) {
            out.add(a.withNullability(Nullability.TRUE));
        }
        return out;
    }

    @Override
    public boolean expressionsResolved() {
        return config.expressionsResolved();
    }

    @Override
    public boolean resolved() {
        // resolve the join if
        // - the children are resolved
        // - the condition (if present) is resolved to a boolean
        return childrenResolved() && expressionsResolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Join other = (Join) obj;
        return config.equals(other.config) && Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }
}
