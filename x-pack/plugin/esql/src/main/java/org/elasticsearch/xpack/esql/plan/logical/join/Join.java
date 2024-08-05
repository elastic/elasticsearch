/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Join extends BinaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Join", Join::new);

    private final JoinConfig config;
    private List<Attribute> lazyOutput;

    public Join(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right);
        this.config = config;
    }

    public Join(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> matchFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields
    ) {
        super(source, left, right);
        this.config = new JoinConfig(type, matchFields, leftFields, rightFields);
    }

    public Join(StreamInput in) throws IOException {
        super(
            Source.readFrom((PlanStreamInput) in),
            ((PlanStreamInput) in).readLogicalPlanNode(),
            ((PlanStreamInput) in).readLogicalPlanNode()
        );
        this.config = new JoinConfig(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        ((PlanStreamOutput) out).writeLogicalPlanNode(left());
        ((PlanStreamOutput) out).writeLogicalPlanNode(right());
        config.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public JoinConfig config() {
        return config;
    }

    @Override
    protected NodeInfo<Join> info() {
        // Do not just add the JoinConfig as a whole - this would prevent correctly registering the
        // expressions and references.
        return NodeInfo.create(
            this,
            Join::new,
            left(),
            right(),
            config.type(),
            config.matchFields(),
            config.leftFields(),
            config.rightFields()
        );
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
            lazyOutput = computeOutput(left().output(), right().output(), config);
        }
        return lazyOutput;
    }

    /**
     * Merge output fields.
     * Currently only implemented for LEFT JOINs; the rightOutput shadows the leftOutput, except for any attributes that
     * occur in the join's matchFields.
     */
    public static List<Attribute> computeOutput(List<Attribute> leftOutput, List<Attribute> rightOutput, JoinConfig config) {
        AttributeSet matchFieldSet = new AttributeSet(config.matchFields());
        Set<String> matchFieldNames = new HashSet<>(Expressions.names(config.matchFields()));
        return switch (config.type()) {
            case LEFT -> {
                // Right side becomes nullable.
                List<Attribute> fieldsAddedFromRight = removeCollisionsWithMatchFields(rightOutput, matchFieldSet, matchFieldNames);
                yield mergeOutputAttributes(makeNullable(makeReference(fieldsAddedFromRight)), leftOutput);
            }
            default -> throw new UnsupportedOperationException("Other JOINs than LEFT not supported");
        };
    }

    private static List<Attribute> removeCollisionsWithMatchFields(
        List<Attribute> attributes,
        AttributeSet matchFields,
        Set<String> matchFieldNames
    ) {
        List<Attribute> result = new ArrayList<>();
        for (Attribute attr : attributes) {
            if ((matchFields.contains(attr) || matchFieldNames.contains(attr.name())) == false) {
                result.add(attr);
            }
        }
        return result;
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
            if (a.resolved() && a instanceof ReferenceAttribute == false) {
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
