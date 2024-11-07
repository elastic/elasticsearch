/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.MvExpand.calculateOutput;

public class MvExpandExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "MvExpandExec",
        MvExpandExec::new
    );

    private final NamedExpression target;
    private final Attribute expanded;
    private final List<Attribute> output;

    public MvExpandExec(Source source, PhysicalPlan child, NamedExpression target, Attribute expanded) {
        super(source, child);
        this.target = target;
        this.expanded = expanded;
        this.output = calculateOutput(child.output(), target, expanded);
    }

    private MvExpandExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(NamedExpression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(target());
        out.writeNamedWriteable(expanded());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected AttributeSet computeReferences() {
        return target.references();
    }

    @Override
    protected NodeInfo<MvExpandExec> info() {
        return NodeInfo.create(this, MvExpandExec::new, child(), target, expanded);
    }

    @Override
    public MvExpandExec replaceChild(PhysicalPlan newChild) {
        return new MvExpandExec(source(), newChild, target, expanded);
    }

    public NamedExpression target() {
        return target;
    }

    public Attribute expanded() {
        return expanded;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, child(), expanded);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MvExpandExec other = (MvExpandExec) obj;

        return Objects.equals(target, other.target) && Objects.equals(child(), other.child()) && Objects.equals(expanded, other.expanded);
    }
}
