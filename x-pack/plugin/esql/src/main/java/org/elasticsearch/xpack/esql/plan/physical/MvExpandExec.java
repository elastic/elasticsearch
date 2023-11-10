/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.MvExpand.calculateOutput;

public class MvExpandExec extends UnaryExec {

    private final NamedExpression target;
    private final Attribute expanded;
    private final List<Attribute> output;

    public MvExpandExec(Source source, PhysicalPlan child, NamedExpression target, Attribute expanded) {
        super(source, child);
        this.target = target;
        this.expanded = expanded;
        this.output = calculateOutput(child.output(), target, expanded);
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
