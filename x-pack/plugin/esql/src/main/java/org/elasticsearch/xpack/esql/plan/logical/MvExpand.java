/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MvExpand extends UnaryPlan {
    private final NamedExpression target;
    private final Attribute expanded;

    private List<Attribute> output;
    private Limit limit; // a temporary "marker" indicating if a limit has been "associated" with this mv_expand

    public MvExpand(Source source, LogicalPlan child, NamedExpression target, Attribute expanded, Limit limit) {
        super(source, child);
        this.target = target;
        this.expanded = expanded;
        this.limit = limit;
    }

    public static List<Attribute> calculateOutput(List<Attribute> input, NamedExpression target, Attribute expanded) {
        List<Attribute> result = new ArrayList<>();
        for (Attribute attribute : input) {
            if (attribute.name().equals(target.name())) {
                result.add(expanded);
            } else {
                result.add(attribute);
            }
        }
        return result;
    }

    public NamedExpression target() {
        return target;
    }

    public Attribute expanded() {
        return expanded;
    }

    public Limit limit() {
        return limit;
    }

    @Override
    public boolean expressionsResolved() {
        return target.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new MvExpand(source(), newChild, target, expanded, limit);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = calculateOutput(child().output(), target, expanded);
        }
        return output;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, MvExpand::new, child(), target, expanded, limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), target, expanded, limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var other = (MvExpand) obj;
            return Objects.equals(target, other.target) && Objects.equals(expanded, other.expanded) && Objects.equals(limit, other.limit);
        }
        return false;
    }
}
