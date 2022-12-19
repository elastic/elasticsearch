/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.eql.execution.assembler.ExecutionManager;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SampleExec extends PhysicalPlan {

    private final List<List<Attribute>> keys;
    private final Limit limit;

    public SampleExec(Source source, List<PhysicalPlan> children, List<List<Attribute>> keys) {
        this(source, children, keys, null);
    }

    public SampleExec(Source source, List<PhysicalPlan> children, List<List<Attribute>> keys, Limit limit) {
        super(source, children);
        this.keys = keys;
        this.limit = limit;
    }

    @Override
    protected NodeInfo<SampleExec> info() {
        return NodeInfo.create(this, SampleExec::new, children(), keys, limit);
    }

    @Override
    public PhysicalPlan replaceChildren(List<PhysicalPlan> newChildren) {
        return new SampleExec(source(), newChildren, keys, limit);
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> attrs = new ArrayList<>();
        for (List<? extends NamedExpression> ne : keys) {
            attrs.addAll(Expressions.asAttributes(ne));
        }
        return attrs;
    }

    public List<List<Attribute>> keys() {
        return keys;
    }

    @Override
    public void execute(EqlSession session, ActionListener<Payload> listener) {
        new ExecutionManager(session).assemble(keys(), children(), limit).execute(listener);
    }

    public PhysicalPlan with(Limit limit) {
        return new SampleExec(this.source(), this.children(), this.keys, limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keys, children(), limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SampleExec other = (SampleExec) obj;
        return Objects.equals(children(), other.children()) && Objects.equals(keys, other.keys) && Objects.equals(limit, other.limit);
    }
}
