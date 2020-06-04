/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.assembler.ExecutionManager;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class SequenceExec extends PhysicalPlan {

    private final List<List<Attribute>> keys;
    private final Attribute timestamp;

    public SequenceExec(Source source,
                        List<List<Attribute>> keys,
                        List<PhysicalPlan> matches,
                        List<Attribute> untilKeys,
                        PhysicalPlan until,
                        Attribute timestampField) {
        this(source, CollectionUtils.combine(matches, until), CollectionUtils.combine(keys, singletonList(untilKeys)), timestampField);
    }

    private SequenceExec(Source source, List<PhysicalPlan> children, List<List<Attribute>> keys, Attribute timestampField) {
        super(source, children);
        this.keys = keys;
        this.timestamp = timestampField;
    }

    @Override
    protected NodeInfo<SequenceExec> info() {
        return NodeInfo.create(this, SequenceExec::new, children(), keys, timestamp);
    }

    @Override
    public PhysicalPlan replaceChildren(List<PhysicalPlan> newChildren) {
        if (newChildren.size() != children().size()) {
            throw new EqlIllegalArgumentException("Expected the same number of children [{}] but got [{}]", children().size(), newChildren
                    .size());
        }
        return new SequenceExec(source(), newChildren, keys, timestamp);
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> attrs = new ArrayList<>();
        attrs.add(timestamp);
        for (List<? extends NamedExpression> ne : keys) {
            attrs.addAll(Expressions.asAttributes(ne));
        }
        return attrs;
    }

    public List<List<Attribute>> keys() {
        return keys;
    }

    public Attribute timestamp() {
        return timestamp;
    }

    @Override
    public void execute(EqlSession session, ActionListener<Results> listener) {
        new ExecutionManager(session).from(this).execute(listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, keys, children());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SequenceExec other = (SequenceExec) obj;
        return Objects.equals(timestamp, other.timestamp)
                && Objects.equals(children(), other.children())
                && Objects.equals(keys, other.keys);
    }
}