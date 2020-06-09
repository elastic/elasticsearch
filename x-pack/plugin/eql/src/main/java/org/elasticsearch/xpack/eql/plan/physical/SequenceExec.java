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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;

public class SequenceExec extends PhysicalPlan {

    private final List<List<Attribute>> keys;
    private final Attribute timestamp;
    private final Attribute tieBreaker;

    public SequenceExec(Source source,
                        List<List<Attribute>> keys,
                        List<PhysicalPlan> matches,
                        List<Attribute> untilKeys,
                        PhysicalPlan until,
                        Attribute timestamp,
                        Attribute tieBreaker) {
        this(source, combine(matches, until), combine(keys, singletonList(untilKeys)), timestamp, tieBreaker);
    }

    private SequenceExec(Source source, List<PhysicalPlan> children, List<List<Attribute>> keys, Attribute ts, Attribute tb) {
        super(source, children);
        this.keys = keys;
        this.timestamp = ts;
        this.tieBreaker = tb;
    }

    @Override
    protected NodeInfo<SequenceExec> info() {
        return NodeInfo.create(this, SequenceExec::new, children(), keys, timestamp, tieBreaker);
    }

    @Override
    public PhysicalPlan replaceChildren(List<PhysicalPlan> newChildren) {
        if (newChildren.size() != children().size()) {
            throw new EqlIllegalArgumentException("Expected the same number of children [{}] but got [{}]",
                    children().size(),
                    newChildren.size());
        }
        return new SequenceExec(source(), newChildren, keys, timestamp, tieBreaker);
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> attrs = new ArrayList<>();
        attrs.add(timestamp);
        if (Expressions.isPresent(tieBreaker)) {
            attrs.add(tieBreaker);
        }
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

    public Attribute tieBreaker() {
        return tieBreaker;
    }

    @Override
    public void execute(EqlSession session, ActionListener<Results> listener) {
        new ExecutionManager(session).assemble(keys(), children(), timestamp(), tieBreaker()).execute(listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, tieBreaker, keys, children());
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
                && Objects.equals(tieBreaker, other.tieBreaker)
                && Objects.equals(children(), other.children())
                && Objects.equals(keys, other.keys);
    }
}