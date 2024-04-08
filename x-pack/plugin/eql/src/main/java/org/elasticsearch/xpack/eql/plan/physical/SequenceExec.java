/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.eql.execution.assembler.ExecutionManager;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
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
    private final Attribute tiebreaker;
    private final Limit limit;
    private final OrderDirection direction;
    private final TimeValue maxSpan;
    private final boolean[] missing;

    public SequenceExec(
        Source source,
        List<List<Attribute>> keys,
        List<PhysicalPlan> matches,
        List<Attribute> untilKeys,
        PhysicalPlan until,
        Attribute timestamp,
        Attribute tiebreaker,
        OrderDirection direction,
        TimeValue maxSpan,
        boolean[] missing
    ) {
        this(
            source,
            combine(matches, until),
            combine(keys, singletonList(untilKeys)),
            timestamp,
            tiebreaker,
            null,
            direction,
            maxSpan,
            missing
        );
    }

    private SequenceExec(
        Source source,
        List<PhysicalPlan> children,
        List<List<Attribute>> keys,
        Attribute ts,
        Attribute tb,
        Limit limit,
        OrderDirection direction,
        TimeValue maxSpan,
        boolean[] missing
    ) {
        super(source, children);
        this.keys = keys;
        this.timestamp = ts;
        this.tiebreaker = tb;
        this.limit = limit;
        this.direction = direction;
        this.maxSpan = maxSpan;
        this.missing = missing;
    }

    @Override
    protected NodeInfo<SequenceExec> info() {
        return NodeInfo.create(this, SequenceExec::new, children(), keys, timestamp, tiebreaker, limit, direction, maxSpan, missing);
    }

    @Override
    public PhysicalPlan replaceChildren(List<PhysicalPlan> newChildren) {
        return new SequenceExec(source(), newChildren, keys, timestamp, tiebreaker, limit, direction, maxSpan, missing);
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> attrs = new ArrayList<>();
        attrs.add(timestamp);
        if (Expressions.isPresent(tiebreaker)) {
            attrs.add(tiebreaker);
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

    public Attribute tiebreaker() {
        return tiebreaker;
    }

    public Limit limit() {
        return limit;
    }

    public OrderDirection direction() {
        return direction;
    }

    public SequenceExec with(Limit limit) {
        return new SequenceExec(source(), children(), keys(), timestamp(), tiebreaker(), limit, direction, maxSpan, missing);
    }

    @Override
    public void execute(EqlSession session, ActionListener<Payload> listener) {
        new ExecutionManager(session).assemble(keys(), children(), timestamp(), tiebreaker(), direction, maxSpan, limit(), missing)
            .execute(listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, tiebreaker, keys, limit, direction, children());
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
            && Objects.equals(tiebreaker, other.tiebreaker)
            && Objects.equals(limit, other.limit)
            && Objects.equals(direction, other.direction)
            && Objects.equals(children(), other.children())
            && Objects.equals(keys, other.keys);
    }
}
