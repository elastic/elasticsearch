/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class Join extends AbstractJoin {

    private final KeyedFilter until;
    private final Attribute timestamp;
    private final Attribute tiebreaker;
    private final OrderDirection direction;

    public Join(
        Source source,
        List<KeyedFilter> queries,
        KeyedFilter until,
        Attribute timestamp,
        Attribute tiebreaker,
        OrderDirection direction
    ) {
        super(source, queries, until);
        this.until = until;
        this.timestamp = timestamp;
        this.tiebreaker = tiebreaker;
        this.direction = direction;
    }

    private Join(
        Source source,
        List<LogicalPlan> queries,
        LogicalPlan until,
        Attribute timestamp,
        Attribute tiebreaker,
        OrderDirection direction
    ) {
        this(source, asKeyed(queries), asKeyed(until), timestamp, tiebreaker, direction);
    }

    @Override
    protected NodeInfo<? extends Join> info() {
        return NodeInfo.create(this, Join::new, queries(), until, timestamp, tiebreaker, direction);
    }

    @Override
    public Join replaceChildren(List<LogicalPlan> newChildren) {
        int lastIndex = newChildren.size() - 1;
        return new Join(source(), newChildren.subList(0, lastIndex), newChildren.get(lastIndex), timestamp, tiebreaker, direction);
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> out = new ArrayList<>();

        out.add(timestamp);
        if (Expressions.isPresent(tiebreaker)) {
            out.add(tiebreaker);
        }

        out.addAll(super.output());
        return out;
    }

    @Override
    public boolean expressionsResolved() {
        return timestamp.resolved() && tiebreaker.resolved() && until.resolved() && super.expressionsResolved();
    }

    public KeyedFilter until() {
        return until;
    }

    public Attribute timestamp() {
        return timestamp;
    }

    public Attribute tiebreaker() {
        return tiebreaker;
    }

    public OrderDirection direction() {
        return direction;
    }

    public Join with(List<KeyedFilter> queries, KeyedFilter until, OrderDirection direction) {
        return new Join(source(), queries, until, timestamp, tiebreaker, direction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(direction, timestamp, tiebreaker, until, super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            Join other = (Join) obj;
            return Objects.equals(direction, other.direction)
                && Objects.equals(until, other.until)
                && Objects.equals(timestamp, other.timestamp)
                && Objects.equals(tiebreaker, other.tiebreaker);
        }
        return false;
    }

    @Override
    public List<Object> nodeProperties() {
        return singletonList(direction);
    }
}
