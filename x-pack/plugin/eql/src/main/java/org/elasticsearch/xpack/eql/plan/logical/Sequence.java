/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

public class Sequence extends Join {

    private final TimeValue maxSpan;

    public Sequence(Source source,
                    List<KeyedFilter> queries,
                    KeyedFilter until,
                    TimeValue maxSpan,
                    Attribute timestamp,
                    Attribute tiebreaker,
                    OrderDirection direction) {
        super(source, queries, until, timestamp, tiebreaker, direction);
        this.maxSpan = maxSpan;
    }

    private Sequence(Source source,
                     List<LogicalPlan> queries,
                     LogicalPlan until,
                     TimeValue maxSpan,
                     Attribute timestamp,
                     Attribute tiebreaker,
                     OrderDirection direction) {
        super(source, asKeyed(queries), asKeyed(until), timestamp, tiebreaker, direction);
        this.maxSpan = maxSpan;
    }

    @Override
    protected NodeInfo<Sequence> info() {
        return NodeInfo.create(this, Sequence::new, queries(), until(), maxSpan, timestamp(), tiebreaker(), direction());
    }

    @Override
    public Join replaceChildren(List<LogicalPlan> newChildren) {
        int lastIndex = newChildren.size() - 1;
        return new Sequence(source(), newChildren.subList(0, lastIndex), newChildren.get(lastIndex), maxSpan, timestamp(), tiebreaker(),
                direction());
    }

    public TimeValue maxSpan() {
        return maxSpan;
    }

    @Override
    public Join with(List<KeyedFilter> queries, KeyedFilter until, OrderDirection direction) {
        return new Sequence(source(), queries, until, maxSpan, timestamp(), tiebreaker(), direction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxSpan, super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            Sequence other = (Sequence) obj;
            return Objects.equals(maxSpan, other.maxSpan);
        }
        return false;
    }

    @Override
    public List<Object> nodeProperties() {
        return asList(maxSpan, direction());
    }
}
